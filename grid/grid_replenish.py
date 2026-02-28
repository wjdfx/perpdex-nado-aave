"""
网格交易补单逻辑模块

包含网格补单的核心逻辑：开仓侧/平仓侧补单、大间距补单等。
"""

import logging
import asyncio
import time
from typing import List, Optional, Tuple

from . import grid_state

logger = logging.getLogger(__name__)


def calculate_grid_prices(
    current_price: float, grid_count: int, grid_spread: float
) -> List[float]:
    """
    计算网格开仓价格列表
    
    订单以 GRID_SPREAD 的价差比例，均匀分布在当前价格"开仓方向"一侧
    
    Args:
        current_price: 当前价格
        grid_count: 网格数量
        grid_spread: 网格价差百分比
        
    Returns:
        开仓价格列表（已排序）
    """
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    
    open_prices = []

    # 价差比例（百分比转换为小数）
    spread_decimal = grid_spread / 100

    # 计算网格价格
    for i in range(grid_count):
        distance = (i + 1) * spread_decimal
        if not OPEN_SIDE_IS_ASK:
            # 做多: 开仓价格在当前价格 BELOW
            price = current_price * (1 - distance)
        else:
            # 做空: 开仓价格在当前价格 ABOVE
            price = current_price * (1 + distance)

        open_prices.append(round(price, 2))

    # 排序价格（从低到高）
    open_prices.sort()

    return open_prices


async def replenish_grid(filled_signal: bool, trade_price: float = 0.0):
    """
    补充网格订单逻辑
    
    基于原始订单价格分布和当前价格，计算补充订单的价格和方向
    
    Args:
        filled_signal: 是否有订单成交
        trade_price: 成交价格
    """
    trading_state = grid_state.trading_state
    
    if trading_state.grid_pause:
        logger.info("网格交易处于暂停状态，跳过补单")
        return

    if trading_state.open_orders_count == 0 and trading_state.close_orders_count == 0:
        # 初始化网格交易
        from .quant_grid_universal import initialize_grid_trading
        if not await initialize_grid_trading(trading_state.grid_trading):
            logger.exception("网格交易初始化失败，退出")
            return

    try:
        if filled_signal:
            # 开仓侧被吃单 (e.g. Long Buy filled)
            await _on_open_side_filled(trade_price)
            # 平仓侧被吃单 (e.g. Long Sell filled)
            await _on_close_side_filled(trade_price)

        # 大间距补单
        await _over_range_replenish_order()

        # 开仓侧补充不少于配置单的数量
        await _replenish_config_open_orders()

        # 平仓侧补充不少于配置单的数量
        if trading_state.available_position_size > 0:
            await _replenish_config_close_orders()

    except Exception:
        logger.exception(f"补充网格订单时发生错误")


async def _on_open_side_filled(trade_price: float = 0.0):
    """
    开仓侧被吃单到需要补单时 (Position Increased)
    
    Args:
        trade_price: 成交价格
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    
    # 如果上一次成交是平仓侧，则跳过
    if trading_state.last_filled_order_is_close_side:
        return

    logger.info("开仓侧被吃单补单")
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    CLOSE_SIDE_IS_ASK = grid_state.CLOSE_SIDE_IS_ASK
    open_orders = []
    close_order = None

    # 1. 补充开仓单 (继续建仓)
    if (
        not trading_state.grid_pause
        and trading_state.open_orders_count < GRID_CONFIG["GRID_COUNT"]
    ):
        new_open_order = await _calc_next_open_side_open_order()
        if new_open_order:
            open_orders.append(new_open_order)

    # 2. 补充平仓单 (配对止盈单)：买单成交后必须挂出对应卖单
    new_close_order = await _calc_next_open_side_close_order(trade_price)
    if new_close_order:
        close_order = new_close_order
    else:
        # 计算失败时仍用成交价+步长挂出配对平仓单，确保开仓成交必有止盈单
        step = trading_state.base_grid_single_price
        if not OPEN_SIDE_IS_ASK:  # 做多：卖单价格 = 成交价 + 步长
            close_price = round(trade_price + step, 2)
            close_order = (CLOSE_SIDE_IS_ASK, close_price, GRID_CONFIG["GRID_AMOUNT"])
        else:  # 做空：买单价格 = 成交价 - 步长
            close_price = round(trade_price - step, 2)
            close_order = (CLOSE_SIDE_IS_ASK, close_price, GRID_CONFIG["GRID_AMOUNT"])
        logger.info(f"开仓侧成交后使用 fallback 挂出配对平仓单: 价格={close_order[1]}")

    all_order_ids = []

    # 先下开仓单（不使用 reduce_only）
    if open_orders:
        success, order_ids = await trading_state.grid_trading.place_multi_orders(open_orders)
        if success:
            for idx, oid in enumerate(order_ids):
                is_ask, price, _ = open_orders[idx]
                if is_ask:
                    trading_state.sell_orders[oid] = price
                else:
                    trading_state.buy_orders[oid] = price
            all_order_ids.extend(order_ids)
        else:
            logger.error("开仓侧补充开仓单失败")

    # 再下配对平仓单（使用 reduce_only=True）
    if close_order:
        is_ask, _price, amount = close_order
        if float(trade_price) > 0:
            fill_price_for_retry = float(trade_price)
        else:
            step_for_backfill = float(trading_state.base_grid_single_price or 0.0)
            if step_for_backfill <= 0:
                step_for_backfill = float(trading_state.active_grid_signle_price or 0.0)
            if not OPEN_SIDE_IS_ASK:  # LONG: _price≈fill+step
                fill_price_for_retry = float(_price) - step_for_backfill
            else:  # SHORT: _price≈fill-step
                fill_price_for_retry = float(_price) + step_for_backfill
        success, order_id, final_price = await _place_paired_close_order_with_retry(
            is_ask=is_ask,
            fill_price=fill_price_for_retry,
            amount=amount,
        )
        if success:
            trading_state.paired_close_retry_block_until = 0.0
            trading_state.paired_close_target_price = 0.0
            if is_ask:
                trading_state.sell_orders[order_id] = final_price
            else:
                trading_state.buy_orders[order_id] = final_price
            all_order_ids.append(order_id)
            logger.info(
                f"开仓侧被吃单补充订单成功: 开仓单={len(open_orders)}, 配对平仓单=1, 订单ID={all_order_ids}"
            )
        else:
            trading_state.paired_close_target_price = float(final_price)
            trading_state.paired_close_retry_block_until = time.time() + 20
            logger.error(
                "开仓侧补充配对平仓单失败: is_ask=%s, final_price=%s",
                is_ask,
                final_price,
            )
            logger.warning(
                "配对平仓单失败，已临时禁止大间距平仓补单20秒: target_price=%s",
                final_price,
            )


async def _place_paired_close_order_with_retry(
    is_ask: bool,
    fill_price: float,
    amount: float,
    stage3_retry_interval_sec: float = 2.0,
    stage3_log_interval_sec: float = 30.0,
) -> Tuple[bool, str, float]:
    """
    开仓成交后的配对平仓单重试状态机（1x -> 2x -> 3x）。
    - 全程 post-only + reduce_only
    - 1x失败后升2x，2x失败后升3x
    - 3x阶段持续重试直到成功（日志节流）
    """
    trading_state = grid_state.trading_state
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK

    step = float(trading_state.base_grid_single_price or 0.0)
    if step <= 0:
        step = float(trading_state.active_grid_signle_price or 0.0)
    if step <= 0:
        logger.error("配对平仓单重试失败：step无效，无法计算目标价格")
        return False, "", float(fill_price)

    stages = [
        ("1x", 1, 3),
        ("2x", 2, 5),
        ("3x", 3, None),  # 持续重试
    ]
    stage_index = 0
    last_error = "unknown"
    last_log_time = 0.0

    def calc_target_price(multiplier: int) -> float:
        if not OPEN_SIDE_IS_ASK:  # LONG: 买入后挂卖单
            return round(float(fill_price) + step * multiplier, 2)
        # SHORT: 卖出后挂买单
        return round(float(fill_price) - step * multiplier, 2)

    while stage_index < len(stages):
        stage_name, stage_multiplier, stage_retry_limit = stages[stage_index]
        target_price = calc_target_price(stage_multiplier)

        # 若目标价已落后于当前价，阶段升级（LONG: target<=current, SHORT: target>=current）
        current_price = float(trading_state.current_price or 0.0)
        is_behind_market = (
            (not OPEN_SIDE_IS_ASK and current_price > 0 and target_price <= current_price)
            or (OPEN_SIDE_IS_ASK and current_price > 0 and target_price >= current_price)
        )
        if is_behind_market and stage_index < 2:
            old_stage = stage_name
            stage_index += 1
            new_stage = stages[stage_index][0]
            logger.warning(
                "配对平仓阶段升级: from=%s to=%s, reason=target_behind_market, fill=%s, step=%s, current=%s",
                old_stage,
                new_stage,
                round(float(fill_price), 6),
                round(step, 6),
                round(current_price, 6),
            )
            continue

        attempt = 0
        while stage_retry_limit is None or attempt < stage_retry_limit:
            attempt += 1
            target_price = calc_target_price(stage_multiplier)
            trading_state.paired_close_target_price = float(target_price)
            trading_state.paired_close_retry_block_until = time.time() + 20

            success, order_id = await trading_state.grid_trading.place_single_order(
                is_ask=is_ask,
                price=target_price,
                amount=amount,
                reduce_only=True,
            )
            if success:
                logger.info(
                    "配对平仓成功: stage=%s, attempt=%s, order_id=%s, price=%s",
                    stage_name,
                    attempt,
                    order_id,
                    target_price,
                )
                return True, order_id, target_price

            last_error = "place_single_order_failed"
            logger.warning(
                "配对平仓重试: stage=%s, attempt=%s%s, price=%s, error=%s",
                stage_name,
                attempt,
                "" if stage_retry_limit is None else f"/{stage_retry_limit}",
                target_price,
                last_error,
            )

            # 3x 阶段持续重试并节流日志
            if stage_retry_limit is None:
                now = time.time()
                if now - last_log_time >= stage3_log_interval_sec:
                    logger.warning(
                        "配对平仓仍未成功，持续重试中: stage=%s, fill=%s, target=%s",
                        stage_name,
                        round(float(fill_price), 6),
                        target_price,
                    )
                    last_log_time = now
                await asyncio.sleep(stage3_retry_interval_sec)
            else:
                await asyncio.sleep(0.35 * attempt)

        # 当前阶段耗尽，升级阶段
        if stage_index < 2:
            old_stage = stage_name
            stage_index += 1
            new_stage = stages[stage_index][0]
            logger.warning(
                "配对平仓阶段升级: from=%s to=%s, reason=retry_exhausted, fill=%s, step=%s",
                old_stage,
                new_stage,
                round(float(fill_price), 6),
                round(step, 6),
            )
        else:
            # 理论上不会到这里（3x为无限重试），防御性返回
            break

    return False, "", calc_target_price(3)


async def _calc_next_open_side_open_order() -> Optional[Tuple[bool, float, float]]:
    """
    计算基于开仓侧方向的下一个开仓单 (Further into the trend)
    
    Returns:
        (is_ask, price, amount) 元组，或 None
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    
    # 获取"最远"的开仓价
    # 做多: 最低价。做空: 最高价。

    if trading_state.open_orders_count > 0:
        if not OPEN_SIDE_IS_ASK:  # 做多
            furthest_price = min(trading_state.open_orders.values())
        else:  # 做空
            furthest_price = max(trading_state.open_orders.values())
    else:
        # 无开仓订单时的回退逻辑
        multiplier = -1 if not OPEN_SIDE_IS_ASK else 1
        furthest_price = trading_state.current_price + (
            trading_state.active_grid_signle_price * multiplier
        )

    # 计算下一个价格
    # 做多: 最低价 - Step。做空: 最高价 + Step。
    multiplier = -1 if not OPEN_SIDE_IS_ASK else 1
    new_price = round(
        furthest_price + (trading_state.active_grid_signle_price * multiplier), 2
    )

    # 安全检查：
    # 做多: 新价格必须 < 当前价格
    # 做空: 新价格必须 > 当前价格

    if not OPEN_SIDE_IS_ASK:  # 做多
        while new_price >= trading_state.current_price:
            new_price = round(new_price - trading_state.active_grid_signle_price, 2)
    else:  # 做空
        while new_price <= trading_state.current_price:
            new_price = round(new_price + trading_state.active_grid_signle_price, 2)

    amount = GRID_CONFIG["GRID_AMOUNT"]
    return (OPEN_SIDE_IS_ASK, new_price, amount)


async def _calc_next_open_side_close_order(
    trade_price: float = 0.0,
) -> Optional[Tuple[bool, float, float]]:
    """
    计算基于开仓侧方向的配套平仓单
    
    Args:
        trade_price: 成交价格
        
    Returns:
        (is_ask, price, amount) 元组，或 None
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    CLOSE_SIDE_IS_ASK = grid_state.CLOSE_SIDE_IS_ASK
    
    # 1. 获取"最近"的平仓订单价格
    # 做多: 卖单，"最近" = 最低卖价
    # 做空: 买单，"最近" = 最高买价

    nearest_close_price = None
    if trading_state.close_orders_count > 0:
        if not OPEN_SIDE_IS_ASK:  # 做多 (平仓=卖)
            nearest_close_price = min(trading_state.close_orders.values())
        else:  # 做空 (平仓=买)
            nearest_close_price = max(trading_state.close_orders.values())

    # 2. 获取"最近"的开仓订单价格
    nearest_open_price = None
    if trading_state.open_orders_count > 0:
        if not OPEN_SIDE_IS_ASK:  # 做多
            nearest_open_price = max(trading_state.open_orders.values())
        else:  # 做空
            nearest_open_price = min(trading_state.open_orders.values())

    # 默认值
    if nearest_close_price is None:
        if not OPEN_SIDE_IS_ASK:  # 做多
            nearest_close_price = (
                trading_state.current_price + trading_state.base_grid_single_price * 2
            )
        else:  # 做空
            nearest_close_price = (
                trading_state.current_price - trading_state.base_grid_single_price * 2
            )

    if nearest_open_price is None:
        if not OPEN_SIDE_IS_ASK:  # 做多
            nearest_open_price = (
                trading_state.current_price - trading_state.base_grid_single_price
            )
        else:  # 做空
            nearest_open_price = (
                trading_state.current_price + trading_state.base_grid_single_price
            )

    # 3. 计算新的平仓价格
    # 默认: 向"更近"的方向推一步
    # 做多: 最低卖 - Step
    # 做空: 最高买 + Step

    multiplier = -1 if not OPEN_SIDE_IS_ASK else 1
    new_close_price = round(
        nearest_close_price + (trading_state.base_grid_single_price * multiplier), 2
    )

    # 4. 使用成交价格覆盖逻辑
    if trade_price > 0:
        # 做多: 成交(买) + Step
        # 做空: 成交(卖) - Step
        price_multiplier = 1 if not OPEN_SIDE_IS_ASK else -1
        new_close_price = round(
            trade_price + (trading_state.base_grid_single_price * price_multiplier), 2
        )

    # 5. 间距检查
    diff = abs(new_close_price - trading_state.current_price)
    if diff > trading_state.base_grid_single_price * 2:
        safe_multiplier = 1 if not OPEN_SIDE_IS_ASK else -1
        new_close_price = round(
            nearest_open_price
            + (
                trading_state.active_grid_signle_price
                + trading_state.base_grid_single_price
            )
            * safe_multiplier,
            2,
        )

    # 6. 当前价格安全检查
    # 做多: 平仓价格 (卖) 应 > 当前价格；若略低仍挂出，确保买单成交后必有对应卖单
    # 做空: 平仓价格 (买) 应 < 当前价格；若略高仍挂出，确保卖单成交后必有对应买单

    if not OPEN_SIDE_IS_ASK:  # 做多
        if trading_state.current_price < new_close_price:
            return (CLOSE_SIDE_IS_ASK, new_close_price, GRID_CONFIG["GRID_AMOUNT"])
        # 放宽：当前价略高于计算卖价时，仍用成交价+步长挂出卖单，保证买单成交必有配对卖单
        if trade_price > 0:
            fallback_price = round(
                trade_price + trading_state.base_grid_single_price, 2
            )
            if fallback_price > trading_state.current_price:
                return (CLOSE_SIDE_IS_ASK, fallback_price, GRID_CONFIG["GRID_AMOUNT"])
            return (CLOSE_SIDE_IS_ASK, new_close_price, GRID_CONFIG["GRID_AMOUNT"])
    else:  # 做空
        if trading_state.current_price > new_close_price:
            return (CLOSE_SIDE_IS_ASK, new_close_price, GRID_CONFIG["GRID_AMOUNT"])
        if trade_price > 0:
            fallback_price = round(
                trade_price - trading_state.base_grid_single_price, 2
            )
            if fallback_price < trading_state.current_price:
                return (CLOSE_SIDE_IS_ASK, fallback_price, GRID_CONFIG["GRID_AMOUNT"])
            return (CLOSE_SIDE_IS_ASK, new_close_price, GRID_CONFIG["GRID_AMOUNT"])

    return None


async def _on_close_side_filled(trade_price: float = 0.0):
    """
    平仓侧被吃单到需要补单时 (Profit Taking)
    
    Args:
        trade_price: 成交价格
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    
    # 如果上一次成交不是平仓侧（是开仓侧），则跳过
    if not trading_state.last_filled_order_is_close_side:
        return

    logger.info("平仓侧被吃单补单")
    open_orders = []
    close_orders = []

    # 1. 补充开仓单 (Buy Back)
    if not trading_state.grid_pause:
        new_open_order = await _calc_next_close_side_open_order()
        if new_open_order:
            open_orders.append(new_open_order)

    # 2. 补充平仓单 (如果还有剩余仓位需要止盈)
    current_close_orders_volume = (
        trading_state.close_orders_count * GRID_CONFIG["GRID_AMOUNT"]
    )

    if (
        trading_state.available_position_size
        > current_close_orders_volume + GRID_CONFIG["GRID_AMOUNT"]
        and trading_state.close_orders_count > 0
    ):
        new_close_order = await _calc_next_close_side_close_order()
        if new_close_order:
            close_orders.append(new_close_order)

    # 分别处理开仓单和平仓单
    all_order_ids = []
    
    # 先处理开仓单（不使用 reduce_only）
    if open_orders:
        success, order_ids = await trading_state.grid_trading.place_multi_orders(open_orders)
        if success:
            for idx, oid in enumerate(order_ids):
                is_ask, price, _ = open_orders[idx]
                if is_ask:
                    trading_state.sell_orders[oid] = price
                else:
                    trading_state.buy_orders[oid] = price
            all_order_ids.extend(order_ids)
        else:
            logger.error("平仓侧补充开仓单失败")
    
    # 再处理平仓单（使用 reduce_only=True）
    if close_orders:
        for is_ask, price, amount in close_orders:
            success, order_id = await trading_state.grid_trading.place_single_order(
                is_ask=is_ask,
                price=price,
                amount=amount,
                reduce_only=True,  # 平仓单使用 Reduce Only，避免部分成交后剩余订单消失
            )
            if success:
                if is_ask:
                    trading_state.sell_orders[order_id] = price
                else:
                    trading_state.buy_orders[order_id] = price
                all_order_ids.append(order_id)
            else:
                logger.error(f"平仓侧补充平仓单失败: is_ask={is_ask}, price={price}")
    
    if all_order_ids:
        logger.info(
            f"平仓侧被吃单补充订单成功: "
            f"开仓单={len(open_orders)}, 平仓单={len(close_orders)}, "
            f"订单ID={all_order_ids}"
        )


async def _calc_next_close_side_open_order() -> Optional[Tuple[bool, float, float]]:
    """
    计算基于平仓侧成交后的补充开仓单 (Buy Back)
    
    Returns:
        (is_ask, price, amount) 元组
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    
    # 做多: 我们卖高了，想低买回来
    # 做空: 我们买低了，想高卖回来

    nearest_open_price = None
    if trading_state.open_orders_count > 0:
        if not OPEN_SIDE_IS_ASK:  # 做多
            nearest_open_price = max(trading_state.open_orders.values())
        else:  # 做空
            nearest_open_price = min(trading_state.open_orders.values())

    if nearest_open_price is None:
        nearest_open_price = trading_state.current_price

    # 计算新的开仓价格
    multiplier = 1 if not OPEN_SIDE_IS_ASK else -1
    new_open_price = round(
        nearest_open_price + (trading_state.active_grid_signle_price * multiplier), 2
    )

    return (OPEN_SIDE_IS_ASK, new_open_price, GRID_CONFIG["GRID_AMOUNT"])


async def _calc_next_close_side_close_order() -> Optional[Tuple[bool, float, float]]:
    """
    计算基于平仓侧成交后的补充平仓单 (Further Profit Taking)
    
    Returns:
        (is_ask, price, amount) 元组
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    CLOSE_SIDE_IS_ASK = grid_state.CLOSE_SIDE_IS_ASK
    
    # 做多: 最高卖 + Step
    # 做空: 最低买 - Step

    furthest_close_price = None
    if trading_state.close_orders_count > 0:
        if not OPEN_SIDE_IS_ASK:  # 做多
            furthest_close_price = max(trading_state.close_orders.values())
        else:  # 做空
            furthest_close_price = min(trading_state.close_orders.values())

    if furthest_close_price is None:
        furthest_close_price = trading_state.current_price

    multiplier = 1 if not OPEN_SIDE_IS_ASK else -1
    new_close_price = round(
        furthest_close_price + (trading_state.active_grid_signle_price * multiplier), 2
    )

    return (CLOSE_SIDE_IS_ASK, new_close_price, GRID_CONFIG["GRID_AMOUNT"])


async def _over_range_replenish_order():
    """
    大间距补单逻辑
    
    当开仓侧和平仓侧之间的间距过大时，在中间补充订单。
    """
    trading_state = grid_state.trading_state
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    
    if trading_state.grid_pause:
        return

    # 获取最近的平仓价格
    nearest_close_price = None
    if trading_state.close_orders_count > 0:
        if not OPEN_SIDE_IS_ASK:  # 做多
            nearest_close_price = min(trading_state.close_orders.values())
        else:  # 做空
            nearest_close_price = max(trading_state.close_orders.values())
    else:
        multiplier = 1 if not OPEN_SIDE_IS_ASK else -1
        nearest_close_price = trading_state.current_price + (
            trading_state.active_grid_signle_price * 2 * multiplier
        )

    # 获取最近的开仓价格
    nearest_open_price = None
    if trading_state.open_orders_count > 0:
        if not OPEN_SIDE_IS_ASK:  # 做多
            nearest_open_price = max(trading_state.open_orders.values())
        else:  # 做空
            nearest_open_price = min(trading_state.open_orders.values())
    else:
        multiplier = -1 if not OPEN_SIDE_IS_ASK else 1
        nearest_open_price = trading_state.current_price + (
            trading_state.active_grid_signle_price * 2 * multiplier
        )

    # 检查间距
    gap = abs(nearest_close_price - nearest_open_price)

    if gap > 2.5 * trading_state.active_grid_signle_price:
        # 间距过大！

        # 1. 补充开仓侧
        dist_to_open = abs(trading_state.current_price - nearest_open_price)
        if dist_to_open > trading_state.active_grid_signle_price * 1.5:
            await _over_range_replenish_open_order(nearest_open_price)

        # 2. 补充平仓侧
        dist_to_close = abs(nearest_close_price - trading_state.current_price)
        if dist_to_close > trading_state.active_grid_signle_price * 1.5:
            if trading_state.available_position_size > 0:
                await _over_range_replenish_close_order(nearest_open_price)


async def _over_range_replenish_open_order(nearest_open_price: float):
    """
    大间距开仓补单
    
    Args:
        nearest_open_price: 最近的开仓价格
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    
    if trading_state.open_orders_count < GRID_CONFIG["MAX_TOTAL_ORDERS"]:
        # 如果上次成交是开仓侧且存在订单，不再补开仓单
        if (
            not trading_state.last_filled_order_is_close_side
            and trading_state.open_orders_count > 0
            and trading_state.close_orders_count > 0
        ):
            return

        multiplier = 1 if not OPEN_SIDE_IS_ASK else -1
        new_price = round(
            nearest_open_price + (trading_state.active_grid_signle_price * multiplier),
            2,
        )

        # 检查当前价格
        if not OPEN_SIDE_IS_ASK:
            if new_price >= trading_state.current_price:
                return
        else:
            if new_price <= trading_state.current_price:
                return

        success, order_id = await trading_state.grid_trading.place_single_order(
            is_ask=OPEN_SIDE_IS_ASK,
            price=new_price,
            amount=GRID_CONFIG["GRID_AMOUNT"],
        )
        if success:
            if OPEN_SIDE_IS_ASK:
                trading_state.sell_orders[order_id] = new_price
            else:
                trading_state.buy_orders[order_id] = new_price
            logger.info(f"大间距开仓补单成功: {order_id}, {new_price}")


async def _over_range_replenish_close_order(nearest_open_price: float):
    """
    大间距平仓补单
    
    Args:
        nearest_open_price: 最近的开仓价格
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    CLOSE_SIDE_IS_ASK = grid_state.CLOSE_SIDE_IS_ASK
    
    if trading_state.paired_close_retry_block_until > time.time():
        remain = round(trading_state.paired_close_retry_block_until - time.time(), 2)
        logger.info(
            "配对平仓单重试窗口内，跳过大间距平仓补单: remain=%ss, target_price=%s",
            remain,
            trading_state.paired_close_target_price,
        )
        return
    
    if (
        trading_state.last_filled_order_is_close_side
        and trading_state.close_orders_count > 0
    ):
        return

    # 计算新的平仓价格
    # 做多: 最高买 + 2 * Step
    # 做空: 最低卖 - 2 * Step

    multiplier = 1 if not OPEN_SIDE_IS_ASK else -1
    new_price = round(
        nearest_open_price + (trading_state.active_grid_signle_price * 2 * multiplier),
        2,
    )

    # 检查当前价格
    if not OPEN_SIDE_IS_ASK:
        if new_price <= trading_state.current_price:
            return
    else:
        if new_price >= trading_state.current_price:
            return

    success, order_id = await trading_state.grid_trading.place_single_order(
        is_ask=CLOSE_SIDE_IS_ASK,
        price=new_price,
        amount=GRID_CONFIG["GRID_AMOUNT"],
        reduce_only=True,  # 平仓单使用 Reduce Only，避免部分成交后剩余订单消失
    )
    if success:
        if CLOSE_SIDE_IS_ASK:
            trading_state.sell_orders[order_id] = new_price
        else:
            trading_state.buy_orders[order_id] = new_price
        logger.info(f"大间距平仓补单成功: {order_id}, {new_price}")


async def _replenish_config_open_orders():
    """
    开仓侧补充不少于配置单的数量
    
    只向远距离补单。
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    
    if trading_state.grid_pause:
        return
    
    while (
        trading_state.open_orders_count < GRID_CONFIG["GRID_COUNT"]
        and trading_state.open_orders_count < GRID_CONFIG["MAX_TOTAL_ORDERS"]
    ):
        # 计算最远的开仓价格
        furthest_open_price = None
        if trading_state.open_orders_count > 0:
            if not OPEN_SIDE_IS_ASK:  # 做多
                furthest_open_price = min(trading_state.open_orders.values())
            else:  # 做空
                furthest_open_price = max(trading_state.open_orders.values())
        
        if furthest_open_price is None:
            # 基于当前价格计算
            multiplier = -1 if not OPEN_SIDE_IS_ASK else 1
            furthest_open_price = trading_state.current_price + (
                trading_state.active_grid_signle_price * multiplier
            )
        
        multiplier = -1 if not OPEN_SIDE_IS_ASK else 1
        new_price = round(
            furthest_open_price + (trading_state.active_grid_signle_price * multiplier),
            2,
        )
        
        # 有效性检查
        if not OPEN_SIDE_IS_ASK:  # 做多：买单价格必须 < 当前价格
            while new_price >= trading_state.current_price:
                new_price = round(
                    new_price - trading_state.active_grid_signle_price, 2
                )
        else:  # 做空：卖单价格必须 > 当前价格
            while new_price <= trading_state.current_price:
                new_price = round(
                    new_price + trading_state.active_grid_signle_price, 2
                )
        
        success, order_id = await trading_state.grid_trading.place_single_order(
            is_ask=OPEN_SIDE_IS_ASK,
            price=new_price,
            amount=GRID_CONFIG["GRID_AMOUNT"],
        )
        if success:
            if OPEN_SIDE_IS_ASK:
                trading_state.sell_orders[order_id] = new_price
            else:
                trading_state.buy_orders[order_id] = new_price
            logger.info(f"补充开仓单成功: {order_id}, 价格={new_price}")
        else:
            logger.error(f"补充开仓单失败，退出循环。价格={new_price}")
            break


async def _replenish_config_close_orders():
    """
    平仓侧补充不少于配置单的数量
    
    只向远距离补单。
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    CLOSE_SIDE_IS_ASK = grid_state.CLOSE_SIDE_IS_ASK
    
    # 使用可承载的“整数平仓单数量”作为上限，避免浮点边界导致反复补/删同一档位订单。
    max_close_orders_by_position = int(
        (trading_state.available_position_size + 1e-9) / GRID_CONFIG["GRID_AMOUNT"]
    )

    while (
        trading_state.close_orders_count < max_close_orders_by_position
        and trading_state.close_orders_count < GRID_CONFIG["MAX_TOTAL_ORDERS"]
    ):
        # 计算最远的平仓价格
        furthest_close_price = None
        if trading_state.close_orders_count > 0:
            if not OPEN_SIDE_IS_ASK:
                furthest_close_price = max(trading_state.close_orders.values())
            else:
                furthest_close_price = min(trading_state.close_orders.values())

        if furthest_close_price is None:
            # 基于最近的开仓价格计算
            if trading_state.open_orders_count > 0:
                if not OPEN_SIDE_IS_ASK:
                    nearest_open = max(trading_state.open_orders.values())
                else:
                    nearest_open = min(trading_state.open_orders.values())
            else:
                nearest_open = trading_state.current_price - (
                    trading_state.active_grid_signle_price
                    * (1 if not OPEN_SIDE_IS_ASK else -1)
                )

            multiplier = 1 if not OPEN_SIDE_IS_ASK else -1
            furthest_close_price = nearest_open + (
                trading_state.active_grid_signle_price * multiplier
            )

        multiplier = 1 if not OPEN_SIDE_IS_ASK else -1
        new_price = round(
            furthest_close_price
            + (trading_state.active_grid_signle_price * multiplier),
            2,
        )

        # 有效性检查
        if not OPEN_SIDE_IS_ASK:
            while new_price <= trading_state.current_price:
                new_price = round(
                    new_price + trading_state.active_grid_signle_price, 2
                )
        else:
            while new_price >= trading_state.current_price:
                new_price = round(
                    new_price - trading_state.active_grid_signle_price, 2
                )

        success, order_id = await trading_state.grid_trading.place_single_order(
            is_ask=CLOSE_SIDE_IS_ASK,
            price=new_price,
            amount=GRID_CONFIG["GRID_AMOUNT"],
            reduce_only=True,  # 平仓单使用 Reduce Only，避免部分成交后剩余订单消失
        )
        if success:
            if CLOSE_SIDE_IS_ASK:
                trading_state.sell_orders[order_id] = new_price
            else:
                trading_state.buy_orders[order_id] = new_price
        else:
            logger.error(f"补充平仓单失败，退出循环。价格={new_price}")
            break

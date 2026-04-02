"""
网格交易补单逻辑模块

包含网格补单的核心逻辑：开仓侧/平仓侧补单、大间距补单等。
"""

import logging
import asyncio
import time
from typing import List, Optional, Tuple

from . import grid_state
from .grid_state import format_price_for_display, round_price_to_precision

logger = logging.getLogger(__name__)


def _get_open_order_price_guard_status() -> Tuple[bool, str, str]:
    """
    返回当前价格保护状态。
    (是否触发, 原因, 动作)
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG or {}

    if not bool(GRID_CONFIG.get("OPEN_ORDER_PRICE_GUARD_ENABLED", False)):
        return False, "", "pause_open_orders"

    current_price = float(trading_state.current_price or 0.0)
    if current_price <= 0:
        return False, "", str(
            GRID_CONFIG.get("OPEN_ORDER_PRICE_GUARD_ACTION", "pause_open_orders")
        )

    min_price = GRID_CONFIG.get("OPEN_ORDER_PRICE_GUARD_MIN_PRICE")
    max_price = GRID_CONFIG.get("OPEN_ORDER_PRICE_GUARD_MAX_PRICE")
    action = str(
        GRID_CONFIG.get("OPEN_ORDER_PRICE_GUARD_ACTION", "pause_open_orders")
    )

    if min_price is not None and current_price <= float(min_price):
        return (
            True,
            f"当前价={format_price_for_display(current_price)} <= 下限={format_price_for_display(float(min_price))}",
            action,
        )

    if max_price is not None and current_price >= float(max_price):
        return (
            True,
            f"当前价={format_price_for_display(current_price)} >= 上限={format_price_for_display(float(max_price))}",
            action,
        )

    return False, "", action


def _announce_open_order_price_guard_state() -> bool:
    """
    价格阈值开关状态变化时打印一次日志，避免每轮重复刷屏。
    """
    trading_state = grid_state.trading_state
    blocked, reason, action = _get_open_order_price_guard_status()
    action_text = (
        "撤销当前挂单并停机"
        if action == "cancel_orders_and_stop"
        else "暂停开仓单"
    )
    if blocked != trading_state.open_order_price_guard_blocked:
        if blocked:
            logger.info(
                "价格保护触发（基于 Nado mark_price）：%s，%s",
                action_text,
                reason,
            )
        else:
            logger.info(
                "价格保护恢复（基于 Nado mark_price）：当前价=%s，恢复开仓单",
                format_price_for_display(float(trading_state.current_price or 0.0)),
            )
        trading_state.open_order_price_guard_blocked = blocked
    return blocked


async def handle_open_order_price_guard_action() -> bool:
    """
    执行价格保护动作。
    返回 True 表示本轮已触发停机，调用方应尽快结束后续逻辑。
    """
    trading_state = grid_state.trading_state
    blocked, reason, action = _get_open_order_price_guard_status()

    if not blocked or action != "cancel_orders_and_stop":
        return False

    if trading_state.open_order_price_guard_stop_triggered:
        return True

    trading_state.open_order_price_guard_stop_triggered = True
    trading_state.open_order_price_guard_blocked = True
    trading_state.stop_reason = f"价格保护触发（基于 Nado mark_price）：{reason}"
    logger.warning(
        "%s，执行撤单并停机",
        trading_state.stop_reason,
    )

    active_order_ids = list(trading_state.buy_orders.keys()) + list(
        trading_state.sell_orders.keys()
    )
    if active_order_ids:
        try:
            from .grid_order import _cancel_orders

            await _cancel_orders(active_order_ids)
        except Exception:
            logger.exception("价格保护停机前撤单失败，继续执行停机")
    else:
        logger.info("价格保护停机：当前无活跃挂单，直接停机")

    trading_state.is_running = False
    return True


def _stage1_paired_close_target_price(fill_price: float, open_side_is_ask: bool, step: float) -> float:
    """
    与 _place_paired_close_order_with_retry 的 stage「1x」首档目标价一致（calc_target_price(1)）。
    """
    if step <= 0:
        return round_price_to_precision(float(fill_price))
    if not open_side_is_ask:
        return round_price_to_precision(float(fill_price) + float(step))
    return round_price_to_precision(float(fill_price) - float(step))


def _close_side_has_order_near_price(
    trading_state,
    target_price: float,
    step: float,
    open_side_is_ask: bool,
) -> bool:
    """
    平仓侧是否已有与 target_price 在 0.5*step 内的挂单。
    与大间距平仓补单 _over_range_replenish_close_order 的容差规则一致。
    """
    if step <= 0:
        return False
    close_side = trading_state.sell_orders if not open_side_is_ask else trading_state.buy_orders
    existing_prices = list(close_side.values())
    if not existing_prices:
        return False
    tol = float(step) * 0.5
    return any(abs(float(p) - float(target_price)) <= tol for p in existing_prices)


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

        open_prices.append(round_price_to_precision(price))

    # 排序价格（从低到高）
    open_prices.sort()

    return open_prices


async def replenish_grid(
    filled_signal: bool,
    trade_price: float = 0.0,
    trade_prices: Optional[List[float]] = None,
    source: str = "WS",
):
    """
    补充网格订单逻辑

    基于原始订单价格分布和当前价格，计算补充订单的价格和方向

    Args:
        filled_signal: 是否有订单成交
        trade_price: 成交价格（单笔时使用）
        trade_prices: 多笔成交价列表（sync 批量消失单时传入，内部按价格容差去重）
        source: 触发来源，用于日志区分 "WS"（WebSocket 成交）或 "REST"（对账/消失单）
    """
    trading_state = grid_state.trading_state
    _announce_open_order_price_guard_state()
    if filled_signal:
        trading_state._replenish_source = source

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
            # 开仓侧被吃单：支持批量成交价，逐笔补单（配对平仓在 _on_open_side_filled 内按首档价做 step 容差去重）
            prices = trade_prices if trade_prices else ([trade_price] if trade_price else [])
            for p in prices:
                if float(p) > 0:
                    await _on_open_side_filled(float(p))
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

    src = getattr(trading_state, "_replenish_source", "WS")
    logger.info("开仓侧被吃单补单 [%s]", src)
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    CLOSE_SIDE_IS_ASK = grid_state.CLOSE_SIDE_IS_ASK
    open_orders = []
    close_order = None

    # 1. 补充开仓单 (继续建仓)
    if (
        not trading_state.grid_pause
        and not _announce_open_order_price_guard_state()
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
            close_price = round_price_to_precision(trade_price + step)
            close_order = (CLOSE_SIDE_IS_ASK, close_price, GRID_CONFIG["GRID_AMOUNT"])
        else:  # 做空：买单价格 = 成交价 - 步长
            close_price = round_price_to_precision(trade_price - step)
            close_order = (CLOSE_SIDE_IS_ASK, close_price, GRID_CONFIG["GRID_AMOUNT"])
        logger.info(f"开仓侧成交后使用回退逻辑挂出配对平仓单: 价格={close_order[1]}")

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

        step_pc = float(trading_state.base_grid_single_price or 0.0)
        if step_pc <= 0:
            step_pc = float(trading_state.active_grid_signle_price or 0.0)
        stage1_target = _stage1_paired_close_target_price(
            fill_price_for_retry, OPEN_SIDE_IS_ASK, step_pc
        )
        if _close_side_has_order_near_price(
            trading_state, stage1_target, step_pc, OPEN_SIDE_IS_ASK
        ):
            logger.info(
                "配对平仓跳过: 首档目标价 %s 在容差 0.5*step(%s) 内已有平仓单，避免同价重复挂单",
                format_price_for_display(stage1_target),
                format_price_for_display(step_pc * 0.5),
            )
        else:
            success, order_id, final_price = await _place_paired_close_order_with_retry(
                is_ask=is_ask,
                fill_price=fill_price_for_retry,
                amount=amount,
            )
            if success:
                trading_state.paired_close_retry_block_until = 0.0
                trading_state.paired_close_target_price = 0.0
                if order_id:
                    if is_ask:
                        trading_state.sell_orders[order_id] = final_price
                    else:
                        trading_state.buy_orders[order_id] = final_price
                    all_order_ids.append(order_id)
                logger.info(
                    f"开仓侧被吃单补充订单成功 [%s]: 开仓单={len(open_orders)}, "
                    f"配对平仓单={1 if order_id else 0}, 订单ID={all_order_ids}",
                    getattr(trading_state, "_replenish_source", "WS"),
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
            return round_price_to_precision(float(fill_price) + step * multiplier)
        # SHORT: 卖出后挂买单
        return round_price_to_precision(float(fill_price) - step * multiplier)

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
                format_price_for_display(float(fill_price)),
                format_price_for_display(step),
                format_price_for_display(current_price),
            )
            continue

        def _is_post_only_cross(err: str, error_code: object) -> bool:
            if error_code is not None and error_code == 2008:
                return True
            e = (err or "").lower()
            return "post-only" in e and ("cross" in e or "crosses" in e)

        def _is_reduce_only_increases_position(err: str, error_code: object) -> bool:
            """仓位为 0 时挂 reduce_only 会被拒，不再重试"""
            if error_code is not None and error_code == 2064:
                return True
            return err and "reduce only" in (err or "").lower() and "increases" in (err or "").lower()

        attempt = 0
        while stage_retry_limit is None or attempt < stage_retry_limit:
            attempt += 1
            target_price = calc_target_price(stage_multiplier)
            trading_state.paired_close_target_price = float(target_price)
            trading_state.paired_close_retry_block_until = time.time() + 20

            try_price = target_price
            max_price_retries = 8
            for price_retry in range(max_price_retries):
                success, order_id, err, error_code = await trading_state.grid_trading.place_single_order(
                    is_ask=is_ask,
                    price=try_price,
                    amount=amount,
                    reduce_only=True,
                )
                if success:
                    logger.info(
                        "配对平仓成功: stage=%s, attempt=%s, order_id=%s, price=%s",
                        stage_name,
                        attempt,
                        order_id,
                        try_price,
                    )
                    return True, order_id, try_price

                if _is_reduce_only_increases_position(err, error_code):
                    logger.info(
                        "配对平仓跳过: 无仓可平(error_code=2064/Reduce only increases position)，不再重试"
                    )
                    return True, "", try_price

                if _is_post_only_cross(err, error_code):
                    current_price = float(trading_state.current_price or 0.0)
                    if current_price and current_price > 0:
                        if not OPEN_SIDE_IS_ASK:
                            try_price = round_price_to_precision(current_price + step * (price_retry + 1))
                        else:
                            try_price = round_price_to_precision(current_price - step * (price_retry + 1))
                        logger.info(
                            "配对平仓 post-only 跨盘调价重试: 原价=%s, 市价=%s, 第%d档价=%s, error_code=%s",
                            format_price_for_display(target_price),
                            format_price_for_display(current_price),
                            price_retry + 1,
                            format_price_for_display(try_price),
                            error_code,
                        )
                        continue
                break

            last_error = "place_single_order_failed"
            logger.warning(
                "配对平仓重试: stage=%s, attempt=%s%s, price=%s, error=%s",
                stage_name,
                attempt,
                "" if stage_retry_limit is None else f"/{stage_retry_limit}",
                try_price,
                last_error,
            )

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
                format_price_for_display(float(fill_price)),
                format_price_for_display(step),
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
    new_price = round_price_to_precision(
        furthest_price + (trading_state.active_grid_signle_price * multiplier)
    )

    # 安全检查：
    # 做多: 新价格必须 < 当前价格
    # 做空: 新价格必须 > 当前价格

    if not OPEN_SIDE_IS_ASK:  # 做多
        while new_price >= trading_state.current_price:
            new_price = round_price_to_precision(new_price - trading_state.active_grid_signle_price)
    else:  # 做空
        while new_price <= trading_state.current_price:
            new_price = round_price_to_precision(new_price + trading_state.active_grid_signle_price)

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
    new_close_price = round_price_to_precision(
        nearest_close_price + (trading_state.base_grid_single_price * multiplier)
    )

    # 4. 使用成交价格覆盖逻辑
    if trade_price > 0:
        # 做多: 成交(买) + Step
        # 做空: 成交(卖) - Step
        price_multiplier = 1 if not OPEN_SIDE_IS_ASK else -1
        new_close_price = round_price_to_precision(
            trade_price + (trading_state.base_grid_single_price * price_multiplier)
        )

    # 5. 间距检查
    diff = abs(new_close_price - trading_state.current_price)
    if diff > trading_state.base_grid_single_price * 2:
        safe_multiplier = 1 if not OPEN_SIDE_IS_ASK else -1
        new_close_price = round_price_to_precision(
            nearest_open_price
            + (
                trading_state.active_grid_signle_price
                + trading_state.base_grid_single_price
            )
            * safe_multiplier
        )

    # 6. 当前价格安全检查
    # 做多: 平仓价格 (卖) 应 > 当前价格；若略低仍挂出，确保买单成交后必有对应卖单
    # 做空: 平仓价格 (买) 应 < 当前价格；若略高仍挂出，确保卖单成交后必有对应买单

    if not OPEN_SIDE_IS_ASK:  # 做多
        if trading_state.current_price < new_close_price:
            return (CLOSE_SIDE_IS_ASK, new_close_price, GRID_CONFIG["GRID_AMOUNT"])
        # 放宽：当前价略高于计算卖价时，仍用成交价+步长挂出卖单，保证买单成交必有配对卖单
        if trade_price > 0:
            fallback_price = round_price_to_precision(
                trade_price + trading_state.base_grid_single_price
            )
            if fallback_price > trading_state.current_price:
                return (CLOSE_SIDE_IS_ASK, fallback_price, GRID_CONFIG["GRID_AMOUNT"])
            return (CLOSE_SIDE_IS_ASK, new_close_price, GRID_CONFIG["GRID_AMOUNT"])
    else:  # 做空
        if trading_state.current_price > new_close_price:
            return (CLOSE_SIDE_IS_ASK, new_close_price, GRID_CONFIG["GRID_AMOUNT"])
        if trade_price > 0:
            fallback_price = round_price_to_precision(
                trade_price - trading_state.base_grid_single_price
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
    # 若计算出的开仓价等于刚成交的平仓价，则跳过，避免同价买卖 round-trip 浪费手续费
    if not trading_state.grid_pause and not _announce_open_order_price_guard_state():
        new_open_order = await _calc_next_close_side_open_order()
        if new_open_order:
            _is_ask, new_open_price, _ = new_open_order
            if round_price_to_precision(new_open_price) != round_price_to_precision(trading_state.last_trade_price):
                open_orders.append(new_open_order)
            else:
                logger.info(
                    "平仓侧被吃单补单: 开仓价 %s 与刚成交平仓价相同，跳过以避免同价 round-trip",
                    format_price_for_display(new_open_price),
                )

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
            success, order_id, _, _ = await trading_state.grid_trading.place_single_order(
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
        open_prices = [o[1] for o in open_orders]
        close_prices = [c[1] for c in close_orders]
        logger.info(
            f"平仓侧被吃单补充订单成功: "
            f"开仓单={len(open_orders)}(价格={open_prices}), 平仓单={len(close_orders)}(价格={close_prices}), "
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
    new_open_price = round_price_to_precision(
        nearest_open_price + (trading_state.active_grid_signle_price * multiplier)
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
    new_close_price = round_price_to_precision(
        furthest_close_price + (trading_state.active_grid_signle_price * multiplier)
    )

    return (CLOSE_SIDE_IS_ASK, new_close_price, GRID_CONFIG["GRID_AMOUNT"])


async def _over_range_replenish_order():
    """
    大间距补单逻辑
    
    当开仓侧和平仓侧之间的间距过大时，在中间补充订单。
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
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

    # 无卖单时追单：开仓单离当前价过远，取消最远单并在靠近当前价处补单（使用 OVER_RANGE_GAP_MULTIPLIER）
    if trading_state.close_orders_count == 0 and trading_state.open_orders_count > 0:
        step = trading_state.active_grid_signle_price
        gap_mult = float(GRID_CONFIG.get("OVER_RANGE_GAP_MULTIPLIER", 2.5))
        if not OPEN_SIDE_IS_ASK:  # 做多：最远买单 = 最低价
            farthest_open = min(trading_state.open_orders.values())
            dist = trading_state.current_price - farthest_open
        else:  # 做空：最远卖单 = 最高价
            farthest_open = max(trading_state.open_orders.values())
            dist = farthest_open - trading_state.current_price
        if dist > step * gap_mult:
            await _over_range_trailing_open_order()

    # 检查开平仓间距
    gap = abs(nearest_close_price - nearest_open_price)
    gap_multiplier = float(GRID_CONFIG.get("OVER_RANGE_GAP_MULTIPLIER", 2.5))
    gap_threshold = gap_multiplier * trading_state.active_grid_signle_price

    logger.debug(
        "大间距检测: gap=%s, threshold=%s, nearest_open=%s, nearest_close=%s, 当前价=%s",
        format_price_for_display(gap),
        format_price_for_display(gap_threshold),
        format_price_for_display(nearest_open_price),
        format_price_for_display(nearest_close_price),
        format_price_for_display(trading_state.current_price or 0),
    )

    if gap > gap_threshold:
        # 间距过大！
        step = trading_state.active_grid_signle_price
        min_dist = step * 1.5
        dist_to_open = abs(trading_state.current_price - nearest_open_price) if trading_state.close_orders_count > 0 else 0.0
        dist_to_close = abs(nearest_close_price - trading_state.current_price)
        need_for_one_more = (trading_state.close_orders_count + 1) * GRID_CONFIG["GRID_AMOUNT"]
        can_add_close = trading_state.available_position_size >= need_for_one_more

        logger.info(
            "大间距触发: gap=%s > threshold=%s, 开仓数=%s, 平仓数=%s",
            format_price_for_display(gap),
            format_price_for_display(gap_threshold),
            trading_state.open_orders_count,
            trading_state.close_orders_count,
        )

        tried_open = False
        tried_close = False

        # 1. 补充开仓侧（仅当有卖单时，无卖单时由上方追单处理）
        if trading_state.close_orders_count > 0:
            if dist_to_open > min_dist:
                logger.info("大间距: 尝试补充开仓侧, dist_to_open=%s", format_price_for_display(dist_to_open))
                await _over_range_replenish_open_order(nearest_open_price)
                tried_open = True
            else:
                logger.debug("大间距: 跳过开仓侧补单, dist_to_open=%s <= 1.5*step", format_price_for_display(dist_to_open))

        # 2. 补充平仓侧：可用须能容纳「当前网格平仓单数 + 1」格，否则补单会导致可平仓量>持仓（做多变净空）
        if dist_to_close > min_dist:
            if can_add_close:
                logger.info("大间距: 尝试补充平仓侧, dist_to_close=%s", format_price_for_display(dist_to_close))
                await _over_range_replenish_close_order(nearest_open_price)
                tried_close = True
            else:
                logger.debug(
                    "大间距: 跳过平仓侧补单, 可用=%s < need=%s",
                    format_price_for_display(trading_state.available_position_size),
                    format_price_for_display(need_for_one_more),
                )

        if not tried_open and not tried_close:
            reasons = []
            if trading_state.close_orders_count > 0 and dist_to_open <= min_dist:
                reasons.append("开仓侧: 当前价距最近开仓价=%s <= 1.5*step=%s" % (format_price_for_display(dist_to_open), format_price_for_display(min_dist)))
            if dist_to_close <= min_dist:
                reasons.append("平仓侧: 当前价距最近平仓价=%s <= 1.5*step=%s" % (format_price_for_display(dist_to_close), format_price_for_display(min_dist)))
            if dist_to_close > min_dist and not can_add_close:
                reasons.append("平仓侧: 可用仓位=%s < 需再挂一格=%s" % (format_price_for_display(trading_state.available_position_size), format_price_for_display(need_for_one_more)))
            logger.info("大间距触发但未补单: %s", "; ".join(reasons))


async def _over_range_replenish_open_order(nearest_open_price: float):
    """
    大间距开仓补单
    
    Args:
        nearest_open_price: 最近的开仓价格
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK

    if _announce_open_order_price_guard_state():
        return
    
    if trading_state.open_orders_count >= GRID_CONFIG["MAX_TOTAL_ORDERS"]:
        logger.debug("大间距开仓补单: 跳过, 开仓单数=%s >= MAX_TOTAL_ORDERS=%s", trading_state.open_orders_count, GRID_CONFIG["MAX_TOTAL_ORDERS"])
        return

    # 大间距时允许补开仓单；若该笔成交，后续会按「开仓侧被吃单补单」挂出配对平仓单，无需因「上次成交是开仓侧」而跳过
    multiplier = 1 if not OPEN_SIDE_IS_ASK else -1
    new_price = round_price_to_precision(
        nearest_open_price + (trading_state.active_grid_signle_price * multiplier)
    )

    # 若该价格已有开仓单（例如初始化刚挂的），则不再补，避免重复挂单
    if new_price in trading_state.open_orders.values():
        logger.debug("大间距开仓补单: 跳过, 价格%s已有开仓单", format_price_for_display(new_price))
        return

    # 检查当前价格
    if not OPEN_SIDE_IS_ASK:
        if new_price >= trading_state.current_price:
            logger.debug("大间距开仓补单: 跳过(做多), 新价%s >= 当前价%s", format_price_for_display(new_price), format_price_for_display(trading_state.current_price or 0))
            return
    else:
        if new_price <= trading_state.current_price:
            logger.debug("大间距开仓补单: 跳过(做空), 新价%s <= 当前价%s", format_price_for_display(new_price), format_price_for_display(trading_state.current_price or 0))
            return

    success, order_id, _, _ = await trading_state.grid_trading.place_single_order(
        is_ask=OPEN_SIDE_IS_ASK,
        price=new_price,
        amount=GRID_CONFIG["GRID_AMOUNT"],
    )
    if success:
        if OPEN_SIDE_IS_ASK:
            trading_state.sell_orders[order_id] = new_price
        else:
            trading_state.buy_orders[order_id] = new_price
        logger.info("大间距开仓补单成功: %s, %s", order_id, format_price_for_display(new_price))


async def _over_range_trailing_open_order():
    """
    无卖单时追单：开仓单离当前价过远，取消最远单并在靠近当前价处补单。
    """
    from exchanges.order_converter import normalize_order_to_ccxt
    from .grid_order import _cancel_orders

    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK

    if _announce_open_order_price_guard_state():
        return

    if not trading_state.current_price or trading_state.open_orders_count == 0:
        return

    step = trading_state.active_grid_signle_price
    current_price = trading_state.current_price

    if not OPEN_SIDE_IS_ASK:
        furthest_price = min(trading_state.open_orders.values())
        candidates = [(oid, p) for oid, p in trading_state.open_orders.items() if p == furthest_price]
    else:
        furthest_price = max(trading_state.open_orders.values())
        candidates = [(oid, p) for oid, p in trading_state.open_orders.items() if p == furthest_price]

    if not candidates:
        return

    order_id, order_price = candidates[0]
    if order_id in getattr(trading_state, "pause_orders", {}):
        return

    if not OPEN_SIDE_IS_ASK:
        nearest = max(trading_state.open_orders.values())
        new_price = round_price_to_precision(nearest + step)
        # 做多：新买单必须低于市价；若算出的价已≥当前价，改为当前价下方一档，实现“跟价”挂单
        if new_price >= current_price:
            new_price = round_price_to_precision(current_price - step)
    else:
        nearest = min(trading_state.open_orders.values())
        new_price = round_price_to_precision(nearest - step)
        # 做空：新卖单必须高于市价；若算出的价已≤当前价，改为当前价上方一档
        if new_price <= current_price:
            new_price = round_price_to_precision(current_price + step)

    if new_price <= 0:
        return

    existing_prices = set(trading_state.open_orders.values())
    if any(abs(new_price - p) < step * 0.5 for p in existing_prices if p != order_price):
        return

    if not OPEN_SIDE_IS_ASK and new_price >= current_price:
        return
    if OPEN_SIDE_IS_ASK and new_price <= current_price:
        return

    try:
        order_amount = GRID_CONFIG["GRID_AMOUNT"]
        orders = await trading_state.grid_trading.get_orders_by_rest()
        if orders:
            for order in orders:
                normalized = normalize_order_to_ccxt(order)
                oid = str(normalized.get("clientOrderId") or normalized.get("id", ""))
                if oid == str(order_id):
                    order_amount = float(normalized.get("amount", GRID_CONFIG["GRID_AMOUNT"]))
                    break

        await _cancel_orders([str(order_id)])

        success, new_order_id, _, _ = await trading_state.grid_trading.place_single_order(
            is_ask=OPEN_SIDE_IS_ASK,
            price=new_price,
            amount=order_amount,
        )
        if success:
            if OPEN_SIDE_IS_ASK:
                trading_state.sell_orders[new_order_id] = new_price
            else:
                trading_state.buy_orders[new_order_id] = new_price
            logger.info(
                "大间距追单成功: 原订单ID=%s, 新订单ID=%s, 原价格=%s, 新价格=%s, 当前价=%s",
                order_id,
                new_order_id,
                format_price_for_display(order_price),
                format_price_for_display(new_price),
                format_price_for_display(current_price),
            )
        else:
            logger.warning("大间距追单重新下单失败: 原订单ID=%s, 新价格=%s", order_id, format_price_for_display(new_price))
    except Exception as e:
        logger.error("大间距追单异常: 订单ID=%s, 错误=%s", order_id, e, exc_info=True)


async def _over_range_replenish_close_order(nearest_open_price: float):
    """
    大间距平仓补单。
    若计算出的 new_price 与已有平仓单（含本轮刚挂的配对卖单）同价则跳过，避免重复挂单。
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
    step = float(trading_state.active_grid_signle_price or trading_state.base_grid_single_price or 0)
    new_price = round_price_to_precision(
        nearest_open_price + (step * 2 * multiplier)
    )

    # 仅当 new_price 与已有平仓单「同档」（约 0.5*step 内）才跳过，避免重复挂单。
    # 若用整 step 判断，会误判：例如仅有卖单 2123.9、目标 2122 时 |2123.9-2122|<step 被跳过，
    # 导致 2119.7 与 2123.9 之间缺一档（2121.9/2122），大间距无法填补。
    close_side_orders = (
        trading_state.sell_orders if not OPEN_SIDE_IS_ASK else trading_state.buy_orders
    )
    existing_prices = list(close_side_orders.values())
    same_level_tolerance = step * 0.5
    if step > 0 and any(abs(float(p) - new_price) <= same_level_tolerance for p in existing_prices):
        logger.info(
            "大间距平仓补单跳过: 目标价 %s 的 0.5*step(%s) 内已有平仓单，无需重复挂单",
            format_price_for_display(new_price),
            format_price_for_display(same_level_tolerance),
        )
        return

    # 检查当前价格
    if not OPEN_SIDE_IS_ASK:
        if new_price <= trading_state.current_price:
            return
    else:
        if new_price >= trading_state.current_price:
            return

    success, order_id, _, _ = await trading_state.grid_trading.place_single_order(
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
        logger.info("大间距平仓补单成功: %s, %s", order_id, format_price_for_display(new_price))


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

    if _announce_open_order_price_guard_state():
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
        new_price = round_price_to_precision(
            furthest_open_price + (trading_state.active_grid_signle_price * multiplier)
        )
        
        # 有效性检查
        if not OPEN_SIDE_IS_ASK:  # 做多：买单价格必须 < 当前价格
            while new_price >= trading_state.current_price:
                new_price = round_price_to_precision(
                    new_price - trading_state.active_grid_signle_price
                )
        else:  # 做空：卖单价格必须 > 当前价格
            while new_price <= trading_state.current_price:
                new_price = round_price_to_precision(
                    new_price + trading_state.active_grid_signle_price
                )
        
        success, order_id, _, _ = await trading_state.grid_trading.place_single_order(
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
    平仓侧补充：仅在「当前没有任何平仓单」时按配置补单（如重启后）。
    已有平仓单时，新卖单只应由「买单成交后的配对平仓」产生，不在此处按任意价插单。
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    CLOSE_SIDE_IS_ASK = grid_state.CLOSE_SIDE_IS_ASK
    
    max_close_orders_by_position = int(
        (trading_state.available_position_size + 1e-9) / GRID_CONFIG["GRID_AMOUNT"]
    )

    step = trading_state.active_grid_signle_price
    close_prices_set = set(round_price_to_precision(p) for p in trading_state.close_orders.values()) if trading_state.close_orders_count > 0 else set()

    # 已有平仓单时不再按“配置数量”补单，避免在中间插入与买单成交无关的价位
    if trading_state.close_orders_count > 0:
        return

    while (
        trading_state.close_orders_count < max_close_orders_by_position
        and trading_state.close_orders_count < GRID_CONFIG["MAX_TOTAL_ORDERS"]
    ):
        # 优先在「靠近当前价」补平仓单（最近开仓价起逐档 +step），便于成交、赚低买高卖差价；避免补在最远卖价外（如 119+）难以成交
        new_price = None
        nearest_open_price = None
        if trading_state.open_orders_count > 0:
            if not OPEN_SIDE_IS_ASK:
                nearest_open_price = max(trading_state.open_orders.values())
            else:
                nearest_open_price = min(trading_state.open_orders.values())
        if nearest_open_price is not None:
            mult = 1 if not OPEN_SIDE_IS_ASK else -1
            for k in range(1, 20):  # 最多尝试 20 档，避免死循环
                candidate = round_price_to_precision(nearest_open_price + step * k * mult)
                current_ok = (
                    (not OPEN_SIDE_IS_ASK and candidate > trading_state.current_price)
                    or (OPEN_SIDE_IS_ASK and candidate < trading_state.current_price)
                )
                # 若该价等于上次开仓成交价，补平仓单会形成同价买卖 round-trip，浪费手续费
                if (
                    current_ok
                    and candidate not in close_prices_set
                    and not (
                        round_price_to_precision(trading_state.last_trade_price) == candidate
                        and not trading_state.last_filled_order_is_close_side
                    )
                ):
                    new_price = candidate
                    break

        if new_price is None:
            # 回退：按原逻辑在最远平仓价外补一档
            furthest_close_price = None
            if trading_state.close_orders_count > 0:
                if not OPEN_SIDE_IS_ASK:
                    furthest_close_price = max(trading_state.close_orders.values())
                else:
                    furthest_close_price = min(trading_state.close_orders.values())

            if furthest_close_price is None:
                if trading_state.open_orders_count > 0:
                    if not OPEN_SIDE_IS_ASK:
                        nearest_open = max(trading_state.open_orders.values())
                    else:
                        nearest_open = min(trading_state.open_orders.values())
                else:
                    nearest_open = trading_state.current_price - (
                        step * (1 if not OPEN_SIDE_IS_ASK else -1)
                    )
                multiplier = 1 if not OPEN_SIDE_IS_ASK else -1
                furthest_close_price = nearest_open + (step * multiplier)

            multiplier = 1 if not OPEN_SIDE_IS_ASK else -1
            new_price = round_price_to_precision(
                furthest_close_price + (step * multiplier)
            )

        # 有效性检查
        if not OPEN_SIDE_IS_ASK:
            while new_price <= trading_state.current_price:
                new_price = round_price_to_precision(new_price + step)
        else:
            while new_price >= trading_state.current_price:
                new_price = round_price_to_precision(new_price - step)

        success, order_id, _, _ = await trading_state.grid_trading.place_single_order(
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
            close_prices_set.add(round_price_to_precision(new_price))
        else:
            logger.error(f"补充平仓单失败，退出循环。价格={new_price}")
            break

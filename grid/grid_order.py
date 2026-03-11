"""
网格交易订单管理模块

包含订单检查、取消、同步和成交处理。
"""

import asyncio
import logging
import time
from typing import List

from . import grid_state
from exchanges.order_converter import normalize_order_to_ccxt

logger = logging.getLogger(__name__)


async def _resolve_fill_order_id_with_retry(
    raw_order_id: str,
    is_ask: bool,
    price: float,
    filled_amount: float,
    status: str,
    trading_state,
) -> str:
    """
    对未知 fill 订单ID先进行短暂重试（等待映射同步），再降级到 side+price 匹配。
    """
    active_buy = trading_state.buy_orders
    active_sell = trading_state.sell_orders
    if raw_order_id in active_buy or raw_order_id in active_sell:
        return raw_order_id

    if not (filled_amount > 0 and status in ["open", "closed", "filled"]):
        return raw_order_id

    exchange = getattr(getattr(trading_state, "grid_trading", None), "exchange", None)

    async def _try_resolve_once() -> str:
        # 1) 直接通过 adapter 的 digest->client_order_id 映射反查
        if (
            isinstance(raw_order_id, str)
            and raw_order_id.startswith("0x")
            and exchange is not None
            and hasattr(exchange, "order_digests")
        ):
            for cid, dg in exchange.order_digests.items():
                if dg == raw_order_id and (cid in active_buy or cid in active_sell):
                    return cid

        # 2) 再做 side+price 的唯一匹配
        guessed = _match_order_id_by_side_price(
            is_ask=is_ask,
            price=float(price),
            trading_state=trading_state,
            tolerance=0.01,
        )
        if guessed:
            return guessed
        return ""

    # 先重试几次，给 websocket/rest 同步一点时间
    for _ in range(3):
        resolved = await _try_resolve_once()
        if resolved:
            if resolved != raw_order_id:
                logger.warning(
                    "fill事件订单ID延迟解析成功: raw_id=%s -> matched_id=%s, side=%s, price=%s",
                    raw_order_id,
                    resolved,
                    "sell" if is_ask else "buy",
                    price,
                )
            return resolved
        await asyncio.sleep(0.35)

    return raw_order_id


def _match_order_id_by_side_price(
    is_ask: bool,
    price: float,
    trading_state,
    tolerance: float = 0.01,
) -> str:
    """
    当回调里没有可用 clientOrderId（例如只拿到 digest）时，
    使用 side + price 在活跃订单中做保守唯一匹配。
    """
    candidates = trading_state.sell_orders if is_ask else trading_state.buy_orders
    matches = []
    for oid, p in candidates.items():
        try:
            if abs(float(p) - float(price)) <= tolerance:
                matches.append((oid, abs(float(p) - float(price))))
        except Exception:
            continue

    if len(matches) == 1:
        return matches[0][0]
    return ""


async def check_order_fills(orders: dict):
    """
    检查订单成交情况
    
    Args:
        orders: 订单列表
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    replenish_grid_lock = grid_state.replenish_grid_lock
    
    for order in orders:
        # 从 CCXT 格式提取字段
        client_order_index = str(order.get("clientOrderId") or order.get("id", ""))
        status = order.get("status")
        side = order.get("side", "buy")  # 'buy' or 'sell'
        price = order.get("price", 0)
        filled_amount = float(order.get("filled", 0))
        initial_base_amount = float(order.get("amount", 0))

        is_ask = side == "sell"

        # 兜底：若回调只带 digest 或未知ID，先短暂重试解析，再做保守匹配。
        if client_order_index not in trading_state.buy_orders and client_order_index not in trading_state.sell_orders:
            client_order_index = await _resolve_fill_order_id_with_retry(
                raw_order_id=client_order_index,
                is_ask=is_ask,
                price=float(price),
                filled_amount=filled_amount,
                status=status,
                trading_state=trading_state,
            )

        # 判断是开仓侧还是平仓侧订单
        if OPEN_SIDE_IS_ASK:  # 做空策略
            is_open_side_order = is_ask
            is_close_side_order = not is_ask
        else:  # 做多策略
            is_open_side_order = not is_ask
            is_close_side_order = is_ask

        # 过滤非网格订单 (占位订单等)
        if initial_base_amount > GRID_CONFIG["GRID_AMOUNT"]:
            continue
        
        # 如果是已知的占位订单，也忽略 (防止update消息中amount为0导致的误判)
        if client_order_index in trading_state.pause_orders:
            continue

        # 记录是否需要补单，如果不在列表中，有可能是直接成交，则不补单
        replenish = False

        remaining_amount = float(order.get("remaining", initial_base_amount - filled_amount))
        
        logger.info(
            f"检查订单: ID={client_order_index}, 方向={side}, "
            f"价格={price}, 状态={status}, 成交量={filled_amount}, "
            f"总数量={initial_base_amount}, 剩余={remaining_amount}"
        )

        async with replenish_grid_lock:
            # 检测部分成交情况：订单有成交但状态不是 closed/filled
            is_partially_filled = (
                filled_amount > 0 
                and filled_amount < initial_base_amount 
                and status not in ["closed", "filled"]
            )
            
            # 检测完全成交或部分成交后剩余部分被取消的情况
            is_fully_filled_or_cancelled = (
                filled_amount > 0 
                and remaining_amount == 0 
                and status not in ["closed", "filled"]
            )
            
            if status in ["open"]:
                if is_ask:
                    trading_state.sell_orders[client_order_index] = float(price)
                else:
                    trading_state.buy_orders[client_order_index] = float(price)
            
            # 处理部分成交的情况
            if is_partially_filled:
                logger.warning(
                    f"检测到部分成交订单: ID={client_order_index}, "
                    f"已成交={filled_amount}, 剩余={remaining_amount}, 状态={status}"
                )
                # 如果订单在活跃列表中，需要处理部分成交
                if is_ask and client_order_index in trading_state.sell_orders:
                    # 部分成交的订单仍然在活跃列表中，暂时不删除
                    # 但如果剩余数量很小（小于最小交易单位），可以视为完全成交
                    if remaining_amount < GRID_CONFIG["GRID_AMOUNT"] * 0.1:  # 剩余小于10%视为完全成交
                        logger.info(
                            f"部分成交订单剩余量过小，视为完全成交: ID={client_order_index}, "
                            f"剩余={remaining_amount}"
                        )
                        trading_state.last_filled_order_is_close_side = is_close_side_order
                        trading_state.last_trade_price = float(price)
                        trading_state.filled_count += 1
                        del trading_state.sell_orders[client_order_index]
                        replenish = True
                elif not is_ask and client_order_index in trading_state.buy_orders:
                    if remaining_amount < GRID_CONFIG["GRID_AMOUNT"] * 0.1:
                        logger.info(
                            f"部分成交订单剩余量过小，视为完全成交: ID={client_order_index}, "
                            f"剩余={remaining_amount}"
                        )
                        trading_state.last_filled_order_is_close_side = is_close_side_order
                        trading_state.last_trade_price = float(price)
                        trading_state.filled_count += 1
                        del trading_state.buy_orders[client_order_index]
                        replenish = True

            # 如果订单已完全成交（状态为 closed/filled 或剩余为0）
            if (status in ["closed", "filled"] or is_fully_filled_or_cancelled) and filled_amount > 0:
                # 平仓单若已在同步路径记为成交，仅从列表移除，不重复更新状态与补单
                synced_close = getattr(trading_state, "replenished_by_sync_close_order_ids", None)
                if is_close_side_order and synced_close and client_order_index in synced_close:
                    synced_close.discard(client_order_index)
                    if is_ask and client_order_index in trading_state.sell_orders:
                        del trading_state.sell_orders[client_order_index]
                    elif not is_ask and client_order_index in trading_state.buy_orders:
                        del trading_state.buy_orders[client_order_index]
                    replenish = False
                    logger.info("平仓单成交已在同步路径处理，跳过 fill 事件重复更新: ID=%s", client_order_index)
                else:
                    trading_state.filled_count += 1
                    trading_state.last_trade_price = float(price)
                    trading_state.last_filled_order_is_close_side = is_close_side_order

                    if is_ask:
                        if client_order_index in trading_state.sell_orders:
                            del trading_state.sell_orders[client_order_index]
                            logger.info(
                                f"从活跃卖单订单列表删除订单ID={client_order_index}, 价格={price}, "
                                f"已成交={filled_amount}"
                            )
                            replenish = True
                    else:
                        if client_order_index in trading_state.buy_orders:
                            del trading_state.buy_orders[client_order_index]
                            logger.info(
                                f"从活跃买单订单列表删除订单ID={client_order_index}, 价格={price}, "
                                f"已成交={filled_amount}"
                            )
                            replenish = True

                # 如果是平仓单（Close Side）成交（且未在同步路径处理过）
                if is_close_side_order and replenish:
                    # 按实际成交数量计算仓位变化，而不是固定的 GRID_AMOUNT
                    actual_filled = min(filled_amount, GRID_CONFIG["GRID_AMOUNT"])
                    trading_state.available_position_size = round(
                        trading_state.available_position_size - actual_filled,
                        2,
                    )

                    # 按实际成交数量计算收益
                    once_profit = (
                        trading_state.base_grid_single_price * actual_filled
                    )
                    trading_state.active_profit += once_profit
                    trading_state.total_profit += once_profit
                    trading_state.available_reduce_profit += once_profit
                    
                    logger.info(
                        f"平仓单成交处理: 订单ID={client_order_index}, "
                        f"实际成交={actual_filled}, 仓位减少={actual_filled}, "
                        f"收益={once_profit}"
                    )

        # 在锁范围外补充网格订单（若同步路径已按「消失开仓单」补过单则跳过，避免同笔成交补两次卖单）
        if replenish:
            skip_replenish = False
            if is_open_side_order:
                synced_ids = getattr(trading_state, "replenished_by_sync_open_order_ids", None)
                if synced_ids is not None and client_order_index in synced_ids:
                    synced_ids.discard(client_order_index)
                    skip_replenish = True
                    logger.info("开仓单成交已在同步路径补单，跳过 fill 事件补单: ID=%s", client_order_index)
            if not skip_replenish:
                from .grid_replenish import replenish_grid
                async with replenish_grid_lock:
                    await replenish_grid(True, float(price))
                    trading_state.last_replenish_time = time.time()


async def check_current_orders(position_delta: float = 0.0):
    """
    检查当前订单是否合理：
    如果有一侧订单过多，取消最远的订单

    Args:
        position_delta: 仓位增量（本轮 - 上轮），用于判断消失的开仓订单是否为成交
    """
    # 优先同步最新订单状态，确保 pause_orders 和 active orders 正确分类
    await _sync_current_orders(position_delta=position_delta)

    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK

    # 同步后按最新占位重算可用仓位，避免与 REST 占位状态不一致
    from .grid_risk import _get_current_pause_position
    current_pause = await _get_current_pause_position()
    trading_state.available_position_size = round(
        trading_state.current_position_size - current_pause, 2
    )

    # 如果 Open Side 订单过多，取消最远的订单
    if trading_state.open_orders_count > GRID_CONFIG["GRID_COUNT"] + 1:
        logger.info(f"开仓侧订单过多，删除多余订单")
        cancel_orders = []

        # 排序订单
        # 做多：买单，最远的是最低价，正序排列取前N个
        # 做空：卖单，最远的是最高价，逆序排列取前N个
        reverse_sort = OPEN_SIDE_IS_ASK
        sorted_orders = sorted(
            trading_state.open_orders.items(), 
            key=lambda item: item[1], 
            reverse=reverse_sort
        )

        cancel_count = trading_state.open_orders_count - (GRID_CONFIG["GRID_COUNT"] + 1)
        orders_to_iter = dict(sorted_orders)

        for order_id, price in orders_to_iter.items():
            if len(cancel_orders) < cancel_count:
                cancel_orders.append(order_id)
                logger.info(f"取消最远开仓单，价格={price}, 订单ID={order_id}")
            else:
                break

        await _cancel_orders(cancel_orders)

    # 如果 Close Side 订单过多
    if trading_state.close_orders_count > GRID_CONFIG["MAX_TOTAL_ORDERS"]:
        cancel_orders = []
        
        # 做多：卖单，最远的是最高价，逆序排列
        # 做空：买单，最远的是最低价，正序排列
        reverse_sort = not OPEN_SIDE_IS_ASK
        sorted_orders = sorted(
            trading_state.close_orders.items(), 
            key=lambda item: item[1], 
            reverse=reverse_sort
        )

        cancel_count = trading_state.close_orders_count - GRID_CONFIG["MAX_TOTAL_ORDERS"] + 2

        for order_id, price in dict(sorted_orders).items():
            # 双重保护：如果该订单是占位订单，绝对不取消
            if order_id in trading_state.pause_orders:
                continue
                
            if len(cancel_orders) < cancel_count:
                cancel_orders.append(order_id)
                logger.info(f"取消最远平仓单，价格={price}, 订单ID={order_id}")
            else:
                break

        await _cancel_orders(cancel_orders)

    # 平仓侧订单不能超过持仓量 (Position Sizing check)，仅在有网格平仓单时修剪（占位订单不参与）
    # 可用仓位为负时不按“超过持仓”修剪，避免可承载=-8 等误判导致误删卖单
    # 可承载数量用 (available+1e-9)/GRID_AMOUNT 取整，避免 0.3/0.1 浮点成 2 导致误修剪
    if trading_state.available_position_size <= 0:
        pass  # 不执行下方修剪
    else:
        max_close_by_position = int(
            (trading_state.available_position_size + 1e-9) / GRID_CONFIG["GRID_AMOUNT"]
        )
        if (
            trading_state.close_orders_count > 0
            and trading_state.close_orders_count > max_close_by_position
            and (time.time() - trading_state.start_time) > 60
        ):
            logger.info(
                "平仓单总量超过持仓，进行修剪: 平仓单数=%s, 可承载=%s, 可用仓位=%s",
                trading_state.close_orders_count,
                max_close_by_position,
                round(trading_state.available_position_size, 6),
            )
            cancel_orders = []
            reverse_sort = not OPEN_SIDE_IS_ASK
            sorted_orders = sorted(
                trading_state.close_orders.items(),
                key=lambda item: item[1],
                reverse=reverse_sort
            )
            cancel_count = trading_state.close_orders_count - max_close_by_position
            if cancel_count > 0:
                for order_id, price in dict(sorted_orders).items():
                    if order_id in trading_state.pause_orders:
                        continue
                    if len(cancel_orders) < cancel_count:
                        cancel_orders.append(order_id)
                        logger.info(f"取消最远平仓单(超出持仓)，价格={price}, 订单ID={order_id}")
                    else:
                        break
                await _cancel_orders(cancel_orders)

    # 交易暂停清理
    if trading_state.grid_pause:
        if len(trading_state.buy_orders) > 0:
            await _cancel_orders(list(trading_state.buy_orders.keys()))
        if len(trading_state.sell_orders) > 0:
            await _cancel_orders(list(trading_state.sell_orders.keys()))

    # 检查重复订单
    await _check_duplicate_orders(trading_state.buy_orders)
    await _check_duplicate_orders(trading_state.sell_orders)


async def _check_duplicate_orders(orders: dict):
    """
    检查并取消重复价格的订单
    
    Args:
        orders: 订单字典
    """
    if len(orders) > 0:
        cancel_orders = []
        sorted_orders = dict(sorted(orders.copy().items(), key=lambda item: item[1]))
        prev_price = None
        for order_id, price in sorted_orders.items():
            if prev_price is not None and round(price, 4) == round(prev_price, 4):
                cancel_orders.append(order_id)
                logger.info(f"检测到重复价格订单，删除ID={order_id}, 价格={price}")
            prev_price = price
        if len(cancel_orders) > 0:
            await _cancel_orders(cancel_orders)


async def _cancel_orders(cancel_orders: List[int]):
    """
    批量取消订单
    
    Args:
        cancel_orders: 要取消的订单ID列表
    """
    trading_state = grid_state.trading_state
    
    if not cancel_orders:
        return
    success = await trading_state.grid_trading.cancel_grid_orders(cancel_orders)
    if success:
        for order_id in cancel_orders:
            if order_id in trading_state.buy_orders:
                del trading_state.buy_orders[order_id]
            if order_id in trading_state.sell_orders:
                del trading_state.sell_orders[order_id]
            # 若取消的是占位单，同步更新 pause_orders / pause_positions
            if order_id in trading_state.pause_orders:
                info = trading_state.pause_orders[order_id]
                price, amount = info["price"], info["amount"]
                del trading_state.pause_orders[order_id]
                if price in trading_state.pause_positions:
                    trading_state.pause_positions[price] = round(
                        trading_state.pause_positions[price] - amount, 6
                    )
                    if trading_state.pause_positions[price] <= 0:
                        del trading_state.pause_positions[price]
        logger.info(f"批量取消订单成功: {len(cancel_orders)}个")


async def _sync_current_orders(position_delta: float = 0.0):
    """
    同步订单状态（通过 REST API 核对当前订单列表）
    检测订单消失并处理部分成交的情况

    Args:
        position_delta: 仓位增量（本轮 - 上轮），用于判断消失的开仓订单是否为成交

    重要说明：
    - 占位订单（pause_orders）是熔断时用于回本的订单
    - 占位订单不计入 buy_orders/sell_orders（活跃订单）
    - 占位订单只存在于 pause_orders 和 pause_positions 中
    - close_orders_count = len(close_orders) 不包含占位订单
    - 这样确保占位订单不影响网格交易的活跃订单计数
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    CLOSE_SIDE_IS_ASK = grid_state.CLOSE_SIDE_IS_ASK

    # 保存同步前的订单列表，用于检测消失的订单
    previous_buy_orders = trading_state.buy_orders.copy()
    previous_sell_orders = trading_state.sell_orders.copy()

    # 通过 rest api 核对当前订单列表
    orders = await trading_state.grid_trading.get_orders_by_rest()
    if orders is None:
        return

    normalized_orders = (
        [normalize_order_to_ccxt(order) for order in orders]
        if isinstance(orders, list)
        else []
    )

    # buy_orders/sell_orders：活跃的网格交易订单
    # pause_orders：熔断占位订单（不计入活跃订单）
    buy_orders = {}
    sell_orders = {}
    trading_state.pause_orders = {}
    # 保存旧的 pause_positions，用于检测已成交的部分
    old_pause_positions = trading_state.pause_positions.copy()
    trading_state.pause_positions = {}

    # 记录同步时发现的订单ID
    found_order_ids = set()

    for order in normalized_orders:
        order_id = str(order.get("clientOrderId") or order.get("id", ""))
        side = order.get("side", "buy")
        is_ask = side == "sell"
        price = round(float(order.get("price", 0)), 6)
        status = order.get("status")
        initial_base_amount = float(order.get("amount", 0))
        filled_amount = float(order.get("filled", 0))
        remaining_amount = float(order.get("remaining", initial_base_amount - filled_amount))

        found_order_ids.add(order_id)

        # 检查是否是占位订单（通过 order_id 判断）
        is_pause_order = order_id in old_pause_positions or order_id in trading_state.pause_orders

        # 处理非活跃状态的订单（包括部分成交后剩余部分被取消的情况）
        if status != "open":
            # 检查是否是部分成交后剩余部分被取消的订单
            if filled_amount > 0:
                logger.warning(
                    f"检测到订单部分成交或完成: ID={order_id}, "
                    f"已成交={filled_amount}, 剩余={remaining_amount}, 总数量={initial_base_amount}, 状态={status}"
                )
                # 触发订单成交处理逻辑（处理占位订单成交）
                # 注意：这里使用 original_initial_amount 作为原始数量
                # 对于占位订单，需要从 old_pause_positions 中获取原始数量
                original_initial_amount = initial_base_amount
                if is_pause_order and price in old_pause_positions:
                    original_initial_amount = old_pause_positions[price]
                await _handle_disappeared_order_with_fills(
                    order_id, is_ask, price, filled_amount, original_initial_amount
                )
            continue

        # 判断订单是否在平仓侧
        is_close_side_order = is_ask == CLOSE_SIDE_IS_ASK

        if is_close_side_order and initial_base_amount > GRID_CONFIG["GRID_AMOUNT"]:
            # 非网格订单，记录为熔断占位订单 (仅平仓方向且数量大于网格单量)
            # 占位订单用于熔断时的回本卖出，不计入网格交易活跃订单
            # 这确保 close_orders_count 不包含占位订单
            # 如果是部分成交的订单，amount 字段可能表示剩余数量
            # 我们需要检查是否之前存在这个订单，如果存在则使用原始数量
            pause_amount = initial_base_amount
            if price in old_pause_positions:
                # 保留原始数量，不更新（因为剩余量可能已经变化）
                # 如果实际剩余量不同，说明已经部分成交，但订单还在
                if remaining_amount < old_pause_positions[price]:
                    # 部分成交了，但订单仍然存在
                    pause_amount = old_pause_positions[price]
                    logger.info(
                        f"检测到占位订单部分成交: 价格={price}, "
                        f"原始={old_pause_positions[price]}, 剩余={remaining_amount}"
                    )
                    # 未完全成交的平仓侧占位单视为未成交，不因部分成交更新 pause_positions
                    # 保持原始数量冻结，直到完全成交或取消（做多/做空逻辑一致）
                    trading_state.pause_positions[price] = old_pause_positions[price]
                    trading_state.pause_orders[order_id] = {
                        "price": price,
                        "amount": pause_amount,  # 保留原始数量用于后续处理
                    }
                    continue

            trading_state.pause_positions[price] = pause_amount
            trading_state.pause_orders[order_id] = {
                "price": price,
                "amount": pause_amount,
            }
            logger.info(f"同步发现占位订单: ID={order_id}, 价格={price}, 数量={pause_amount}")
            # ← 重要：continue 确保占位订单不计入 buy_orders/sell_orders
            # 占位订单只存在于 pause_orders 中，不影响 close_orders_count 计算
            continue

        if is_ask:
            sell_orders[order_id] = price
        else:
            buy_orders[order_id] = price

    # 检测消失的订单（在本地存在但在交易所不存在）
    disappeared_buy_orders = set(previous_buy_orders.keys()) - found_order_ids
    disappeared_sell_orders = set(previous_sell_orders.keys()) - found_order_ids

    # 本轮 sync 内是否调用过 replenish_grid（会新挂配对单/补开仓单）；若调用过，后续赋值 state 时需合并而非覆盖，避免刚挂的单被 REST 快照覆盖导致下一轮 replenish(False) 误触大间距
    replenish_called_in_sync = False

    # 处理消失的订单
    # 开仓单：从交易所消失视为成交，直接触发配对补卖单，不依赖仓位增量（REST 仓位常有延迟）
    # 平仓单：从交易所消失视为成交，更新 filled_count / 收益 / 可用仓位，保证状态一致
    if disappeared_buy_orders or disappeared_sell_orders:
        logger.warning(
            f"检测到订单消失: 买单={len(disappeared_buy_orders)}, "
            f"卖单={len(disappeared_sell_orders)}"
        )
        grid_amount = float(GRID_CONFIG["GRID_AMOUNT"])

        disappeared_open_orders = (
            [(oid, previous_buy_orders[oid]) for oid in disappeared_buy_orders if oid in previous_buy_orders]
            if not OPEN_SIDE_IS_ASK
            else [(oid, previous_sell_orders[oid]) for oid in disappeared_sell_orders if oid in previous_sell_orders]
        )
        for oid, price in disappeared_open_orders:
            logger.info(
                "消失的开仓单视为成交并触发配对补单: ID=%s, 价格=%s",
                oid,
                price,
            )
            # 从本地开仓订单映射中删除该订单，避免后续合并 state 时把已成交订单重新加入 buy_orders/sell_orders
            if not OPEN_SIDE_IS_ASK:
                if oid in trading_state.buy_orders:
                    del trading_state.buy_orders[oid]
            else:
                if oid in trading_state.sell_orders:
                    del trading_state.sell_orders[oid]

            trading_state.last_filled_order_is_close_side = False
            trading_state.last_trade_price = float(price)
            trading_state.filled_count += 1

            from .grid_replenish import replenish_grid

            if not hasattr(trading_state, "replenished_by_sync_open_order_ids"):
                trading_state.replenished_by_sync_open_order_ids = set()
            trading_state.replenished_by_sync_open_order_ids.add(oid)
            await replenish_grid(True, float(price))
            replenish_called_in_sync = True

        disappeared_close = disappeared_sell_orders if not OPEN_SIDE_IS_ASK else disappeared_buy_orders
        if disappeared_close:
            close_prices = (
                [(oid, previous_sell_orders[oid]) for oid in disappeared_close if oid in previous_sell_orders]
                if not OPEN_SIDE_IS_ASK
                else [(oid, previous_buy_orders[oid]) for oid in disappeared_close if oid in previous_buy_orders]
            )
            if not hasattr(trading_state, "replenished_by_sync_close_order_ids"):
                trading_state.replenished_by_sync_close_order_ids = set()
            for oid, price in close_prices:
                # 从本地平仓订单映射中删除该订单，后续不再将其视为活跃卖单
                if not OPEN_SIDE_IS_ASK:
                    if oid in trading_state.sell_orders:
                        del trading_state.sell_orders[oid]
                else:
                    if oid in trading_state.buy_orders:
                        del trading_state.buy_orders[oid]

                trading_state.replenished_by_sync_close_order_ids.add(oid)
                trading_state.last_filled_order_is_close_side = True
                trading_state.last_trade_price = float(price)
                trading_state.filled_count += 1
                # 可用仓位由主循环用 REST 仓位重算，此处不扣减，避免被 check_current_orders 覆盖
                once_profit = trading_state.base_grid_single_price * grid_amount
                trading_state.active_profit += once_profit
                trading_state.total_profit += once_profit
                trading_state.available_reduce_profit += once_profit
                logger.info(
                    "消失的平仓单视为成交并更新状态: ID=%s, 价格=%s, 收益=%.2f",
                    oid,
                    price,
                    once_profit,
                )
            logger.warning(
                "消失平仓单已记为成交: IDs=%s, 数量=%s",
                sorted(disappeared_close),
                len(close_prices),
            )

    # 检查 pause_position_exist 标志
    if len(trading_state.pause_orders) > 0:
        trading_state.pause_position_exist = True
        logger.info(f"同步后发现 {len(trading_state.pause_orders)} 个占位订单")
    else:
        trading_state.pause_position_exist = False

    # 用 REST 同步结果更新 state；若本轮 sync 内调用过 replenish_grid，需合并保留其新挂的单，避免被覆盖导致下一轮 replenish(False) 误触大间距
    if replenish_called_in_sync:
        for oid, pr in trading_state.buy_orders.items():
            if oid not in buy_orders:
                buy_orders[oid] = pr
        for oid, pr in trading_state.sell_orders.items():
            if oid not in sell_orders:
                sell_orders[oid] = pr
    trading_state.buy_orders = buy_orders
    trading_state.sell_orders = sell_orders

    # 处理待确认成交队列（订单消失但仓位延迟更新）
    if getattr(trading_state, "pending_open_fill_candidates", None):
        remaining_delta = float(position_delta)
        grid_amount = float(GRID_CONFIG["GRID_AMOUNT"])
        # 必须满格才视为成交，避免部分成交被当成满格挂卖单，导致反向仓位（如 0.1 空单）
        threshold = grid_amount
        now = time.time()
        PENDING_TIMEOUT = 60.0

        for candidate in list(trading_state.pending_open_fill_candidates):
            oid, price, ts = candidate
            if now - ts > PENDING_TIMEOUT:
                trading_state.pending_open_fill_candidates.remove(candidate)
                logger.info("待确认成交超时移除: ID=%s, 价格=%s", oid, price)
            elif remaining_delta >= threshold:
                trading_state.pending_open_fill_candidates.remove(candidate)
                logger.info(
                    "待确认成交视为成交(仓位增量=%.2f): ID=%s, 价格=%s, 触发配对补单",
                    remaining_delta,
                    oid,
                    price,
                )
                trading_state.last_filled_order_is_close_side = False
                trading_state.last_trade_price = float(price)
                trading_state.filled_count += 1
                remaining_delta -= grid_amount

                from .grid_replenish import replenish_grid

                await replenish_grid(True, float(price))
                break


async def _handle_disappeared_order_with_fills(
    order_id: str,
    is_ask: bool,
    price: float,
    filled_amount: float,
    initial_amount: float
):
    """
    处理消失的订单（部分成交后剩余部分被取消）

    Args:
        order_id: 订单ID
        is_ask: 是否为卖单
        price: 订单价格
        filled_amount: 已成交数量
        initial_amount: 初始订单数量
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    CLOSE_SIDE_IS_ASK = grid_state.CLOSE_SIDE_IS_ASK

    # 判断是开仓侧还是平仓侧订单
    if OPEN_SIDE_IS_ASK:  # 做空策略
        is_close_side_order = not is_ask
    else:  # 做多策略
        is_close_side_order = is_ask

    # 优先检查是否为占位订单
    is_pause_order = order_id in trading_state.pause_orders

    if is_pause_order:
        # 处理占位订单成交
        order_info = trading_state.pause_orders[order_id]
        pause_price = order_info.get("price", price)

        if filled_amount > 0:
            # 占位订单成交，更新收益
            actual_filled = min(filled_amount, initial_amount)
            once_profit = trading_state.base_grid_single_price * actual_filled
            trading_state.active_profit += once_profit
            trading_state.total_profit += once_profit
            trading_state.available_reduce_profit += once_profit

            logger.info(
                f"占位订单成交: ID={order_id}, 价格={pause_price}, "
                f"已成交={actual_filled}, 收益={once_profit}"
            )

        # 从 pause_orders 中删除
        if order_id in trading_state.pause_orders:
            del trading_state.pause_orders[order_id]

        # 从 pause_positions 中更新或删除
        # 如果完全成交（remaining=0），则删除
        if filled_amount >= initial_amount:
            if pause_price in trading_state.pause_positions:
                del trading_state.pause_positions[pause_price]
            logger.info(f"占位订单完全成交，清理记录: 价格={pause_price}")
        else:
            # 部分成交后订单消失：释放冻结（做多/做空逻辑一致）
            remaining = initial_amount - filled_amount
            if pause_price in trading_state.pause_positions:
                del trading_state.pause_positions[pause_price]
            logger.info(f"占位订单部分成交后消失: 价格={pause_price}, 已成交={filled_amount}, 释放冻结")

        # 如果所有占位订单都已清理，重置标志
        if len(trading_state.pause_orders) == 0:
            trading_state.pause_position_exist = False
            logger.info("所有占位订单已清理，重置 pause_position_exist")

        # 占位订单成交后不触发补单（因为已熔断暂停交易）
        return

    # 从活跃订单列表中删除
    if is_ask:
        if order_id in trading_state.sell_orders:
            del trading_state.sell_orders[order_id]
            logger.info(
                f"处理消失的卖单: ID={order_id}, 已成交={filled_amount}, "
                f"总数量={initial_amount}"
            )
    else:
        if order_id in trading_state.buy_orders:
            del trading_state.buy_orders[order_id]
            logger.info(
                f"处理消失的买单: ID={order_id}, 已成交={filled_amount}, "
                f"总数量={initial_amount}"
            )

    # 如果是平仓单，按实际成交数量更新仓位和收益
    if is_close_side_order and filled_amount > 0:
        actual_filled = min(filled_amount, GRID_CONFIG["GRID_AMOUNT"])
        trading_state.available_position_size = round(
            trading_state.available_position_size - actual_filled,
            2,
        )

        once_profit = (
            trading_state.base_grid_single_price * actual_filled
        )
        trading_state.active_profit += once_profit
        trading_state.total_profit += once_profit
        trading_state.available_reduce_profit += once_profit

        logger.info(
            f"处理消失的平仓单: 订单ID={order_id}, 实际成交={actual_filled}, "
            f"仓位减少={actual_filled}, 收益={once_profit}"
        )

        # 触发补单逻辑
        from .grid_replenish import replenish_grid
        await replenish_grid(True, float(price))
    
    # 如果是开仓侧订单（买单成交），需要挂出对应的卖单
    elif not is_close_side_order and filled_amount > 0:
        actual_filled = min(filled_amount, GRID_CONFIG["GRID_AMOUNT"])
        
        # 更新仓位和状态
        trading_state.available_position_size = round(
            trading_state.available_position_size + actual_filled,
            2,
        )
        trading_state.last_filled_order_is_close_side = False
        trading_state.last_trade_price = float(price)
        trading_state.filled_count += 1
        
        logger.info(
            f"处理消失的开仓单: 订单ID={order_id}, 实际成交={actual_filled}, "
            f"仓位增加={actual_filled}, 价格={price}, 触发补单挂出对应卖单"
        )
        
        # 触发补单逻辑，挂出对应的卖单
        from .grid_replenish import replenish_grid
        await replenish_grid(True, float(price))

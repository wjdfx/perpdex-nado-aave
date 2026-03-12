"""
网格交易仓位管理模块

包含仓位计算、浮亏计算和降仓逻辑。
"""

import logging
import time
from typing import Optional

from . import grid_state
from .grid_state import format_price_for_display

logger = logging.getLogger(__name__)


async def _cal_position_highest_amount_price() -> float:
    """
    计算"最远/亏损最大"的持仓成本价估算。
    
    如果当前存在占位订单，则直接使用占位订单进行计算；
    如果没有占位订单，则按照当前最后的成交价格，加上仓位计算最高距离的订单价格。
    
    Returns:
        估算的最远持仓价格
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    
    # 估算持仓均价/最远价格
    # 做多：价格越低亏损越大，最远价格 = last_trade_price + position_grids * step
    # 做空：价格越高亏损越大，最远价格 = last_trade_price - position_grids * step
    
    direction_multiplier = 1 if not OPEN_SIDE_IS_ASK else -1

    target_price = (
        trading_state.last_trade_price
        + (trading_state.available_position_size / GRID_CONFIG["GRID_AMOUNT"])
        * trading_state.base_grid_single_price
        * direction_multiplier
    )

    if len(trading_state.pause_orders):
        order_id, order_info = max(
            trading_state.pause_orders.items(), key=lambda item: item[1]["amount"]
        )
        target_price = order_info["price"]

    return round(target_price, 6)


async def _highest_order_lost() -> float:
    """
    计算数量最大的仓位浮亏
    
    Returns:
        浮亏金额（正数表示亏损）
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    
    target_price = await _cal_position_highest_amount_price()

    # 做多：(Entry - Current) * Amount. If Entry > Current, diff > 0 (Loss).
    # 做空：(Current - Entry) * Amount. If Current > Entry, diff > 0 (Loss).

    if not OPEN_SIDE_IS_ASK:  # 做多
        diff = target_price - trading_state.current_price
    else:  # 做空
        diff = trading_state.current_price - target_price

    lost = diff * GRID_CONFIG["GRID_AMOUNT"]
    return lost


async def _reduce_position():
    """
    降低仓位的逻辑
    
    使用已实现收益来平掉亏损最大的仓位，以降低整体风险敞口。
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    
    if not trading_state.grid_decrease_status:
        return

    # 只允许此比例的收益用来减仓，以保留收益
    REDUCE_MULTIPLIER = 0.7

    highest_lost = round(await _highest_order_lost(), 6)

    # 如果没有浮亏（盈利状态），不需要用利润填坑
    if highest_lost < 0:
        return

    if trading_state.available_reduce_profit * REDUCE_MULTIPLIER < highest_lost:
        # 当前动态收益不够降仓
        logger.info(
            f"当前可用减仓收益不够降低仓位, 需减仓网格浮亏: {highest_lost}, "
            f"当前可用减仓收益: {round(trading_state.available_reduce_profit, 2)}"
        )
        return

    # 降低占位订单交易数量，对数量最大的那个订单降低，以求平均
    if len(trading_state.pause_orders) > 0:
        logger.info(f"占位订单: {trading_state.pause_orders}")
        order_id, order_info = max(
            trading_state.pause_orders.items(), key=lambda item: item[1]["amount"]
        )
        max_price = order_info["price"]
        success = await trading_state.grid_trading.modify_grid_order(
            order_id=order_id,
            new_price=max_price,
            new_amount=round(
                trading_state.pause_positions[max_price] - GRID_CONFIG["GRID_AMOUNT"], 6
            ),
        )
        if success:
            trading_state.pause_positions[max_price] -= GRID_CONFIG["GRID_AMOUNT"]
            logger.info(
                f"降低占位订单交易数量成功，订单ID: {order_id}, "
                f"新数量: {trading_state.pause_positions[max_price]}"
            )
            del trading_state.pause_orders[order_id]

    import asyncio
    await asyncio.sleep(0.5)
    
    # 降仓 - 下市价单平掉 'GRID_AMOUNT' 大小的仓位
    # 做多（仓位 > 0），需要卖出来减仓。(is_ask=True)
    # 做空（仓位 < 0），需要买入来减仓。(is_ask=False)

    reduce_side_is_ask = True if not OPEN_SIDE_IS_ASK else False

    success, order_id = await trading_state.grid_trading.place_single_market_order(
        is_ask=reduce_side_is_ask,
        price=trading_state.current_price,
        amount=GRID_CONFIG["GRID_AMOUNT"],
    )
    if success:
        trading_state.active_profit = trading_state.active_profit - highest_lost
        # 为避免始终疲于降仓，以致总收益永远上不去，每次用来减仓的利润中，剩余部分不再用于之后的减仓
        trading_state.available_reduce_profit = (
            trading_state.available_reduce_profit
            - round(highest_lost / REDUCE_MULTIPLIER, 2)
        )
        logger.info(
            f"降低仓位成功，当前价格: {trading_state.current_price}, "
            f"已平掉浮亏: {highest_lost}, "
            f"当前剩余动态收益: {round(trading_state.active_profit, 2)}"
        )


async def check_position_limits(position_size: float):
    """
    检查仓位是否超出限制
    
    Args:
        position_size: 当前仓位大小
    """
    from .grid_risk import _get_current_pause_position
    
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    
    trading_state.current_position_size = position_size
    current_pause_position = await _get_current_pause_position()
    trading_state.available_position_size = round(
        trading_state.current_position_size - current_pause_position, 2
    )
    prev_open_spread_alert = trading_state.grid_open_spread_alert
    alert_pos = GRID_CONFIG["ALER_POSITION"]
    alert_log_interval_sec = 60

    if position_size == 0:
        return

    # 当仓位到了警戒线时，触发挂单倾斜，将单边挂单网格距离增大
    if position_size >= alert_pos:
        trading_state.grid_open_spread_alert = True
        trading_state.grid_decrease_status = False
        now = time.time()
        should_log = (
            (not prev_open_spread_alert)
            or now - getattr(trading_state, "last_alert_spread_log_time", 0.0)
            >= alert_log_interval_sec
        )
        if should_log:
            target_step = trading_state.base_grid_single_price * 2
            logger.warning(
                "⚠️ 仓位达到预警阈值，触发开仓侧间距放大: 当前仓位=%s, 预警阈值=%s, 基础间距=%s, 当前动态间距=%s, 目标间距(基础*2)=%s",
                round(position_size, 6),
                round(alert_pos, 6),
                format_price_for_display(trading_state.base_grid_single_price),
                format_price_for_display(trading_state.active_grid_signle_price),
                format_price_for_display(target_step),
            )
            trading_state.last_alert_spread_log_time = now
    else:
        trading_state.grid_open_spread_alert = False
        trading_state.grid_close_spread_alert = False
        if len(trading_state.original_open_prices) > 1:
            # 重置价差为基础价差（需要至少2个价格点来计算价差）
            trading_state.base_grid_single_price = abs(
                trading_state.original_open_prices[1]
                - trading_state.original_open_prices[0]
            )
            trading_state.grid_decrease_status = False
        if prev_open_spread_alert:
            logger.info(
                "✅ 仓位低于预警阈值，恢复基础间距: 当前仓位=%s, 预警阈值=%s, 基础间距=%s",
                round(position_size, 6),
                round(alert_pos, 6),
                format_price_for_display(trading_state.base_grid_single_price),
            )

    max_pos = GRID_CONFIG["MAX_POSITION"]
    if position_size > max_pos:
        logger.warning(f"⚠️ 仓位超出限制: 当前={position_size}, 限制={max_pos}")
        # 网格交易暂停
        trading_state.grid_pause = True

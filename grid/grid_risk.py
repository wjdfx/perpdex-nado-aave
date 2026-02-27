"""
网格交易风控管理模块

包含趋势检测、熔断逻辑和急涨急跌检测。
"""

import logging
from typing import Dict, Tuple

import pandas as pd

from . import grid_state
from . import quota

logger = logging.getLogger(__name__)


async def _risk_check(start: bool = False):
    """
    风控检查主函数
    
    Args:
        start: 是否为启动时的检查
    """
    from .grid_position import _reduce_position
    
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    
    grid_trading = trading_state.grid_trading

    cs_15m = await grid_trading.candle_stick(
        market_id=GRID_CONFIG["MARKET_ID"],
        resolution="15m",
        count_back=int(GRID_CONFIG["RISK_KLINE_COUNT"]),
    )

    # 检测不利趋势 (Adverse Trend)
    # 做多：下跌趋势不利。做空：上涨趋势不利。
    is_adverse, details = await _check_adverse_trend(cs_15m)

    if is_adverse:
        logger.info("⚠️ 警告：当前15分钟线处于不利趋势，暂停交易")

    is_ema_filter, ema_filter_details = await _check_ema_reversion(cs_15m)
    if is_ema_filter:
        logger.info("⚠️ 警告：当前EMA均值回归趋势不利，暂停交易")
    
    logger.info(
        "15分钟线不利趋势检测: %s",
        details | {"result": is_adverse},
    )
    logger.info(
        "EMA均值回归检测: %s",
        ema_filter_details | {"result": is_ema_filter},
    )

    if is_adverse or is_ema_filter:
        trading_state.grid_pause = True
        # if start:
        #     trading_state.pause_position_exist = True
        # else:
        if not trading_state.pause_position_exist:
            await _save_pause_position()
    else:
        if trading_state.current_position_size < GRID_CONFIG["MAX_POSITION"]:
            # 解除熔断
            trading_state.grid_pause = False
            trading_state.pause_position_exist = False

    if (
        trading_state.grid_pause
        and trading_state.available_position_size > GRID_CONFIG["GRID_AMOUNT"]
        and not trading_state.pause_position_exist
        and not trading_state.placing_pause_order
    ):
        # 已经熔断状态下如果还有可用仓位，下占位单
        logger.info(f"开始创建占位订单。。。。。。。。。。。。")
        await _save_pause_position()

    # if trading_state.grid_decrease_status:
    #     logger.info(f"⚠️ 警告：仓位超出降低点，开始降低仓位")
    #     await _reduce_position()


async def _check_adverse_trend(df: pd.DataFrame) -> Tuple[bool, Dict]:
    """
    检测不利趋势
    
    Args:
        df: K线数据
        
    Returns:
        (是否触发, 详情字典)
    """
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    GRID_CONFIG = grid_state.GRID_CONFIG
    ema_period = int(GRID_CONFIG.get("ADVERSE_EMA_PERIOD", 20))
    rsi_period = int(GRID_CONFIG.get("ADVERSE_RSI_PERIOD", 14))
    adx_period = int(GRID_CONFIG.get("ADVERSE_ADX_PERIOD", 14))
    adx_threshold = float(GRID_CONFIG.get("ADVERSE_ADX_THRESHOLD", 25))
    rsi_long_threshold = float(GRID_CONFIG.get("ADVERSE_RSI_LONG_THRESHOLD", 50))
    rsi_short_threshold = float(GRID_CONFIG.get("ADVERSE_RSI_SHORT_THRESHOLD", 50))
    
    if df is None or len(df) < max(ema_period, rsi_period, adx_period):
        return False, {}

    ema_series = quota.compute_ema(df, period=ema_period)
    rsi_series = quota.compute_rsi(df, period=rsi_period)
    adx_series, pdi_series, mdi_series = quota.compute_adx(df, period=adx_period)

    ema_value = float(ema_series.iloc[-1])
    rsi_value = float(rsi_series.iloc[-1])
    adx_value = float(adx_series.iloc[-1])
    pdi_value = float(pdi_series.iloc[-1])
    mdi_value = float(mdi_series.iloc[-1])
    close_value = float(df["close"].iloc[-1])

    # 是否存在明确趋势
    has_trend = adx_value > adx_threshold

    if not OPEN_SIDE_IS_ASK:  # 做多策略：担心下跌趋势
        is_downtrend = close_value < ema_value
        is_bearish_adx = pdi_value < mdi_value
        weak_rsi = rsi_value < rsi_long_threshold
        result = is_downtrend and has_trend and is_bearish_adx and weak_rsi
    else:  # 做空策略：担心上涨趋势
        is_uptrend = close_value > ema_value
        is_bullish_adx = pdi_value > mdi_value
        strong_rsi = rsi_value > rsi_short_threshold
        result = is_uptrend and has_trend and is_bullish_adx and strong_rsi

    details = {
        "close": round(close_value, 4),
        "ema": round(ema_value, 4),
        "adx": round(adx_value, 4),
        "pdi": round(pdi_value, 4),
        "mdi": round(mdi_value, 4),
        "rsi": round(rsi_value, 4),
    }
    return result, details


async def _check_ema_reversion(df: pd.DataFrame) -> Tuple[bool, Dict]:
    """
    EMA 均值回归过滤器
    
    做多：如果价格过高于EMA，有回落风险
    做空：如果价格过低于EMA，有反弹风险
    
    Args:
        df: K线数据
        
    Returns:
        (是否触发, 详情字典)
    """
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    GRID_CONFIG = grid_state.GRID_CONFIG
    ema_period = int(GRID_CONFIG.get("EMA_REVERSION_PERIOD", 60))
    threshold = float(GRID_CONFIG.get("EMA_REVERSION_THRESHOLD", 0.02))
    
    if df is None or len(df) < ema_period:
        return False, {}

    ema_series = quota.compute_ema(df, period=ema_period, column="close")
    ema_value = float(ema_series.iloc[-1])
    current_price = float(df["close"].iloc[-1])

    distance = (current_price - ema_value) / ema_value

    is_triggered = False

    if not OPEN_SIDE_IS_ASK:  # 做多
        # 担心回落。如果价格远高于EMA？
        is_triggered = distance > threshold
    else:  # 做空
        # 担心反弹。如果价格远低于EMA。
        is_triggered = distance < -threshold

    return is_triggered, {"distance": round(distance, 4), "threshold": threshold}


async def is_rapid_market_move(df: pd.DataFrame, close: float) -> Tuple[bool, Dict]:
    """
    急跌/急涨检测
    
    做多：担心急跌（价格大幅下跌）
    做空：担心急涨（价格大幅上涨）
    
    Args:
        df: K线数据
        close: 当前收盘价
        
    Returns:
        (是否触发, 详情字典)
    """
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    GRID_CONFIG = grid_state.GRID_CONFIG
    
    if df is None:
        return False, {}

    atr_period = int(GRID_CONFIG.get("RAPID_MOVE_ATR_PERIOD", 7))
    source_max_diff_pct = float(GRID_CONFIG.get("RAPID_MOVE_SOURCE_MAX_DIFF_PCT", 0.2))
    atr_series = quota.compute_atr(df, period=atr_period)
    atr_value = float(atr_series.iloc[-1])

    open_val = float(df["open"].iloc[-1])
    kline_close = float(df["close"].iloc[-1])
    realtime_close = float(close)

    # 若实时价与K线收盘价偏差过大，认为数据源可能不一致，回退到K线收盘价避免误触发
    source_diff_pct = (
        abs(realtime_close - kline_close) / kline_close if kline_close > 0 else 0.0
    )
    use_realtime = source_diff_pct <= source_max_diff_pct
    effective_close = realtime_close if use_realtime else kline_close

    change = effective_close - open_val
    change_pct = change / open_val if open_val > 0 else 0.0
    threshold_pct = float(GRID_CONFIG.get("RAPID_MOVE_THRESHOLD_PCT", 0.01))

    triggered = False
    if not OPEN_SIDE_IS_ASK:  # 做多
        # 急跌：下跌超过百分比阈值
        if change_pct < -threshold_pct:
            triggered = True
    else:  # 做空
        # 急涨：上涨超过百分比阈值
        if change_pct > threshold_pct:
            triggered = True

    return triggered, {
        "atr": atr_value,
        "change": change,
        "change_pct": change_pct,
        "threshold_pct": threshold_pct,
        "atr_period": atr_period,
        "source_max_diff_pct": source_max_diff_pct,
        "price_source": "realtime" if use_realtime else "kline_fallback",
        "source_diff_pct": source_diff_pct,
    }


async def _save_pause_position():
    """
    熔断时创建占位仓位订单
    
    拆分规则：
    - 如果仓位 > 4倍 GRID_AMOUNT，拆分为多个订单
    - 每个订单数量在 2-3 倍 GRID_AMOUNT 之间
    - 围绕回本价格均匀定价，间距为 base_grid_single_price * 订单数量
    - 回本价格上方的挂单量不少于下方，确保全部卖出后能回本
    """
    trading_state = grid_state.trading_state
    GRID_CONFIG = grid_state.GRID_CONFIG
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    CLOSE_SIDE_IS_ASK = grid_state.CLOSE_SIDE_IS_ASK

    # 防止重入
    if trading_state.placing_pause_order:
        logger.warning("正在创建占位订单中，跳过本次重复调用")
        return
        
    trading_state.placing_pause_order = True
    
    try:
        # 再次检查是否已经存在占位单（防止并发下的竞态条件）
        if trading_state.pause_position_exist:
            logger.debug("占位订单已存在，跳过重复创建")
            return

        if trading_state.available_position_size <= GRID_CONFIG["GRID_AMOUNT"]:
            logger.debug(
                "可用仓位不足以创建占位订单: available=%s, min_required=%s",
                round(trading_state.available_position_size, 6),
                round(GRID_CONFIG["GRID_AMOUNT"], 6),
            )
            return

        orders = []
        total_position = trading_state.available_position_size
        grid_amount = GRID_CONFIG["GRID_AMOUNT"]
        pause_grid_step = (
            trading_state.base_grid_single_price
            if trading_state.base_grid_single_price > 0
            else trading_state.active_grid_signle_price
        )
        
        # 仓位形成距离（回本价格与最后交易价格的差距）
        position_price_range = (
            total_position / grid_amount * pause_grid_step
        )

        # 成本价（回本价格）：基准价格 +/- 距离差价/2
        # multiplier: 做多时为1（卖出价格需要更高），做空时为-1（买入价格需要更低）
        multiplier = 1 if not OPEN_SIDE_IS_ASK else -1

        # 重启后可能没有 last_trade_price，回退使用 current_price 以确保占位单可创建
        ref_price = trading_state.last_trade_price if trading_state.last_trade_price > 0 else trading_state.current_price
        if ref_price is None or ref_price <= 0:
            logger.warning(
                "创建占位订单跳过：缺少有效价格基准（last_trade_price/current_price 均不可用）"
            )
            return

        # 计算回本价格
        breakeven_price = ref_price + (position_price_range / 2 * multiplier)

        # 判断是否需要拆分订单
        if total_position > grid_amount * 4:
            # 需要拆分：每个订单2-3倍 GRID_AMOUNT
            order_amounts = _split_position_into_orders(total_position, grid_amount)
            # 安全检查：确保总量不超过可用仓位 (精度处理)
            actual_total = sum(order_amounts)
            if actual_total > total_position + 1e-6:
                logger.error(f"严重错误: 拆分订单总量 {actual_total} 超过可用仓位 {total_position}，取消拆分")
                order_amounts = [total_position]
                
            order_count = len(order_amounts)
            
            # 计算价格间距：base_grid_single_price * 平均订单大小(倍数)
            # 例如：如果订单主要是2倍 GRID_AMOUNT，间距就应该是 2 * base_grid_single_price
            avg_multiple = (total_position / grid_amount) / order_count
            price_step = pause_grid_step * avg_multiple
            
            logger.info(
                f"占位订单拆分计算: 总仓位={total_position}, 基础量={grid_amount}, "
                f"订单数={order_count}, 平均倍数={avg_multiple:.2f}, "
                f"占位价差={pause_grid_step}, "
                f"订单间距={price_step:.4f}"
            )
            
            # 围绕回本价格均匀分布订单价格
            # 为确保回本价格上方挂单量 >= 下方挂单量，需要计算上下分布
            # 订单按价格从高到低（做多）或从低到高（做空）排列
            order_prices = _calculate_order_prices(
                breakeven_price, price_step, order_count, order_amounts, multiplier
            )

            # -----------------------------------------------------------
            # 价格安全检查：确保所有挂单价格都优于当前价格
            # 做多(卖单): 最低价必须 > 当前价
            # 做空(买单): 最高价必须 < 当前价
            # -----------------------------------------------------------
            current_price = ref_price
            safe_buffer = pause_grid_step * 0.5 # 安全缓冲距离
            
            if not OPEN_SIDE_IS_ASK: # 做多
                min_price = min(order_prices)
                if min_price <= current_price:
                    offset = current_price - min_price + safe_buffer
                    logger.info(f"占位订单价格修正(做多): 最低价{min_price} <= 当前价{current_price}, 整体上移{offset:.4f}")
                    order_prices = [p + offset for p in order_prices]
            else: # 做空
                max_price = max(order_prices)
                if max_price >= current_price:
                    offset = max_price - current_price + safe_buffer # 正数
                    logger.info(f"占位订单价格修正(做空): 最高价{max_price} >= 当前价{current_price}, 整体下移{offset:.4f}")
                    order_prices = [p - offset for p in order_prices]
            
            # 创建订单列表
            for price, amount in zip(order_prices, order_amounts):
                orders.append((CLOSE_SIDE_IS_ASK, round(price, 2), round(amount, 2)))
        else:
            # 不需要拆分，单个订单
            # 同样应用价格检查
            final_price = breakeven_price
            current_price = ref_price
            safe_buffer = pause_grid_step * 0.5

            if not OPEN_SIDE_IS_ASK: # 做多
                if final_price <= current_price:
                    final_price = current_price + safe_buffer
            else: # 做空
                if final_price >= current_price:
                    final_price = current_price - safe_buffer

            orders.append((CLOSE_SIDE_IS_ASK, round(final_price, 2), round(total_position, 2)))

        logger.info(
            "占位订单计划: 可用仓位=%s, 订单数=%s, 回本价=%s, 基础间距=%s, 详情=%s",
            round(total_position, 6),
            len(orders),
            round(breakeven_price, 6),
            round(pause_grid_step, 6),
            orders,
        )

        # 最终安全检查：再次确认可用仓位是否足够（因为是异步，可能中间变了）
        current_available_position = trading_state.available_position_size
        total_order_amount = sum(o[2] for o in orders)
        
        if total_order_amount > current_available_position + 1e-6:
             logger.warning(
                 f"可用仓位变化(计划:{total_order_amount} > 可用:{current_available_position})，执行截断策略"
             )
             # 用户策略：当仓位不足时，直接丢弃多余的订单，保留剩余仓位不使用
             # 例如：计划0.2，可用0.19 -> 变为0.18 (9个订单), 剩余0.01保留
             while total_order_amount > current_available_position + 1e-6 and orders:
                 removed = orders.pop()
                 total_order_amount -= removed[2]
                 logger.info(f"移除末尾订单以适应仓位: {removed}")
            
             if not orders:
                 logger.warning("调整后无有效订单，放弃本次下单")
                 return
                 
             logger.info(f"订单调整完成，最终计划: {round(total_order_amount, 6)}")

        # 占位订单都是平仓单，使用 reduce_only=True 避免部分成交后剩余订单消失
        order_ids = []
        for is_ask, price, amount in orders:
            success, order_id = await trading_state.grid_trading.place_single_order(
                is_ask=is_ask,
                price=price,
                amount=amount,
                reduce_only=True,  # 占位订单是平仓单，使用 Reduce Only
            )
            if success:
                order_ids.append(order_id)
            else:
                logger.error(f"占位订单创建失败: is_ask={is_ask}, price={price}, amount={amount}")
                # 如果失败，取消已创建的订单
                if order_ids:
                    await trading_state.grid_trading.cancel_grid_orders(order_ids)
                return
        
        if order_ids:
            trading_state.pause_position_exist = True
            trading_state.available_position_size = 0.0
            logger.info(f"占位订单创建成功: {order_ids}, 订单详情: {orders}")
        else:
            logger.error(f"占位订单创建失败, {orders}")
    except Exception as e:
        logger.exception(f"创建占位订单失败: {e}")
    finally:
        trading_state.placing_pause_order = False


def _split_position_into_orders(total_position: float, grid_amount: float) -> list:
    """
    将仓位拆分为多个订单
    
    规则：每个订单数量在 2-3 倍 grid_amount 之间
    例如：total_position=0.07, grid_amount=0.01 -> [0.02, 0.02, 0.03]
    
    Args:
        total_position: 总仓位
        grid_amount: 基础挂单数量
        
    Returns:
        订单数量列表
    """
    min_order_size = grid_amount * 2  # 最小订单大小：2倍
    max_order_size = grid_amount * 3  # 最大订单大小：3倍
    
    order_amounts = []
    remaining = total_position
    
    while remaining > 0:
        if remaining <= max_order_size:
            # 剩余仓位可以作为一个订单
            order_amounts.append(remaining)
            remaining = 0
        elif remaining <= max_order_size + min_order_size:
            # 剩余仓位拆分为两个会导致其中一个小于最小值
            # 所以均分
            half = remaining / 2
            order_amounts.append(half)
            order_amounts.append(remaining - half)
            remaining = 0
        else:
            # 取一个2倍的订单，继续处理剩余
            order_amounts.append(min_order_size)
            remaining -= min_order_size
    
    return order_amounts


def _calculate_order_prices(
    breakeven_price: float,
    price_step: float,
    order_count: int,
    order_amounts: list,
    multiplier: int
) -> list:
    """
    计算订单价格，围绕回本价格均匀分布
    
    规则：
    - 回本价格上方（对做多来说是更高的卖出价格）的挂单量 >= 下方
    - 这样确保全部卖出后能回本
    
    Args:
        breakeven_price: 回本价格
        price_step: 价格间距
        order_count: 订单数量
        order_amounts: 各订单的数量列表
        multiplier: 方向乘数（做多=1，做空=-1）
        
    Returns:
        订单价格列表（与order_amounts对应）
    """
    # 计算上方和下方的订单数量分配
    # 为了让上方挂单量 >= 下方，我们把数量较大的订单放在上方
    # 先按数量降序排序，较大的放上方
    indexed_amounts = list(enumerate(order_amounts))
    indexed_amounts.sort(key=lambda x: x[1], reverse=True)
    
    # 计算每个价格位置（相对于回本价格的偏移）
    # 位置0在回本价格，正数在上方，负数在下方
    # 例如：3个订单 -> 位置 [1, 0, -1] 或 [0.5, -0.5] 取决于奇偶
    if order_count == 1:
        positions = [0]
    else:
        # 生成以0为中心的位置序列
        # 例如：3个订单 -> [1, 0, -1]，4个订单 -> [1.5, 0.5, -0.5, -1.5]
        half_count = order_count / 2
        positions = [half_count - 0.5 - i for i in range(order_count)]
    
    # 将较大的订单分配到较高的位置（上方）
    # 对于做多，multiplier=1，上方是更高的价格
    # 对于做空，multiplier=-1，上方是更低的价格
    result_prices = [0.0] * order_count
    
    for i, (original_idx, amount) in enumerate(indexed_amounts):
        position_offset = positions[i]
        # 计算价格：回本价格 + 位置偏移 * 价格步长 * 方向
        price = breakeven_price + position_offset * price_step * multiplier
        result_prices[original_idx] = price
    
    return result_prices


async def _get_current_pause_position() -> float:
    """
    获取当前价格下熔断占位仓位
    
    Returns:
        冻结仓位数量
    """
    trading_state = grid_state.trading_state
    OPEN_SIDE_IS_ASK = grid_state.OPEN_SIDE_IS_ASK
    
    if len(trading_state.pause_positions) == 0:
        return 0
    
    total = 0
    for price, amount in trading_state.pause_positions.items():
        # 只计算未到达的价格的订单
        # 做多（卖单）：Pending if Price > Current
        # 做空（买单）：Pending if Price < Current

        if not OPEN_SIDE_IS_ASK:
            if price > trading_state.current_price:
                total += amount
        else:
            if price < trading_state.current_price:
                total += amount

    return round(total, 6)

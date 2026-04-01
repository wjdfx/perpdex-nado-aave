"""
通用网格交易策略模块

支持做多和做空两种方向的网格交易策略。
"""

from common.config import (
    BASE_URL,
    API_KEY_PRIVATE_KEY,
    ACCOUNT_INDEX,
    API_KEY_INDEX,
    PROXY_URL,
)

import logging
from common.logging_config import setup_logging

# 配置日志
logger = logging.getLogger(__name__)

import asyncio
import os
import time
from typing import Optional

from .grid_trading import GridTrading
from exchanges import create_exchange_adapter
from exchanges.order_converter import normalize_order_to_ccxt

# 导入状态管理模块
from .grid_state import (
    trading_state,
    GRID_CONFIG,
    OPEN_SIDE_IS_ASK,
    CLOSE_SIDE_IS_ASK,
    replenish_grid_lock,
    configure_direction,
    set_grid_config,
    seconds_formatter,
)

# 导入仓位管理模块
from .grid_position import check_position_limits

# 导入订单管理模块


def _get_grid_position(positions, exchange) -> Optional[dict]:
    """
    从全账户仓位中只取当前策略配置币种的仓位，排除其它币种（如 ETH）。
    positions: 来自 get_account_info() 的 positions，可为 dict(symbol -> position) 或 list
    exchange: 当前交易所 adapter，需有 product_id（如 Nado）
    """
    if not positions:
        return None
    target_product_id = getattr(exchange, "product_id", None)
    if target_product_id is None:
        if isinstance(positions, dict):
            return next(iter(positions.values())) if positions else None
        return positions[0] if isinstance(positions, list) and positions else None
    if isinstance(positions, dict):
        for pos in positions.values():
            if pos.get("product_id") == target_product_id:
                return pos
        return None
    if isinstance(positions, list):
        for pos in positions:
            if isinstance(pos, dict) and pos.get("product_id") == target_product_id:
                return pos
        return None
    return None


from .grid_order import (
    check_order_fills,
    check_current_orders,
)

# 导入风控管理模块
from .grid_risk import (
    _risk_check,
    is_rapid_market_move,
)

# 导入网格补单模块
from .grid_replenish import (
    replenish_grid,
    calculate_grid_prices,
    _announce_open_order_price_guard_state,
)


async def on_market_stats_update(market_id: str, market_stats: dict):
    """
    处理市场统计数据更新
    
    Args:
        market_id: 市场ID
        market_stats: 市场统计数据
    """
    from .grid_state import trading_state, GRID_CONFIG

    mark_price = float(market_stats.get("mark_price"))
    if mark_price:
        trading_state.current_price = mark_price

        cs_1m = trading_state.candle_stick_1m
        if trading_state.grid_trading is not None and cs_1m is not None:
            try:
                # 急跌/暴涨检测
                is_rapid_move, details = await is_rapid_market_move(cs_1m, close=mark_price)

                if is_rapid_move:
                    min_step = trading_state.base_grid_single_price
                    max_step = trading_state.base_grid_single_price * 30

                    raw_step = 0.8 * round(details.get("atr"), 2)
                    trading_state.active_grid_signle_price = max(
                        min_step, min(raw_step, max_step)
                    )
            except Exception as e:
                logger.exception(f"市场统计更新中检查急变时发生错误: {e}")


async def on_account_all_orders_update(account_id: str, orders: dict):
    """
    处理账户所有订单更新
    
    Args:
        account_id: 账户ID
        orders: 订单列表
    """
    # 检查是否有订单成交
    await check_order_fills(orders)


async def on_account_all_positions_update(account_id: str, positions: dict):
    """
    处理账户所有仓位更新（仅处理配置币种仓位，其它币种如 ETH 排除在计算外）
    """
    from .grid_state import trading_state, GRID_CONFIG

    if len(trading_state.original_open_prices) == 0:
        logger.info("等待初始化完成...")
        return
    exchange = getattr(trading_state, "grid_trading", None) and trading_state.grid_trading.exchange
    position = _get_grid_position(positions, exchange) if exchange else None
    if position is None:
        return
    position_size = position.get(
        "position", position.get("size", position.get("amount", 0))
    )
    position_size = round(abs(float(position_size)), 2)
    await check_position_limits(position_size)


async def initialize_grid_trading(grid_trading: GridTrading) -> bool:
    """
    初始化网格交易
    
    Args:
        grid_trading: GridTrading 实例
        
    Returns:
        是否初始化成功
    """
    from .grid_state import (
        trading_state,
        GRID_CONFIG,
        OPEN_SIDE_IS_ASK,
    )
    from .grid_order import _sync_current_orders

    try:
        # 记录初始账户情况
        account_info = await grid_trading.exchange.get_account_info()
        if not account_info:
            logger.info("获取账户信息失败")
            return False
        trading_state.start_collateral = float(
            account_info.get("total_equity") or account_info.get("collateral", 0)
        )

        positions = account_info.get("positions", {})
        position = _get_grid_position(positions, grid_trading.exchange)

        # 仓位数据（仅配置币种，其它币种不计入）
        position_size = 0
        position_sign = 0

        if position:
            position_size = position.get(
                "position", position.get("size", position.get("amount", 0))
            )
            sign_raw = position.get("sign", position.get("side", 0))
            if isinstance(sign_raw, str):
                position_sign = (
                    1
                    if sign_raw.lower() == "buy"
                    else -1 if sign_raw.lower() == "sell" else 0
                )
            else:
                position_sign = int(sign_raw)

        trading_state.current_position_size = abs(float(position_size))
        trading_state.current_position_sign = position_sign
        await check_position_limits(trading_state.current_position_size)

        # 记录最后一单成交价格
        trades = await grid_trading.get_trades_by_rest(0, 1)
        if len(trades) > 0:
            last_trade = trades[0]
            trading_state.last_trade_price = float(last_trade.get("price", 0))

        # 等待获取当前价格
        max_wait = 10
        wait_count = 0
        while trading_state.current_price is None and wait_count < max_wait:
            await asyncio.sleep(1)
            wait_count += 1

        if trading_state.current_price is None:
            return False

        # 放置初始网格订单
        base_price = trading_state.current_price
        grid_count = GRID_CONFIG["GRID_COUNT"]
        grid_amount = GRID_CONFIG["GRID_AMOUNT"]
        grid_spread = GRID_CONFIG["GRID_SPREAD"]

        logger.info(f"🚀 初始化网格交易: 基准价格=${base_price}, 网格数量={grid_count}")
        trading_state.open_price = base_price

        # 同步订单状态
        await _sync_current_orders()

        success = True
        if trading_state.open_orders_count > 0 or trading_state.close_orders_count > 0:
            # 已有订单
            logger.info("当前账户已有未结订单，跳过初始化")
        else:
            if not trading_state.grid_pause:
                if _announce_open_order_price_guard_state():
                    logger.info("初始化跳过开仓单：当前价格处于开单保护区间外，等待价格回到允许区间")
                else:
                    place_spread = grid_spread
                    if trading_state.grid_open_spread_alert:
                        place_spread *= 2

                    # 使用 GridTrading.place_grid_orders 辅助函数
                    # side: 1=Long, -1=Short
                    side_param = 1 if not OPEN_SIDE_IS_ASK else -1
                    success = await grid_trading.place_grid_orders(
                        side_param, base_price, grid_count, grid_amount, place_spread
                    )
                    # 下单成功后必须再次同步订单状态，否则 replenish_grid 后续的大间距/配置补单会认为订单数为 0 而重复下单（风控恢复后尤其明显）
                    if success:
                        await _sync_current_orders()

        if success:
            # 初始化价格列表
            trading_state.open_prices = calculate_grid_prices(
                base_price, grid_count, grid_spread
            )

            # 设置基础价差
            if len(trading_state.open_prices) > 1:
                trading_state.base_grid_single_price = abs(
                    trading_state.open_prices[1] - trading_state.open_prices[0]
                )
            else:
                trading_state.base_grid_single_price = base_price * (grid_spread / 100)

            trading_state.active_grid_signle_price = trading_state.base_grid_single_price
            trading_state.original_open_prices = trading_state.open_prices.copy()

            trading_state.is_running = True
            return True
        else:
            return False

    except Exception as e:
        logger.exception(f"初始化网格交易时发生错误: {e}")
        return False


async def run_grid_trading(_exchange_type: str = "nado", grid_config: dict = None):
    """
    运行网格交易系统
    
    Args:
        _exchange_type: 交易所类型
        grid_config: 网格配置参数
    """
    from .grid_state import trading_state, GRID_CONFIG

    setup_logging(_exchange_type)

    if grid_config is None:
        raise ValueError("Grid configuration must be provided")
    
    # 设置全局配置
    set_grid_config(grid_config)
    
    # 配置交易方向
    direction = grid_config.get("DIRECTION", "LONG").upper()
    configure_direction(direction)
    
    if direction == "SHORT":
        logger.info("配置为做空策略")
    else:
        logger.info("配置为做多策略")

    logger.info("🎯 启动通用网格交易系统")
    logger.info(f"配置参数: {grid_config}")
    logger.info(f"交易所类型: {_exchange_type}")

    # 重新导入配置后的变量
    from .grid_state import (
        GRID_CONFIG as CONFIG,
        OPEN_SIDE_IS_ASK as OPEN_ASK,
    )

    subaccount_name = (os.getenv("NADO_SUBACCOUNT_NAME") or "default").strip()
    is_spot = _exchange_type == "nado_spot"
    if is_spot:
        logger.info("使用 Nado 现货模式（EXCHANGE_TYPE=nado_spot）")
    exchange_adapter = create_exchange_adapter(
        exchange_type=_exchange_type,
        market_id=CONFIG["MARKET_ID"],
        subaccount_name=subaccount_name or "default",
    )
    if exchange_adapter is None:
        logger.exception("不支持的交易所类型")
        return
    exchange = exchange_adapter

    await exchange.initialize_client()
    auth, err = await exchange.create_auth_token()
    if err is not None:
        logger.exception(f"创建认证令牌失败: {auth}")
        return

    # Nado 合约/现货: 启动时打印主账号与 linked signer，便于核对配置
    if _exchange_type in ("nado_perp", "nado_spot") and hasattr(exchange, "owner_address"):
        sub_name = getattr(exchange, "subaccount_name", "default")
        logger.info("账户信息: 主账号(子账号归属)=%s, 子账号名=%s", getattr(exchange, "owner_address", "N/A"), sub_name)
        if getattr(exchange, "address", None):
            logger.info("账户信息: 当前签名账户(NADO_PRIVATE_KEY)=%s", exchange.address)
        if hasattr(exchange, "get_linked_signer"):
            try:
                linked = await exchange.get_linked_signer()
                if linked:
                    logger.info("账户信息: 子账号已绑定 linked signer=%s", linked)
                else:
                    logger.info("账户信息: 子账号未绑定 linked signer（或查询失败）")
            except Exception as e:
                logger.warning("查询 linked signer 失败: %s", e)

    risk_enabled = bool(CONFIG.get("RISK_ENABLED", True))

    grid_trading = GridTrading(
        exchange=exchange,
        market_id=CONFIG["MARKET_ID"],
        risk_binance_symbol=CONFIG["RISK_BINANCE_SYMBOL"],
        risk_binance_market=CONFIG.get("RISK_BINANCE_MARKET", "spot"),
    )

    # 明确打印风控数据源与交易标的，便于核对「配置是 ENA 但实际拿到 XLP」等不一致
    binance_symbol = CONFIG.get("RISK_BINANCE_SYMBOL", "")
    binance_market = CONFIG.get("RISK_BINANCE_MARKET", "spot")
    exchange_label = "Nado Spot" if is_spot else "Nado"
    if risk_enabled:
        logger.info(
            "标的核对: 风控/K线数据源=Binance %s (%s) | 交易标的=%s product_id=%s (target_symbol=%s)",
            binance_symbol or "未配置",
            binance_market,
            exchange_label,
            getattr(exchange, "product_id", "?"),
            getattr(exchange, "target_symbol", "?"),
        )
    else:
        logger.info(
            "标的核对: 风控=已关闭 | 交易标的=%s product_id=%s (target_symbol=%s)",
            exchange_label,
            getattr(exchange, "product_id", "?"),
            getattr(exchange, "target_symbol", "?"),
        )
    if _exchange_type in ("nado_perp", "nado_spot") and hasattr(exchange, "PRODUCT_ID_TO_SYMBOL"):
        pid = getattr(exchange, "product_id", None)
        name = exchange.PRODUCT_ID_TO_SYMBOL.get(pid, "未知") if pid is not None else "未知"
        logger.info("标的核对: Nado product_id=%s 当前映射合约名=%s（若与预期不符请检查 NADO_PRODUCT_ID/NADO_SYMBOL）", pid, name)

    proxy_config = PROXY_URL if PROXY_URL else None
    await exchange.subscribe(
        {
            "market_stats": on_market_stats_update,
            "orders": on_account_all_orders_update,
            "positions": on_account_all_positions_update,
        },
        proxy=proxy_config,
    )

    trading_state.grid_trading = grid_trading

    try:
        await asyncio.sleep(2)
        if risk_enabled:
            await _risk_check(start=True)
        else:
            logger.info("风控已关闭：跳过启动时风控检查")
        if not await initialize_grid_trading(grid_trading):
            logger.error("网格交易初始化失败，退出")
            return

        counter = 0
        while trading_state.is_running:
            try:
                # 每5秒打印一次网格状态
                await asyncio.sleep(5)

                # 记录本轮前的仓位，用于判断消失订单是否为成交（仓位增量信号）
                previous_position_size = trading_state.current_position_size

                # 检查仓位状态
                account_info = await asyncio.wait_for(
                    exchange.get_account_info(), timeout=20
                )
                if not account_info:
                    logger.info("获取账户信息失败")
                    continue
                positions = account_info.get("positions", {})
                exchange = trading_state.grid_trading.exchange if trading_state.grid_trading else None
                position = _get_grid_position(positions, exchange) if exchange else None

                # 处理仓位为空的情况（仅配置币种，其它币种不计入）
                if position is None:
                    position_size = 0
                    position_sign = 0
                else:
                    position_size = position.get(
                        "position", position.get("size", position.get("amount", 0))
                    )
                    sign_raw = position.get("sign", position.get("side", 0))
                    if isinstance(sign_raw, str):
                        position_sign = (
                            1
                            if sign_raw.lower() == "buy"
                            else -1 if sign_raw.lower() == "sell" else 0
                        )
                    else:
                        position_sign = int(sign_raw)

                trading_state.current_position_size = round(abs(float(position_size)), 2)
                trading_state.current_position_sign = position_sign
                position_delta = trading_state.current_position_size - previous_position_size
                if position_size is not None:
                    await check_position_limits(trading_state.current_position_size)

                unrealized_pnl = (
                    float(position.get("unrealized_pnl", position.get("pnl", 0)))
                    if position
                    else 0.0
                )

                # 检查当前账户保证金
                trading_state.current_collateral = float(
                    account_info.get("total_equity") or account_info.get("collateral", 0)
                )

                unrealized_collateral = trading_state.current_collateral + unrealized_pnl
                pnl = unrealized_collateral - trading_state.start_collateral

                from .grid_risk import _get_current_pause_position
                current_pause_position = await _get_current_pause_position()
                time_formatted = await seconds_formatter(
                    time.time() - trading_state.start_time
                )
                # 美化日志输出
                from .grid_state import format_price_for_display
                log_pnl = round(pnl, 6)
                log_total_profit = round(trading_state.total_profit, 2)
                log_active_profit = round(trading_state.active_profit, 2)
                log_reduce_profit = round(trading_state.available_reduce_profit, 2)
                log_grid_step = format_price_for_display(trading_state.active_grid_signle_price or 0)
                log_open_price = format_price_for_display(trading_state.open_price or 0)
                log_current_price = format_price_for_display(trading_state.current_price or 0)
                _announce_open_order_price_guard_state()
                logger.info(
                    f"\n"
                    f"════════════════════ 策略运行报告 ════════════════════\n"
                    f"[资产情况] 初始: {round(trading_state.start_collateral, 6)} | 当前: {round(unrealized_collateral, 6)} | 盈亏: {log_pnl}\n"
                    f"[收益统计] 套利: {log_total_profit:<8} | 动态: {log_active_profit:<8} | 减仓: {log_reduce_profit:<8}\n"
                    f"[仓位管理] 当前: {position_size:<8} | 冻结: {current_pause_position:<8} | 可用: {trading_state.available_position_size:<8}\n"
                    f"[运行状态] 耗时: {time_formatted:<8} | 成交: {trading_state.filled_count:<8} | 间距: {log_grid_step:<8}\n"
                    f"[市场行情] 开仓: {log_open_price:<8} | 当前: {log_current_price:<8}\n"
                    f"[活跃订单] 买单: {trading_state.buy_orders} | 卖单: {trading_state.sell_orders}\n"
                    f"════════════════════════════════════════════════════"
                )

                if risk_enabled:
                    # 获取K线数据
                    cs_1m = await asyncio.wait_for(
                        grid_trading.candle_stick(
                            market_id=CONFIG["MARKET_ID"],
                            resolution="1m",
                            count_back=int(CONFIG["RISK_KLINE_COUNT"]),
                        ),
                        timeout=20,
                    )
                    trading_state.candle_stick_1m = cs_1m

                    # 急跌/急涨 判断 (Rapid Market Move)
                    if trading_state.current_price:
                        is_rapid, details = await is_rapid_market_move(
                            cs_1m, trading_state.current_price
                        )
                        if is_rapid:
                            logger.info(f"⚠️ 警告：当前市场剧烈波动中, {details}")

                        # 波动检测 (Dynamic Step Adjustment)
                        atr_value = details.get("atr", 0)
                        trading_state.current_atr = atr_value

                        if atr_value > CONFIG["ATR_THRESHOLD"]:
                            min_step = trading_state.base_grid_single_price
                            max_step = trading_state.base_grid_single_price * 30

                            raw_step = 0.7 * round(atr_value, 2)
                            trading_state.active_grid_signle_price = max(
                                min_step, min(raw_step, max_step)
                            )
                        else:
                            trading_state.active_grid_signle_price = (
                                trading_state.base_grid_single_price
                            )

                            if trading_state.grid_open_spread_alert:
                                # 开仓侧警告时增加价差
                                trading_state.active_grid_signle_price = (
                                    trading_state.base_grid_single_price * 2
                                )

                    # 定期风控检查 (每60秒)
                    if counter % 6 == 0:
                        if trading_state.current_price and "details" in locals():
                            logger.info("波动检测: %s", details | {"result": is_rapid})
                        await asyncio.wait_for(_risk_check(), timeout=30)
                else:
                    trading_state.candle_stick_1m = None
                    trading_state.current_atr = 0.0
                    trading_state.active_grid_signle_price = trading_state.base_grid_single_price

                # 补单
                async with replenish_grid_lock:
                    if time.time() - trading_state.last_replenish_time > 5:
                        await asyncio.wait_for(
                            check_current_orders(position_delta=position_delta),
                            timeout=20,
                        )
                        await asyncio.wait_for(replenish_grid(False), timeout=20)

                counter += 1
            except asyncio.TimeoutError:
                logger.exception("执行循环检查超时，已跳过本轮")
            except Exception:
                logger.exception("执行循环检查时出现异常")

    except KeyboardInterrupt:
        logger.info("👋 收到停止信号")
    except Exception:
        logger.exception(f"网格交易运行时发生错误")
    finally:
        trading_state.is_running = False
        await exchange.close()
        logger.info("🔚 网格交易系统已停止")


if __name__ == "__main__":
    # 示例配置
    TEST_CONFIG = {
        "MARKET_ID": 1,
        "GRID_COUNT": 10,
        "GRID_AMOUNT": 0.01,
        "GRID_SPREAD": 0.1,
        "MAX_TOTAL_ORDERS": 20,
        "ALER_POSITION": 1.0,
        "MAX_POSITION": 5.0,
        "ATR_THRESHOLD": 5.0,
        "DIRECTION": "LONG",  # 或 "SHORT"
    }
    # 运行时导入此函数并传入配置
    pass

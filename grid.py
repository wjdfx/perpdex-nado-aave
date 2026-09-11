import grid.quant_grid_universal as quant_grid_universal
import asyncio
import sys
import os
from enum import Enum
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class ExchangeType(Enum):
    """Nado 交易所类型：合约与现货二选一"""
    NADO_PERP = "nado_perp"   # 永续合约
    NADO_SPOT = "nado_spot"   # 现货


def _get_required_env(key: str) -> str:
    value = os.getenv(key)
    if value is None or str(value).strip() == "":
        raise ValueError(f"Missing required environment variable: {key}")
    return str(value).strip()


def _get_bool_env(key: str, default: bool = False) -> bool:
    value = os.getenv(key)
    if value is None or str(value).strip() == "":
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _get_optional_float_env(key: str) -> float | None:
    value = os.getenv(key)
    if value is None or str(value).strip() == "":
        return None
    return float(str(value).strip())


def load_grid_configs() -> Dict[str, Dict[str, Any]]:
    """
    Load grid configurations from environment variables
    """
    risk_enabled = _get_bool_env("RISK_ENABLED", True)
    open_order_price_guard_enabled = _get_bool_env(
        "OPEN_ORDER_PRICE_GUARD_ENABLED", False
    )
    open_order_price_guard_action = (
        os.getenv("OPEN_ORDER_PRICE_GUARD_ACTION", "pause_open_orders").strip().lower()
    )
    open_order_price_guard_min = _get_optional_float_env(
        "OPEN_ORDER_PRICE_GUARD_MIN_PRICE"
    )
    open_order_price_guard_max = _get_optional_float_env(
        "OPEN_ORDER_PRICE_GUARD_MAX_PRICE"
    )

    if open_order_price_guard_enabled:
        if open_order_price_guard_action not in {
            "pause_open_orders",
            "cancel_orders_and_stop",
        }:
            raise ValueError(
                "OPEN_ORDER_PRICE_GUARD_ACTION 仅支持 pause_open_orders 或 cancel_orders_and_stop"
            )
        if (
            open_order_price_guard_min is None
            and open_order_price_guard_max is None
        ):
            raise ValueError(
                "OPEN_ORDER_PRICE_GUARD_ENABLED=true 时，至少需要设置 OPEN_ORDER_PRICE_GUARD_MIN_PRICE 或 OPEN_ORDER_PRICE_GUARD_MAX_PRICE"
            )
        if (
            open_order_price_guard_min is not None
            and open_order_price_guard_max is not None
            and open_order_price_guard_min >= open_order_price_guard_max
        ):
            raise ValueError(
                "OPEN_ORDER_PRICE_GUARD_MIN_PRICE 必须小于 OPEN_ORDER_PRICE_GUARD_MAX_PRICE"
            )

    # Load common grid configuration
    common_config = {
        "DIRECTION": _get_required_env("DIRECTION"),  # 交易方向
        "GRID_COUNT": int(_get_required_env("GRID_COUNT")),  # 每侧网格数量
        "GRID_AMOUNT": float(_get_required_env("GRID_AMOUNT")),  # 单网格挂单量
        "GRID_SPREAD": float(_get_required_env("GRID_SPREAD")),  # 单网格价差（百分比）
        "MAX_TOTAL_ORDERS": int(_get_required_env("MAX_TOTAL_ORDERS")),  # 最大活跃订单数量
        "MAX_POSITION": float(_get_required_env("MAX_POSITION")),  # 最大仓位限制
        "ALER_POSITION": float(_get_required_env("ALER_POSITION")),  # 警告仓位限制
        "MARKET_ID": int(_get_required_env("MARKET_ID")),  # 市场ID
        "RISK_ENABLED": risk_enabled,  # 是否启用K线风控
        "OPEN_ORDER_PRICE_GUARD_ENABLED": open_order_price_guard_enabled,  # 是否启用价格区间外暂停开单
        "OPEN_ORDER_PRICE_GUARD_ACTION": open_order_price_guard_action,  # 价格保护触发后动作：暂停开仓或撤单停机
        "OPEN_ORDER_PRICE_GUARD_MIN_PRICE": open_order_price_guard_min,  # 当前价<=该值时暂停开单
        "OPEN_ORDER_PRICE_GUARD_MAX_PRICE": open_order_price_guard_max,  # 当前价>=该值时暂停开单
        "RISK_BINANCE_SYMBOL": (_get_required_env("RISK_BINANCE_SYMBOL") if risk_enabled else os.getenv("RISK_BINANCE_SYMBOL", "")).strip(),  # 风控K线Binance交易对（如 AAVEUSDT）
        "RISK_BINANCE_MARKET": os.getenv("RISK_BINANCE_MARKET", "spot").strip().lower(),  # 风控K线Binance市场：spot|futures
        "RISK_KLINE_COUNT": int(_get_required_env("RISK_KLINE_COUNT") if risk_enabled else os.getenv("RISK_KLINE_COUNT", "100")),  # 风控K线每次拉取根数（1m/15m共用）
        "ATR_THRESHOLD": float(_get_required_env("ATR_THRESHOLD") if risk_enabled else os.getenv("ATR_THRESHOLD", "0")),  # ATR波动阈值
        "RAPID_MOVE_THRESHOLD_PCT": float(_get_required_env("RAPID_MOVE_THRESHOLD_PCT") if risk_enabled else os.getenv("RAPID_MOVE_THRESHOLD_PCT", "0")),  # 急跌/急涨阈值（百分比）
        "RAPID_MOVE_ATR_PERIOD": int(_get_required_env("RAPID_MOVE_ATR_PERIOD") if risk_enabled else os.getenv("RAPID_MOVE_ATR_PERIOD", "7")),  # 急跌/急涨ATR周期
        "RAPID_MOVE_SOURCE_MAX_DIFF_PCT": float(_get_required_env("RAPID_MOVE_SOURCE_MAX_DIFF_PCT") if risk_enabled else os.getenv("RAPID_MOVE_SOURCE_MAX_DIFF_PCT", "0.2")),  # 实时价与K线收盘价最大允许偏差
        "ADVERSE_EMA_PERIOD": int(_get_required_env("ADVERSE_EMA_PERIOD") if risk_enabled else os.getenv("ADVERSE_EMA_PERIOD", "20")),  # 不利趋势EMA周期
        "ADVERSE_RSI_PERIOD": int(_get_required_env("ADVERSE_RSI_PERIOD") if risk_enabled else os.getenv("ADVERSE_RSI_PERIOD", "14")),  # 不利趋势RSI周期
        "ADVERSE_ADX_PERIOD": int(_get_required_env("ADVERSE_ADX_PERIOD") if risk_enabled else os.getenv("ADVERSE_ADX_PERIOD", "14")),  # 不利趋势ADX周期
        "ADVERSE_ADX_THRESHOLD": float(_get_required_env("ADVERSE_ADX_THRESHOLD") if risk_enabled else os.getenv("ADVERSE_ADX_THRESHOLD", "25")),  # 不利趋势ADX阈值
        "ADVERSE_RSI_LONG_THRESHOLD": float(_get_required_env("ADVERSE_RSI_LONG_THRESHOLD") if risk_enabled else os.getenv("ADVERSE_RSI_LONG_THRESHOLD", "50")),  # LONG不利趋势RSI阈值
        "ADVERSE_RSI_SHORT_THRESHOLD": float(_get_required_env("ADVERSE_RSI_SHORT_THRESHOLD") if risk_enabled else os.getenv("ADVERSE_RSI_SHORT_THRESHOLD", "50")),  # SHORT不利趋势RSI阈值
        "EMA_REVERSION_PERIOD": int(_get_required_env("EMA_REVERSION_PERIOD") if risk_enabled else os.getenv("EMA_REVERSION_PERIOD", "60")),  # EMA均值回归周期
        "EMA_REVERSION_THRESHOLD": float(_get_required_env("EMA_REVERSION_THRESHOLD") if risk_enabled else os.getenv("EMA_REVERSION_THRESHOLD", "0.04")),  # EMA均值回归偏离阈值
        "OVER_RANGE_GAP_MULTIPLIER": float(os.getenv("OVER_RANGE_GAP_MULTIPLIER", "2.5")),  # 大间距补单：开平仓间距超过该倍数步长时触发；无卖单时用于开仓价与当前价间距
        "PRICE_PRECISION": float(os.getenv("PRICE_PRECISION", "0.1")),  # 价格精度（最小变动单位），如 0.1=ETH、0.01=部分币种，用于下单前舍入以适配不同交易所/币种
        # REST 对账（兜底）：WS 主驱动，REST 仅低频校准，避免 WS 漏消息导致本地状态漂移
        "REST_SYNC_INTERVAL_SEC": float(os.getenv("REST_SYNC_INTERVAL_SEC", "60")),  # REST 同步间隔秒（默认60）。过低会增加“瞬时漏单”误判风险
        "DISAPPEARED_ORDER_CONFIRM_SEC": float(os.getenv("DISAPPEARED_ORDER_CONFIRM_SEC", "3")),  # REST 检测到订单消失后延迟确认秒（默认3），避免短暂漏单被当成交
        # 补单保护：WS 成交回调内的补单超时，防止配对平仓无限重试时持锁卡死整个补单系统
        "WS_REPLENISH_TIMEOUT_SEC": float(os.getenv("WS_REPLENISH_TIMEOUT_SEC", "30")),  # WS 成交补单单次超时秒（默认30）
        "PAIRED_CLOSE_STAGE3_MAX_RETRY": int(os.getenv("PAIRED_CLOSE_STAGE3_MAX_RETRY", "30")),  # 配对平仓 3x 阶段最大重试次数（默认30，约60秒），耗尽后交给仓位对账兜底
        # 仓位对账：与成交事件无关的幂等兜底，周期性比对「可用仓位」与「平仓单总量」，补齐缺口
        "CLOSE_ORDER_RECONCILE_ENABLED": _get_bool_env("CLOSE_ORDER_RECONCILE_ENABLED", True),  # 是否启用平仓单对账
        "CLOSE_ORDER_RECONCILE_MAX_PER_ROUND": int(os.getenv("CLOSE_ORDER_RECONCILE_MAX_PER_ROUND", "3")),  # 每轮最多补几张，避免突发补单
        # 追价：原逻辑仅在「一张平仓单都没有」时才追价，持仓后网格永远不跟随价格移动
        "TRAILING_MAX_POSITION_RATIO": float(os.getenv("TRAILING_MAX_POSITION_RATIO", "0.3")),  # 仓位 <= ALER_POSITION*该比例时允许追价；0 表示恢复旧行为（仅空仓追价）
        "GRID_TRANSLATE_ENABLED": _get_bool_env("GRID_TRANSLATE_ENABLED", True),  # 向上补开仓单后撤掉最远一张，使网格整体平移而非单向堆积
        "OPEN_ORDER_FOLLOW_MARKET_ENABLED": _get_bool_env("OPEN_ORDER_FOLLOW_MARKET_ENABLED", False),  # 开仓单距市价过远时直接贴近市价挂单（激进，默认关闭）
        "OPEN_ORDER_FOLLOW_MARKET_GAP_MULT": float(os.getenv("OPEN_ORDER_FOLLOW_MARKET_GAP_MULT", "3.0")),  # 距离超过该倍数步长才触发贴近市价
    }
    
    common = common_config.copy()
    return {
        "nado_perp": common.copy(),
        "nado_spot": common.copy(),
    }


def validate_exchange_type(exchange_type: str) -> str:
    """
    校验交易所类型。合约与现货二选一：nado_perp | nado_spot
    """
    t = (exchange_type or "").strip().lower()
    if t == "nado":
        print("Error: EXCHANGE_TYPE 必须明确指定 nado_perp（合约）或 nado_spot（现货），不能用 nado")
        print("  合约: EXCHANGE_TYPE=nado_perp")
        print("  现货: EXCHANGE_TYPE=nado_spot")
        sys.exit(1)
    try:
        return ExchangeType(t).value
    except ValueError:
        allowed = [e.value for e in ExchangeType]
        print(f"Error: EXCHANGE_TYPE='{exchange_type}' 无效。允许: {allowed}")
        sys.exit(1)


if __name__ == "__main__":
    # Access arguments via sys.argv
    # sys.argv[0] is the script name, sys.argv[1:] are the actual arguments
    exchange_type = os.getenv("EXCHANGE_TYPE", "")
    if not exchange_type:
        print("Error: Missing required environment variable: EXCHANGE_TYPE")
        sys.exit(1)
    # Validate the provided exchange type
    exchange_type = validate_exchange_type(exchange_type)
    
    try:
        # Load grid configurations from environment variables (strict mode)
        grid_configs = load_grid_configs()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

    # Get the appropriate grid configuration for the exchange type
    grid_config = grid_configs.get(exchange_type)
    if grid_config is None:
        print(f"Error: No grid configuration found for exchange type '{exchange_type}'")
        sys.exit(1)

    asyncio.run(quant_grid_universal.run_grid_trading(exchange_type, grid_config))

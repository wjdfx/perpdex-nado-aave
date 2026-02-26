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
    NADO = "nado"


def _get_required_env(key: str) -> str:
    value = os.getenv(key)
    if value is None or str(value).strip() == "":
        raise ValueError(f"Missing required environment variable: {key}")
    return str(value).strip()


def load_grid_configs() -> Dict[str, Dict[str, Any]]:
    """
    Load grid configurations from environment variables
    """
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
        "ATR_THRESHOLD": float(_get_required_env("ATR_THRESHOLD")),  # ATR波动阈值
        "RAPID_MOVE_THRESHOLD_PCT": float(_get_required_env("RAPID_MOVE_THRESHOLD_PCT")),  # 急跌/急涨阈值（百分比）
        "RAPID_MOVE_ATR_PERIOD": int(_get_required_env("RAPID_MOVE_ATR_PERIOD")),  # 急跌/急涨ATR周期
        "RAPID_MOVE_SOURCE_MAX_DIFF_PCT": float(_get_required_env("RAPID_MOVE_SOURCE_MAX_DIFF_PCT")),  # 实时价与K线收盘价最大允许偏差
        "ADVERSE_EMA_PERIOD": int(_get_required_env("ADVERSE_EMA_PERIOD")),  # 不利趋势EMA周期
        "ADVERSE_RSI_PERIOD": int(_get_required_env("ADVERSE_RSI_PERIOD")),  # 不利趋势RSI周期
        "ADVERSE_ADX_PERIOD": int(_get_required_env("ADVERSE_ADX_PERIOD")),  # 不利趋势ADX周期
        "ADVERSE_ADX_THRESHOLD": float(_get_required_env("ADVERSE_ADX_THRESHOLD")),  # 不利趋势ADX阈值
        "ADVERSE_RSI_LONG_THRESHOLD": float(_get_required_env("ADVERSE_RSI_LONG_THRESHOLD")),  # LONG不利趋势RSI阈值
        "ADVERSE_RSI_SHORT_THRESHOLD": float(_get_required_env("ADVERSE_RSI_SHORT_THRESHOLD")),  # SHORT不利趋势RSI阈值
        "EMA_REVERSION_PERIOD": int(_get_required_env("EMA_REVERSION_PERIOD")),  # EMA均值回归周期
        "EMA_REVERSION_THRESHOLD": float(_get_required_env("EMA_REVERSION_THRESHOLD")),  # EMA均值回归偏离阈值
    }
    
    return {"nado": common_config.copy()}


def validate_exchange_type(exchange_type: str) -> str:
    """
    Validate that the exchange type is one of the allowed values
    """
    try:
        return ExchangeType(exchange_type).value
    except ValueError:
        allowed_values = [e.value for e in ExchangeType]
        print(f"Error: Invalid exchange type '{exchange_type}'. Allowed values are: {allowed_values}")
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

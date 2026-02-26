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


def load_grid_configs() -> Dict[str, Dict[str, Any]]:
    """
    Load grid configurations from environment variables
    """
    # Load common grid configuration
    common_config = {
        "DIRECTION": os.getenv('DIRECTION', 'LONG'),  # 交易方向
        "GRID_COUNT": int(os.getenv('GRID_COUNT', 3)),  # 每侧网格数量
        "GRID_AMOUNT": float(os.getenv('GRID_AMOUNT', 0.01)),  # 单网格挂单量
        "GRID_SPREAD": float(os.getenv('GRID_SPREAD', 0.05)),  # 单网格价差（百分比）
        "MAX_TOTAL_ORDERS": int(os.getenv('MAX_TOTAL_ORDERS', 10)),  # 最大活跃订单数量
        "MAX_POSITION": float(os.getenv('MAX_POSITION', 1.0)),  # 最大仓位限制
        "ALER_POSITION": float(os.getenv('ALER_POSITION', 0.3)),  # 警告仓位限制
        "MARKET_ID": int(os.getenv('MARKET_ID', 0)),  # 市场ID
        "ATR_THRESHOLD": float(os.getenv('ATR_THRESHOLD', 0.8)),  # ATR波动阈值
        "RAPID_MOVE_THRESHOLD_PCT": float(os.getenv('RAPID_MOVE_THRESHOLD_PCT', 0.01)),  # 急跌/急涨阈值（百分比）
    }
    
    return {"nado": common_config.copy()}


# Load grid configurations from environment variables
GRID_CONFIGS = load_grid_configs()


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
    exchange_type = os.getenv("EXCHANGE_TYPE", ExchangeType.NADO.value)
    # Validate the provided exchange type
    exchange_type = validate_exchange_type(exchange_type)

    # Get the appropriate grid configuration for the exchange type
    grid_config = GRID_CONFIGS.get(exchange_type)
    if grid_config is None:
        print(f"Error: No grid configuration found for exchange type '{exchange_type}'")
        sys.exit(1)

    asyncio.run(quant_grid_universal.run_grid_trading(exchange_type, grid_config))

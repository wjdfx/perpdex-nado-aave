"""
网格交易状态管理模块

包含全局状态类、配置变量和辅助函数。
"""

import asyncio
import time
from typing import Dict, List, Optional
import pandas as pd

from .grid_trading import GridTrading


# 网格交易参数配置（将在 run_grid_trading 函数中传入）
GRID_CONFIG: Optional[dict] = None

# 方向常数
DIRECTION = "LONG"  # Default
OPEN_SIDE_IS_ASK = False
CLOSE_SIDE_IS_ASK = True


class GridTradingState:
    """网格交易全局状态管理类"""

    def __init__(self):
        self.current_price: Optional[float] = None
        self.is_running: bool = False
        self.grid_trading: Optional[GridTrading] = None  # 网格交易实例
        
        # "open_prices" 存储用于开仓的价格（做多时为买单，做空时为卖单）
        self.open_prices: List[float] = []  # 开仓价格列表（有序）
        
        # 买卖订单映射
        self.buy_orders: dict[str, float] = {}  # 买单订单ID到价格映射
        self.sell_orders: dict[str, float] = {}  # 卖单订单ID到价格映射

        # 原始价格序列（用于参考）
        self.original_open_prices: List[float] = []  # 原始开仓价格序列

        self.base_grid_single_price: float = 0  # 单网格价差值
        self.active_grid_signle_price: float = 0  # 动态单网格价差值
        self.start_collateral: float = 0  # 初始保证金
        self.current_collateral: float = 0  # 当前保证金
        self.start_time: float = time.time()  # 启动时间
        self.open_price: Optional[float] = None  # 启动时基准价格
        
        # 记录上一次成交订单是否在平仓侧（止盈）
        self.last_filled_order_is_close_side: bool = True

        self.last_replenish_time: float = 0  # 上次补单时间
        self.last_trade_price: float = 0  # 上次成交价格
        self.grid_pause: bool = False  # 网格交易暂停标志

        self.grid_close_spread_alert: bool = False  # 平仓侧警告
        self.grid_open_spread_alert: bool = False  # 开仓侧警告

        self.grid_decrease_status: bool = False  # 降低仓位状态
        self.current_position_size: float = 0  # 当前仓位大小
        self.current_position_sign: int = 0  # 当前仓位方向
        self.filled_count: int = 0  # 成交订单计数
        self.candle_stick_1m: pd.DataFrame = None  # 1分钟K线数据
        self.current_atr: float = 0.0  # 当前ATR值
        
        self.pause_positions: dict[float, float] = {}  # 熔断时的仓位映射, 价格->仓位
        self.pause_orders: dict[str, dict[str, float]] = {}  # 占位订单ID到价格与数量的映射
        self.pause_position_exist: bool = False  # 记录本次是否已经进行了熔断占位仓位下单
        
        self.available_position_size: float = 0.0  # 可用仓位
        self.active_profit: float = 0.0  # 动态网格收益
        self.total_profit: float = 0.0  # 本次运行总收益
        self.available_reduce_profit: float = 0.0  # 可用来减仓的收益
        
        self.placing_pause_order: bool = False  # 是否正在进行熔断占位下单 (防止重入)
        # 开仓成交后配对平仓单失败时，短时禁止大间距平仓补单，避免错误价格覆盖配对单
        self.paired_close_retry_block_until: float = 0.0
        self.paired_close_target_price: float = 0.0

        # 上一轮仓位，用于判断成交（仓位增量信号）
        self.previous_position_size: float = 0.0

        # 待确认成交队列: [(order_id, price, timestamp), ...]
        # 用于处理「订单消失但仓位未及时更新」（如 Nado 返回延迟）
        self.pending_open_fill_candidates: list = []

        # 同步路径已按「消失开仓单」补过配对卖单的订单 ID，避免 fill 事件再次补单导致同价双卖单
        self.replenished_by_sync_open_order_ids: set = set()
        self.replenished_by_sync_close_order_ids: set = set()  # 同步路径已记为成交的平仓单，避免 fill 事件重复处理

    @property
    def open_orders(self) -> dict[str, float]:
        """返回开仓侧的订单字典"""
        return self.sell_orders if OPEN_SIDE_IS_ASK else self.buy_orders

    @property
    def close_orders(self) -> dict[str, float]:
        """返回平仓侧的订单字典"""
        return self.buy_orders if OPEN_SIDE_IS_ASK else self.sell_orders

    @property
    def open_orders_count(self) -> int:
        """返回开仓侧订单数量"""
        return len(self.open_orders)

    @property
    def close_orders_count(self) -> int:
        """返回平仓侧订单数量"""
        return len(self.close_orders)


# 全局状态实例
trading_state = GridTradingState()

# 全局异步锁，用于保护 replenish_grid() 方法
replenish_grid_lock = asyncio.Lock()


def configure_direction(direction: str) -> None:
    """
    配置交易方向
    
    Args:
        direction: "LONG" 或 "SHORT"
    """
    global DIRECTION, OPEN_SIDE_IS_ASK, CLOSE_SIDE_IS_ASK
    
    DIRECTION = direction.upper()
    if DIRECTION == "SHORT":
        OPEN_SIDE_IS_ASK = True
        CLOSE_SIDE_IS_ASK = False
    else:
        # 默认 Long
        OPEN_SIDE_IS_ASK = False
        CLOSE_SIDE_IS_ASK = True


def set_grid_config(config: dict) -> None:
    """设置网格配置"""
    global GRID_CONFIG
    GRID_CONFIG = config


async def seconds_formatter(seconds: int) -> str:
    """
    将秒数格式化为可读的时间字符串
    
    Args:
        seconds: 秒数
        
    Returns:
        格式化的时间字符串，如 "1天 2小时 30分钟 45秒"
    """
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    return f"{round(days)}天 {round(hours)}小时 {round(minutes)}分钟 {round(seconds)}秒"

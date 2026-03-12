import json
import logging
import time
import asyncio
import pandas as pd
import aiohttp
from typing import Any, Dict, List, Tuple, Optional
from . import quota
from common.config import BASE_URL
from exchanges.interfaces import ExchangeInterface


logger = logging.getLogger(__name__)


class GridTrading:
    """
    网格交易类，用于在基准价格周围创建网格订单
    """

    def __init__(
        self,
        exchange: ExchangeInterface,
        market_id: int = 0,
        risk_binance_symbol: str = "AAVEUSDT",
    ):
        """
        初始化网格交易类

        Args:
            exchange: 交易所适配器实例
            market_id: 市场ID，默认为0
        """
        self.exchange = exchange
        self.market_id = market_id
        self.risk_binance_symbol = risk_binance_symbol.strip().upper()

        # 价格和数量乘数（与quant.py保持一致）
        self.base_amount_multiplier = pow(10, 4)
        self.price_multiplier = pow(10, 2)

    def check_current_orders(self) -> Optional[List[dict]]:
        """
        检查当前账户的所有订单

        Returns:
            订单列表或None（如果获取失败）
        """
        try:
            return self.exchange.get_orders()
        except Exception as e:
            logger.error(f"检查当前订单时发生错误: {e}")
            return None

    def generate_grid_orders(self, side: int, base_price: float, grid_count: int,
                            grid_amount: float, grid_spread: float) -> List[Tuple[bool, float, float]]:
        """
        生成双向网格订单列表（中性策略）

        Args:
            side: -1 做空策略，0 中性策略，1 做多策略
            base_price: 基准价格
            grid_count: 网格数量（每侧）
            grid_amount: 单网格挂单量
            grid_spread: 单网格价差（百分比）

        Returns:
            订单列表，每个元素为 (is_ask, price, amount) 元组
        """
        orders = []

        from .grid_state import round_price_to_precision
        # 生成买单（ask=False）：基准价格下方
        if side != -1:
            for i in range(1, grid_count + 1):
                buy_price = base_price * (1 - grid_spread * i / 100)
                orders.append((False, round_price_to_precision(buy_price), grid_amount))

        # 生成卖单（ask=True）：基准价格上方
        if side != 1:
            for i in range(1, grid_count + 1):
                sell_price = base_price * (1 + grid_spread * i / 100)
                orders.append((True, round_price_to_precision(sell_price), grid_amount))

        return orders
    
    async def place_grid_orders(self, side: int, base_price: float, grid_count: int,
                               grid_amount: float, grid_spread: float) -> bool:
        """
        放置双向网格订单（中性策略）

        Args:
            side: -1 做空策略，0 中性策略，1 做多策略
            base_price: 基准价格
            grid_count: 网格数量（每侧）
            grid_amount: 单网格挂单量
            grid_spread: 单网格价差（百分比）

        Returns:
            bool: 是否成功放置所有订单
        """
        from .grid_state import OPEN_SIDE_IS_ASK, CLOSE_SIDE_IS_ASK
            
        try:
            # 生成网格订单
            orders = self.generate_grid_orders(side, base_price, grid_count,
                                              grid_amount, grid_spread)

            logger.info(f"生成双向网格订单: 基准价格={base_price}, "
                        f"网格数量={grid_count}, 单网格量={grid_amount}, 价差={grid_spread}%")
            logger.info(f"订单详情: {[(f'卖单' if is_ask else '买单', price, amount) for is_ask, price, amount in orders]}")

            # 分别处理开仓单和平仓单
            open_orders = []
            close_orders = []
            
            for is_ask, price, amount in orders:
                # 判断是开仓单还是平仓单
                is_close_side = (is_ask == CLOSE_SIDE_IS_ASK)
                if is_close_side:
                    close_orders.append((is_ask, price, amount))
                else:
                    open_orders.append((is_ask, price, amount))
            
            all_order_ids = []
            
            # 先处理开仓单（不使用 reduce_only）
            if open_orders:
                success, order_ids = await self.exchange.place_multi_orders(open_orders)
                if success:
                    all_order_ids.extend(order_ids)
                else:
                    logger.error("批量发送开仓订单失败")
                    return False
            
            # 再处理平仓单（使用 reduce_only=True）
            if close_orders:
                for is_ask, price, amount in close_orders:
                    success, order_id, _, _ = await self.place_single_order(
                        is_ask=is_ask,
                        price=price,
                        amount=amount,
                        reduce_only=True,  # 平仓单使用 Reduce Only
                    )
                    if success:
                        all_order_ids.append(order_id)
                    else:
                        logger.error(f"平仓单下单失败: is_ask={is_ask}, price={price}")
                        # 如果失败，取消已创建的订单
                        if all_order_ids:
                            await self.cancel_grid_orders(all_order_ids)
                        return False

            if all_order_ids:
                logger.info(f"成功放置 {len(all_order_ids)} 个网格订单 (开仓: {len(open_orders)}, 平仓: {len(close_orders)})")
                return True
            else:
                logger.error("没有成功放置任何订单")
                return False

        except Exception as e:
            logger.exception(f"放置网格订单时发生错误: {e}")
            return False
        
    async def place_multi_orders(self, orders: List[Tuple[bool, float, float]]) -> Tuple[bool, List[str]]:
        """
        放置多个订单

        Args:
            orders: 订单列表，每个元素为 (is_ask, price, amount) 元组

        Returns:
            Tuple[bool, List[str]]: (是否成功放置所有订单, 订单ID列表)
        """
        from .grid_state import round_price_to_precision
        orders = [(is_ask, round_price_to_precision(price), amount) for is_ask, price, amount in orders]
        return await self.exchange.place_multi_orders(orders)
    

    async def place_single_order(self, is_ask: bool, price: float, amount: float, reduce_only: bool = False) -> Tuple[bool, str, str, object]:
        """
        放置单个订单

        Args:
            is_ask: 是否为卖单
            price: 价格
            amount: 数量
            reduce_only: 是否仅减仓（平仓单使用，不能开新仓）

        Returns:
            Tuple[bool, str, str, object]: (是否成功, 订单ID, 错误信息, error_code)
        """
        from .grid_state import round_price_to_precision
        price = round_price_to_precision(price)
        # Check if exchange supports reduce_only parameter
        if hasattr(self.exchange, 'place_single_order'):
            import inspect
            sig = inspect.signature(self.exchange.place_single_order)
            if 'reduce_only' in sig.parameters:
                result = await self.exchange.place_single_order(is_ask, price, amount, reduce_only=reduce_only)
            else:
                result = await self.exchange.place_single_order(is_ask, price, amount)
        else:
            result = await self.exchange.place_single_order(is_ask, price, amount)
        # 兼容 2/3/4 个值的 exchange
        if len(result) >= 4:
            return result
        if len(result) >= 3:
            return result[0], result[1], result[2], None
        return result[0], result[1], "", None
            
    async def place_single_market_order(self, is_ask: bool, price: float, amount: float) -> Tuple[bool, str]:
        """
        放置市价单

        Args:
            is_ask: 是否为卖单
            amount: 数量

        Returns:
            Tuple[bool, str]: (是否成功放置订单, 订单ID)
        """
        return await self.exchange.place_single_market_order(is_ask, price, amount)

    async def cancel_grid_orders(self, order_ids: List[str]) -> bool:
        """
        取消网格订单

        Args:
            order_ids: 要取消的订单ID列表

        Returns:
            bool: 是否成功取消订单
        """
        return await self.exchange.cancel_grid_orders(order_ids)
            
    async def modify_grid_order(self, order_id: int, new_price: float, new_amount: float) -> bool:
        """
        修改网格订单

        Args:
            order_id: 订单ID
            new_price: 新价格
            new_amount: 新数量

        Returns:
            bool: 是否成功修改订单
        """
        return await self.exchange.modify_order(order_id, new_price, new_amount)
            
    async def get_orders_by_rest(self) -> List[dict]:
        """
        通过REST API获取当前账户的所有订单

        Returns:
            订单列表或None（如果获取失败）
        """
        return await self.exchange.get_orders_by_rest()
            
    async def get_trades_by_rest(self, ask_filter: int, limit: int) -> List[dict]:
        """
        通过REST API获取当前账户的所有成交记录

        Returns:
            成交记录列表或None（如果获取失败）
        """
        return await self.exchange.get_trades_by_rest(ask_filter, limit)

    @staticmethod
    def _resolution_to_binance_interval(resolution: str) -> str:
        """
        Convert resolution to Binance interval format.
        """
        mapping = {
            "1m": "1m",
            "3m": "3m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1h": "1h",
            "4h": "4h",
            "1d": "1d",
        }
        if resolution not in mapping:
            raise ValueError(f"Unsupported resolution: {resolution}")
        return mapping[resolution]
    
    async def candle_stick(
        self,
        market_id: int,
        resolution: str = "1m",
        count_back: int = 100,
    ) -> pd.DataFrame:
        """
        Get candlestick data from Binance.
        """
        try:
            # 风控K线交易对完全来自配置，避免代码内硬编码币种
            binance_symbol = self.risk_binance_symbol
            
            # Convert resolution to Binance interval
            binance_interval = self._resolution_to_binance_interval(resolution)
            
            # Use BinanceMarketData to fetch klines
            from exchanges.common_market_data import BinanceMarketData
            binance_data = BinanceMarketData()
            
            # Run sync kline HTTP call in thread to avoid blocking event loop.
            df = await asyncio.to_thread(
                binance_data.get_klines_df,
                symbol=binance_symbol,
                interval=binance_interval,
                limit=count_back,
                timeout=15,
            )
            
            # Rename columns to match expected format
            if not df.empty:
                df = df.rename(columns={
                    "open_time": "time",
                    "open": "open",
                    "high": "high",
                    "low": "low",
                    "close": "close",
                    "volume": "volume"
                })
                
                # Select only the columns we need
                df = df[["time", "open", "high", "low", "close", "volume"]]
            
            return df
            
        except Exception as e:
            logger.error(
                f"candle_stick error for market_id {market_id} ({binance_symbol}): {e}"
            )
            return pd.DataFrame()

    async def is_yindie(
        self,
        df: pd.DataFrame,
        ema_period: int = 20,
        adx_period: int = 14,
        rsi_period: int = 14,
    ) -> Tuple[bool, Dict[str, Any]]:
        if df is None or len(df) < max(ema_period, adx_period, rsi_period):
            logger.warning("阴跌检测数据不足")
            return False, {"reason": "insufficient_data"}

        ema_series = quota.compute_ema(df, period=ema_period)
        rsi_series = quota.compute_rsi(df, period=rsi_period)
        adx_series, pdi_series, mdi_series = quota.compute_adx(df, period=adx_period)

        ema_value = float(ema_series.iloc[-1])
        rsi_value = float(rsi_series.iloc[-1])
        adx_value = float(adx_series.iloc[-1])
        pdi_value = float(pdi_series.iloc[-1])
        mdi_value = float(mdi_series.iloc[-1])
        close_value = float(df["close"].iloc[-1])

        is_downtrend = close_value < ema_value
        has_trend = adx_value > 25 and pdi_value < mdi_value
        weak_rsi = rsi_value < 50

        result = is_downtrend and has_trend and weak_rsi
        details = {
            "close": close_value,
            "ema": ema_value,
            "adx": adx_value,
            "pdi": pdi_value,
            "mdi": mdi_value,
            "rsi": rsi_value,
        }
        # logger.info("阴跌检测: %s", details | {"result": result})
        return result, details

    async def is_jidie(
        self,
        df: pd.DataFrame,
        close: Optional[float] = None,
        atr_period: int = 7,
        atr_ma_period: int = 60,
        fall_threshold: float = 15.0,
        atr_multiplier_threshold: float = 3.0,
    ) -> Tuple[bool, Dict[str, Any]]:
        if df is None or len(df) < max(atr_period, atr_ma_period):
            logger.warning("急跌检测数据不足")
            return False, {"reason": "insufficient_data"}

        atr_series = quota.compute_atr(df, period=atr_period)
        atr_ma_series = atr_series.rolling(window=atr_ma_period).mean()

        atr_value = float(atr_series.iloc[-1])
        atr_ma_value = float(atr_ma_series.iloc[-1]) if not pd.isna(atr_ma_series.iloc[-1]) else 0.0
        atr_multiplier = atr_value / atr_ma_value if atr_ma_value else float("inf")

        latest_open = float(df["open"].iloc[-1])
        latest_close = float(df["close"].iloc[-1])
        if close is not None:
            latest_close = close
        fall_amount = latest_open - latest_close

        fall_condition = fall_amount > fall_threshold
        atr_condition = (
            atr_ma_value > 0
            and atr_value > atr_multiplier_threshold * atr_ma_value
        )
        result = fall_condition or atr_condition

        details = {
            "open": latest_open,
            "close": latest_close,
            "fall_amount": fall_amount,
            "atr": atr_value,
            "atr_ma": atr_ma_value,
            "atr_multiplier": atr_multiplier,
        }
        # logger.info("急跌检测: %s", details | {"result": result})
        return result, details
    
    async def current_atr(
        self,
        df: pd.DataFrame,
        atr_period: int = 7,
    ) -> Optional[float]:
        if df is None or len(df) < atr_period:
            logger.warning("ATR计算数据不足")
            return None

        atr_series = quota.compute_atr(df, period=atr_period)
        atr_value = float(atr_series.iloc[-1])
        return atr_value
    
    async def ema_mean_reversion_filter(
        self,
        df: pd.DataFrame,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        EMA乖离率熔断过滤器
        
        计算乖离率：Distance = (Price - 15m_EMA60) / 15m_EMA60
        如果 Distance > 0.02 (偏离2%)：返回True，表示触发熔断
        
        Args:
            df: K线数据DataFrame，包含close列
            
        Returns:
            Tuple[bool, Dict[str, Any]]: (是否触发熔断, 指标数据)
        """
        if df is None or len(df) < 60:
            logger.warning("EMA乖离率熔断检测数据不足")
            return False, {"reason": "insufficient_data"}
        
        try:
            # 计算15分钟EMA60
            ema_60 = quota.compute_ema(df, period=60, column="close")
            ema_value = float(ema_60.iloc[-1])
            
            # 获取当前价格（最新收盘价）
            current_price = float(df["close"].iloc[-1])
            
            # 计算乖离率
            distance = (current_price - ema_value) / ema_value
            
            # 判断是否触发熔断（偏离超过2%）
            is_triggered = distance > 0.02
            
            # 构建指标数据
            metrics = {
                "current_price": current_price,
                "ema_60": ema_value,
                "distance": round(distance, 6),
                "threshold": 0.02,
            }
            
            # logger.info(f"EMA乖离率熔断检测: 价格={current_price:.2f}, EMA60={ema_value:.2f}, "
            #            f"乖离率={distance:.4f}, 阈值=0.02, 触发={is_triggered}")
            
            return is_triggered, metrics
            
        except Exception as e:
            logger.error(f"EMA乖离率熔断检测时发生错误: {e}")
            return False, {"reason": f"calculation_error: {str(e)}"}

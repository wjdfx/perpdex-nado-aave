#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
获取币安K线数据和实时价格的方法
"""
import requests
import pandas as pd
import asyncio
import websockets
import aiohttp
import json
import time
import math
import logging
from typing import Optional, Dict, List, Callable, Any
from collections import deque

logger = logging.getLogger(__name__)

# 波动检测器
class AbsoluteMoveDetector:
    def __init__(
        self,
        window_ms=200,
        danger_threshold=60.0,
        recover_threshold=25.0,
    ):
        self.window_ms = window_ms
        self.danger_threshold = danger_threshold
        self.recover_threshold = recover_threshold
        self.danger_move = 0.0

        self.prices = deque()  # (timestamp_ms, mid)
        self.state = "NORMAL"

    def on_book(self, bid, ask):
        mid = (bid + ask) * 0.5
        now = time.time() * 1000  # ms

        # 忽略价格没变化的推送（关键）
        if self.prices and self.prices[-1][1] == mid:
            return

        self.prices.append((now, mid))
        self._expire_old(now)
        self._check()

    def _expire_old(self, now):
        cutoff = now - self.window_ms
        while self.prices and self.prices[0][0] < cutoff:
            self.prices.popleft()

    def _check(self):
        if len(self.prices) < 2:
            return

        mids = [p for _, p in self.prices]
        danger_move = max(mids) - min(mids)
        self.danger_move = danger_move

        if danger_move >= self.danger_threshold:
            if self.state != "HIGH_RISK":
                self.state = "HIGH_RISK"
                self.on_high_risk(danger_move)
        elif danger_move <= self.recover_threshold:
            if self.state != "NORMAL":
                self.state = "NORMAL"
                self.on_recover(danger_move)

    def on_high_risk(self, danger_move):
        print(
            f"[HIGH_RISK] "
            f"{danger_move:.2f} USDT in {self.window_ms}ms"
        )

    def on_recover(self, danger_move):
        print(
            f"[RECOVER] "
            f"{danger_move:.2f} USDT"
        )

class BinanceMarketData:
    """
    获取币安市场数据的类
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        detector: Optional[AbsoluteMoveDetector] = None
    ):
        """
        初始化BinanceMarketData类
        
        Args:
            api_key (Optional[str]): API密钥，可选
            api_secret (Optional[str]): API秘钥，可选
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.binance.com"
        self.detector = detector

    async def stream_price(
        self,
        symbol: str,
        on_price: Optional[Callable[[Dict[str, Any]], None]] = None,
        stop_event: Optional[asyncio.Event] = None,
        reconnect_delay: float = 5.0,
        proxy: Optional[str] = None) -> None:
        """
        通过币安WebSocket订阅实时成交价格。

        Args:
            symbol (str): 交易对符号，例如"BTCUSDT"。
            on_price (Optional[Callable[[Dict[str, Any]], None]]): 收到价格时的回调函数，签名为callback(data)。
            stop_event (Optional[asyncio.Event]): 外部停止信号，设置后结束订阅。
            reconnect_delay (float): 发生异常后重连前的等待秒数。
            proxy (Optional[str]): 代理地址，例 "http://127.0.0.1:7890"，留空则直连。
        """
        # stream_url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
        stream_url = f"wss://stream.binance.com:9443/stream?streams={symbol.lower()}@bookTicker"

        async def handle_price(data: Dict[str, Any]) -> None:
            payload = data["data"]
            # logger.info(payload)

            bid = float(payload["b"])
            ask = float(payload["a"])
            if self.detector:
                self.detector.on_book(bid, ask)
            if on_price:
                await on_price(data)
            
        while True:
            if stop_event and stop_event.is_set():
                break

            try:
                if proxy:
                    timeout = aiohttp.ClientTimeout(total=None)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.ws_connect(stream_url, proxy=proxy, heartbeat=20) as ws:
                            async for msg in ws:
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    data = json.loads(msg.data)
                                    await handle_price(data)
                                    if stop_event and stop_event.is_set():
                                        await ws.close()
                                        return
                                elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    err = ws.exception()
                                    raise err if err else ConnectionError(f"WebSocket closed with type {msg.type}")
                else:
                    async with websockets.connect(stream_url, ping_interval=20, ping_timeout=20) as ws:
                        async for message in ws:
                            data = json.loads(message)
                            await handle_price(data)
                            if stop_event and stop_event.is_set():
                                await ws.close()
                                return
            except Exception as exc:
                exc_type = type(exc).__name__
                print(f"WebSocket异常[{exc_type}]: {exc!r}，{reconnect_delay}s后重连……")
                await asyncio.sleep(reconnect_delay)

    def get_klines(self, symbol: str, interval: str, limit: int = 100, **kwargs) -> List[Dict]:
        """
        获取K线数据
        
        Args:
            symbol (str): 交易对符号，例如"BTCUSDT"
            interval (str): K线间隔，例如"1m", "5m", "1h", "1d"等
            limit (int): 返回的K线数量，默认为500，最大为1000
            **kwargs: 其他参数，例如startTime, endTime
            
        Returns:
            List[Dict]: K线数据列表
        """
        endpoint = f"/api/v3/klines"
        timeout = kwargs.pop("timeout", 15)
        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
            **kwargs
        }
        
        url = self.base_url + endpoint
        response = requests.get(url, params=params, timeout=timeout)
        
        if response.status_code != 200:
            raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")
        
        data = response.json()
        
        # 转换为更易读的格式
        klines = []
        for item in data:
            kline = {
                "open_time": item[0],
                "open": float(item[1]),
                "high": float(item[2]),
                "low": float(item[3]),
                "close": float(item[4]),
                "volume": float(item[5]),
                "close_time": item[6],
                "quote_asset_volume": float(item[7]),
                "number_of_trades": int(item[8]),
                "taker_buy_base_asset_volume": float(item[9]),
                "taker_buy_quote_asset_volume": float(item[10]),
                "ignore": item[11]
            }
            klines.append(kline)
        
        return klines
    
    def get_klines_df(self, symbol: str, interval: str, limit: int = 100, **kwargs) -> pd.DataFrame:
        """
        获取K线数据并转换为DataFrame
        
        Args:
            symbol (str): 交易对符号，例如"BTCUSDT"
            interval (str): K线间隔，例如"1m", "5m", "1h", "1d"等
            limit (int): 返回的K线数量，默认为500，最大为1000
            **kwargs: 其他参数，例如startTime, endTime
             
        Returns:
            pd.DataFrame: K线数据DataFrame
        """
        klines = self.get_klines(symbol, interval, limit, **kwargs)
        df = pd.DataFrame(klines)
        
        # 转换时间戳为可读格式
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
        
        return df
    
async def main():
    detector = AbsoluteMoveDetector(
        short_window=50,
        base_window=1800,
        vol_ratio_threshold=5.0,
    )

    async def price_callback(data: Dict[str, Any]) -> None:
        payload = data["data"]
        bid = float(payload["b"])
        ask = float(payload["a"])
        # print(f"Price update - Bid: {bid}, Ask: {ask}")
        print(f"Price update - Bid: {bid}, Ask: {ask}, Detector State: {detector.state}")

    stop = asyncio.Event()
    feed = BinanceMarketData(detector=detector)
    await feed.stream_price(
        "BTCUSDT",
        on_price=price_callback,
        stop_event=stop,
        proxy="http://127.0.0.1:7890",
    )
    
if __name__ == "__main__":
    # binance_data = BinanceMarketData()
    # df = binance_data.get_klines_df("BTCUSDT", "1h", limit=10)
    # print(df.head())
    
    asyncio.run(main())

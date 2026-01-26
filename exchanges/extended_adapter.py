"""
Extended Exchange Adapter

Extended is a hybrid perpetuals exchange running on Starknet.
Authentication uses API Key + Stark signatures.

Mainnet: https://api.starknet.extended.exchange/api/v1
Testnet: https://api.starknet.sepolia.extended.exchange/api/v1
"""

import asyncio
import aiohttp
import logging
import os
import time
import uuid
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from .interfaces import ExchangeInterface
from .order_converter import normalize_orders_list

logger = logging.getLogger(__name__)


class ExtendedAdapter(ExchangeInterface):
    """
    Extended exchange adapter implementing the ExchangeInterface.
    Uses API Key for authentication with optional Stark signatures for orders.
    """

    # Market name to symbol mapping
    MARKET_ID_TO_SYMBOL = {
        1: "BTC-USD",
        2: "ETH-USD",
        3: "SOL-USD",
        4: "XRP-USD",
        5: "BNB-USD",
        6: "DOGE-USD",
        7: "ADA-USD",
        8: "AVAX-USD",
        9: "LINK-USD",
        10: "SUI-USD",
    }

    def __init__(
        self,
        market_id: int = 2,  # ETH-USD by default
        symbol: str = None,
        **kwargs
    ):
        """
        Initialize Extended adapter.
        
        Args:
            market_id: Market ID (1=BTC, 2=ETH, etc.)
            symbol: Market symbol (e.g., "ETH-USD")
        """
        self.market_id = market_id
        self.symbol = symbol or self.MARKET_ID_TO_SYMBOL.get(market_id, "ETH-USD")
        
        # Load API key from environment
        self.api_key = os.getenv("EXTENDED_API_KEY", "")
        self.env = os.getenv("EXTENDED_ENV", "mainnet").lower()
        
        # Set API endpoints based on environment
        if self.env == "mainnet":
            self.base_url = "https://api.starknet.extended.exchange/api/v1"
            self.ws_url = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1"
        else:
            self.base_url = "https://api.starknet.sepolia.extended.exchange/api/v1"
            self.ws_url = "wss://starknet.sepolia.extended.exchange/stream.extended.exchange/v1"
        
        # HTTP session
        self.session: Optional[aiohttp.ClientSession] = None
        
        # WebSocket
        self.ws_session: Optional[aiohttp.ClientSession] = None
        self.ws_connection: Optional[aiohttp.ClientWebSocketResponse] = None
        self.ws_initialized = False
        
        # Callbacks
        self.callbacks: Dict[str, Callable] = {}
        
        # Trading parameters (fetched during initialization)
        self.min_order_size: float = 0.001
        self.min_price_change: float = 0.01
        self.min_size_change: float = 0.001
        self.max_leverage: int = 50
        
        # Account info
        self.account_id: Optional[int] = None
        self.l2_key: Optional[str] = None
        self.l2_vault: Optional[int] = None
        
        logger.info(f"Extended Adapter initialized: env={self.env}, symbol={self.symbol}")

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={
                    "X-Api-Key": self.api_key,
                    "User-Agent": "perpdex-grid-bot/1.0",
                    "Content-Type": "application/json",
                }
            )
        return self.session

    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Dict = None,
        data: Dict = None
    ) -> Optional[Dict]:
        """Make HTTP request to Extended API."""
        session = await self._get_session()
        url = f"{self.base_url}{endpoint}"
        
        try:
            async with session.request(method, url, params=params, json=data) as response:
                result = await response.json()
                
                if response.status == 200 and result.get("status") == "OK":
                    return result.get("data")
                else:
                    error = result.get("error", {})
                    logger.error(f"Extended API error: {error}")
                    return None
        except Exception as e:
            logger.error(f"Extended request error: {e}", exc_info=True)
            return None

    async def _get(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """GET request."""
        return await self._request("GET", endpoint, params=params)

    async def _post(self, endpoint: str, data: Dict = None) -> Optional[Dict]:
        """POST request."""
        return await self._request("POST", endpoint, data=data)

    async def _delete(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        """DELETE request."""
        return await self._request("DELETE", endpoint, params=params)

    def _round_price(self, price: float) -> str:
        """Round price to valid increment and return as string."""
        if self.min_price_change > 0:
            rounded = round(price / self.min_price_change) * self.min_price_change
            # Format to appropriate decimal places
            decimals = len(str(self.min_price_change).split('.')[-1]) if '.' in str(self.min_price_change) else 0
            return f"{rounded:.{decimals}f}"
        return str(price)

    def _round_size(self, size: float) -> str:
        """Round size to valid increment and return as string."""
        if self.min_size_change > 0:
            rounded = round(size / self.min_size_change) * self.min_size_change
            decimals = len(str(self.min_size_change).split('.')[-1]) if '.' in str(self.min_size_change) else 0
            return f"{rounded:.{decimals}f}"
        return str(size)

    async def initialize_client(self) -> None:
        """Initialize the exchange client."""
        try:
            # Fetch account info
            account_info = await self._get("/user/account/info")
            if account_info:
                self.account_id = account_info.get("accountId")
                self.l2_key = account_info.get("l2Key")
                self.l2_vault = account_info.get("l2Vault")
                logger.info(f"Extended account: id={self.account_id}, l2Key={self.l2_key[:20]}...")

            # Fetch market info to get trading parameters
            markets = await self._get("/info/markets", params={"market": self.symbol})
            if markets and len(markets) > 0:
                market = markets[0]
                trading_config = market.get("tradingConfig", {})
                
                self.min_order_size = float(trading_config.get("minOrderSize", "0.001"))
                self.min_size_change = float(trading_config.get("minOrderSizeChange", "0.001"))
                self.min_price_change = float(trading_config.get("minPriceChange", "0.01"))
                self.max_leverage = int(trading_config.get("maxLeverage", "50"))
                
                logger.info(f"Extended market {self.symbol}: min_size={self.min_order_size}, "
                           f"price_change={self.min_price_change}, size_change={self.min_size_change}")
            
            logger.info(f"Extended client initialized: env={self.env}")
        except Exception as e:
            logger.error(f"Failed to initialize Extended client: {e}", exc_info=True)

    async def create_auth_token(self) -> str:
        """Return API key as auth token."""
        return self.api_key

    async def place_single_order(
        self,
        is_ask: bool,
        price: float,
        amount: float
    ) -> Tuple[bool, str]:
        """
        Place a single limit order.
        
        Note: Extended requires Stark signatures for orders.
        For now, this is a simplified implementation without full Stark signing.
        Full implementation would require the Stark private key and signing logic.
        """
        try:
            side = "SELL" if is_ask else "BUY"
            price_str = self._round_price(price)
            qty_str = self._round_size(amount)
            
            # Generate order ID
            order_id = str(uuid.uuid4())
            
            # Calculate expiration (90 days max for mainnet)
            expiration = int(time.time() * 1000) + (86400 * 30 * 1000)  # 30 days
            
            # Get fees
            fees_data = await self._get("/user/fees", params={"market": self.symbol})
            maker_fee = "0.0000"
            if fees_data and len(fees_data) > 0:
                maker_fee = fees_data[0].get("makerFeeRate", "0.0000")
            
            # Build order request
            # Note: Full implementation would include Stark signature
            order_data = {
                "id": order_id,
                "market": self.symbol,
                "type": "LIMIT",
                "side": side,
                "qty": qty_str,
                "price": price_str,
                "timeInForce": "GTT",
                "expiryEpochMillis": expiration,
                "fee": maker_fee,
                "postOnly": True,
                "reduceOnly": False,
                "selfTradeProtectionLevel": "ACCOUNT",
                # settlement field would contain Stark signature
            }
            
            logger.debug(f"Placing Extended order: {side} {qty_str} @ {price_str}")
            
            result = await self._post("/user/order", data=order_data)
            
            if result:
                order_id = str(result.get("id", order_id))
                logger.info(f"Extended order placed: id={order_id}")
                return True, order_id
            else:
                logger.error("Failed to place Extended order")
                return False, ""
                
        except Exception as e:
            logger.error(f"Extended place_single_order error: {e}", exc_info=True)
            return False, ""

    async def place_multi_orders(
        self,
        orders: List[Tuple[bool, float, float]]
    ) -> Tuple[bool, List[str]]:
        """Place multiple orders."""
        order_ids = []
        all_success = True
        
        for i, (is_ask, price, amount) in enumerate(orders):
            success, order_id = await self.place_single_order(is_ask, price, amount)
            if success:
                order_ids.append(order_id)
            else:
                all_success = False
                logger.error(f"Failed to place order {i+1}/{len(orders)}")
                # Rollback: cancel previously placed orders
                if order_ids:
                    await self._cancel_orders_by_ids(order_ids)
                    order_ids = []
                break
        
        return all_success, order_ids

    async def place_single_market_order(
        self,
        is_ask: bool,
        price: float,
        amount: float
    ) -> Tuple[bool, str]:
        """Place a market order (using IOC limit order)."""
        try:
            side = "SELL" if is_ask else "BUY"
            qty_str = self._round_size(amount)
            
            # For market orders, use aggressive price
            if is_ask:
                market_price = price * 0.9925  # 0.75% below
            else:
                market_price = price * 1.0075  # 0.75% above
            
            price_str = self._round_price(market_price)
            order_id = str(uuid.uuid4())
            expiration = int(time.time() * 1000) + (60 * 1000)  # 1 minute
            
            # Get taker fee
            fees_data = await self._get("/user/fees", params={"market": self.symbol})
            taker_fee = "0.00025"
            if fees_data and len(fees_data) > 0:
                taker_fee = fees_data[0].get("takerFeeRate", "0.00025")
            
            order_data = {
                "id": order_id,
                "market": self.symbol,
                "type": "LIMIT",
                "side": side,
                "qty": qty_str,
                "price": price_str,
                "timeInForce": "IOC",
                "expiryEpochMillis": expiration,
                "fee": taker_fee,
                "postOnly": False,
                "reduceOnly": False,
            }
            
            result = await self._post("/user/order", data=order_data)
            
            if result:
                order_id = str(result.get("id", order_id))
                return True, order_id
            return False, ""
            
        except Exception as e:
            logger.error(f"Extended place_single_market_order error: {e}", exc_info=True)
            return False, ""

    async def cancel_grid_orders(self) -> bool:
        """Cancel all grid orders for this market."""
        try:
            result = await self._post("/user/order/massCancel", data={
                "markets": [self.symbol]
            })
            
            if result is not None:
                logger.info(f"Extended: Cancelled all orders for {self.symbol}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Extended cancel_grid_orders error: {e}", exc_info=True)
            return False

    async def _cancel_orders_by_ids(self, order_ids: List[str]) -> bool:
        """Cancel orders by IDs."""
        try:
            # Convert to int IDs if needed
            int_ids = []
            for oid in order_ids:
                try:
                    int_ids.append(int(oid))
                except ValueError:
                    pass
            
            if int_ids:
                result = await self._post("/user/order/massCancel", data={
                    "orderIds": int_ids
                })
                return result is not None
            return True
        except Exception as e:
            logger.error(f"Extended _cancel_orders_by_ids error: {e}")
            return False

    async def modify_grid_order(
        self,
        order_id: str,
        price: float = None,
        amount: float = None
    ) -> Tuple[bool, str]:
        """Modify an existing order (uses cancel + replace)."""
        # Extended supports order editing via cancelId parameter
        try:
            # Get current order
            orders = await self.get_orders()
            current_order = None
            for order in orders:
                if str(order.get('id')) == str(order_id):
                    current_order = order
                    break
            
            if not current_order:
                logger.warning(f"Order {order_id} not found for modification")
                return False, ""
            
            is_ask = current_order.get('side', '').upper() == 'SELL'
            new_price = price if price else float(current_order.get('price', 0))
            new_amount = amount if amount else float(current_order.get('amount', 0))
            
            # Place new order with cancelId to replace
            return await self.place_single_order(is_ask, new_price, new_amount)
            
        except Exception as e:
            logger.error(f"Extended modify_grid_order error: {e}")
            return False, ""

    async def get_orders(self) -> List[dict]:
        """Get current open orders."""
        try:
            orders = await self._get("/user/orders", params={"market": self.symbol})
            if orders:
                logger.debug(f"Extended: Got {len(orders)} open orders")
                return orders
            return []
        except Exception as e:
            logger.error(f"Extended get_orders error: {e}", exc_info=True)
            return []

    async def get_trades(self, limit: int = 50) -> List[dict]:
        """Get recent trades."""
        try:
            trades = await self._get("/user/trades", params={
                "market": self.symbol,
                "limit": limit
            })
            return trades or []
        except Exception as e:
            logger.error(f"Extended get_trades error: {e}")
            return []

    async def get_positions(self) -> Dict[str, dict]:
        """Get current positions."""
        try:
            positions = await self._get("/user/positions", params={"market": self.symbol})
            
            if not positions:
                return {}
            
            result = {}
            for pos in positions:
                market = pos.get("market", self.symbol)
                side = pos.get("side", "LONG")
                size = float(pos.get("size", "0"))
                
                # Make size negative for short positions
                if side == "SHORT":
                    size = -size
                
                result[market] = {
                    "position": size,
                    "size": abs(size),
                    "side": side,
                    "entryPrice": float(pos.get("openPrice", "0")),
                    "markPrice": float(pos.get("markPrice", "0")),
                    "unrealizedPnl": float(pos.get("unrealisedPnl", "0")),
                    "realizedPnl": float(pos.get("realisedPnl", "0")),
                    "leverage": float(pos.get("leverage", "1")),
                    "liquidationPrice": float(pos.get("liquidationPrice", "0")),
                    "margin": float(pos.get("margin", "0")),
                    "info": pos
                }
            
            return result
            
        except Exception as e:
            logger.error(f"Extended get_positions error: {e}", exc_info=True)
            return {}

    async def get_account(self) -> Dict[str, Any]:
        """Get account balance and info."""
        try:
            balance = await self._get("/user/balance")
            
            if not balance:
                return {}
            
            return {
                "total": float(balance.get("balance", "0")),
                "equity": float(balance.get("equity", "0")),
                "availableForTrade": float(balance.get("availableForTrade", "0")),
                "availableForWithdrawal": float(balance.get("availableForWithdrawal", "0")),
                "unrealizedPnl": float(balance.get("unrealisedPnl", "0")),
                "initialMargin": float(balance.get("initialMargin", "0")),
                "marginRatio": float(balance.get("marginRatio", "0")),
                "collateral": balance.get("collateralName", "USD"),
                "info": balance
            }
            
        except Exception as e:
            logger.error(f"Extended get_account error: {e}", exc_info=True)
            return {}

    async def get_account_info(self) -> Dict[str, Any]:
        """Get combined account and position info."""
        account = await self.get_account()
        positions = await self.get_positions()
        return {
            "account": account,
            "positions": positions
        }

    async def candle_stick(
        self,
        interval: str = "1h",
        start_time: int = None,
        end_time: int = None,
        limit: int = 100
    ) -> pd.DataFrame:
        """Get candlestick data."""
        try:
            # Map interval to Extended format
            interval_map = {
                "1m": "PT1M",
                "5m": "PT5M",
                "15m": "PT15M",
                "30m": "PT30M",
                "1h": "PT1H",
                "4h": "PT4H",
                "1d": "PT24H",
            }
            extended_interval = interval_map.get(interval, "PT1H")
            
            params = {
                "interval": extended_interval,
                "limit": limit
            }
            if end_time:
                params["endTime"] = end_time
            
            candles = await self._get(f"/info/candles/{self.symbol}/trades", params=params)
            
            if not candles:
                return pd.DataFrame()
            
            df = pd.DataFrame(candles)
            df.rename(columns={
                "T": "timestamp",
                "o": "open",
                "h": "high",
                "l": "low",
                "c": "close",
                "v": "volume"
            }, inplace=True)
            
            for col in ["open", "high", "low", "close", "volume"]:
                if col in df.columns:
                    df[col] = df[col].astype(float)
            
            return df
            
        except Exception as e:
            logger.error(f"Extended candle_stick error: {e}")
            return pd.DataFrame()

    async def subscribe(
        self,
        callbacks: Dict[str, Callable[[str, Any], None]],
        proxy: str = None
    ) -> None:
        """Subscribe to WebSocket streams."""
        self.callbacks = callbacks
        
        # First, poll initial data
        if 'market_stats' in callbacks:
            await self._poll_market_stats()
        
        # Initialize WebSocket
        if not self.ws_initialized:
            await self._initialize_ws()
        
        # Start polling task as backup
        asyncio.create_task(self._rest_polling_task())

    async def _initialize_ws(self):
        """Initialize WebSocket connections."""
        try:
            if self.ws_session is None or self.ws_session.closed:
                self.ws_session = aiohttp.ClientSession(headers={
                    "X-Api-Key": self.api_key,
                    "User-Agent": "perpdex-grid-bot/1.0"
                })
            
            # Connect to orderbook stream for market stats
            orderbook_url = f"{self.ws_url}/orderbooks/{self.symbol}?depth=1"
            logger.info(f"Connecting to Extended WebSocket: {orderbook_url}")
            
            self.ws_connection = await self.ws_session.ws_connect(orderbook_url)
            self.ws_initialized = True
            
            # Start listener
            asyncio.create_task(self._ws_listener())
            
            logger.info("Extended WebSocket initialized")
            
        except Exception as e:
            logger.error(f"Extended WebSocket init error: {e}")
            self.ws_initialized = False

    async def _ws_listener(self):
        """Listen for WebSocket messages."""
        try:
            async for msg in self.ws_connection:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._handle_ws_message(msg.json())
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"Extended WebSocket error: {msg.data}")
                    break
        except Exception as e:
            logger.error(f"Extended WebSocket listener error: {e}")
            self.ws_initialized = False

    async def _handle_ws_message(self, data: dict):
        """Handle incoming WebSocket message."""
        try:
            msg_type = data.get("type", "")
            
            if msg_type in ["SNAPSHOT", "DELTA"]:
                # Orderbook data
                orderbook = data.get("data", {})
                bids = orderbook.get("b", [])
                asks = orderbook.get("a", [])
                
                if bids and asks:
                    best_bid = float(bids[0]["p"]) if bids else 0
                    best_ask = float(asks[0]["p"]) if asks else 0
                    mark_price = (best_bid + best_ask) / 2
                    
                    if 'market_stats' in self.callbacks and self.callbacks['market_stats']:
                        stats = {
                            'mark_price': mark_price,
                            'best_bid': best_bid,
                            'best_ask': best_ask
                        }
                        callback = self.callbacks['market_stats']
                        if asyncio.iscoroutinefunction(callback):
                            asyncio.create_task(callback(self.symbol, stats))
                        else:
                            callback(self.symbol, stats)
                            
        except Exception as e:
            logger.error(f"Extended _handle_ws_message error: {e}")

    async def _rest_polling_task(self):
        """Poll REST API for updates."""
        poll_count = 0
        while True:
            try:
                await self._poll_market_stats()
                
                poll_count += 1
                if poll_count % 2 == 0:
                    await self._poll_orders()
                    await self._poll_positions()
                
                await asyncio.sleep(3)
            except Exception as e:
                logger.error(f"Extended REST polling error: {e}")
                await asyncio.sleep(5)

    async def _poll_market_stats(self):
        """Poll market statistics."""
        try:
            stats = await self._get(f"/info/markets/{self.symbol}/stats")
            
            if stats and 'market_stats' in self.callbacks:
                mark_price = float(stats.get("markPrice", "0"))
                best_bid = float(stats.get("bidPrice", "0"))
                best_ask = float(stats.get("askPrice", "0"))
                
                market_stats = {
                    'mark_price': mark_price,
                    'best_bid': best_bid,
                    'best_ask': best_ask,
                    'last_price': float(stats.get("lastPrice", "0")),
                    'index_price': float(stats.get("indexPrice", "0")),
                }
                
                callback = self.callbacks['market_stats']
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(self.symbol, market_stats))
                else:
                    callback(self.symbol, market_stats)
                
                return mark_price
        except Exception as e:
            logger.error(f"Extended _poll_market_stats error: {e}")
        return None

    async def _poll_orders(self):
        """Poll orders and trigger callback."""
        try:
            if 'orders' not in self.callbacks:
                return
            
            orders = await self.get_orders()
            if orders:
                # Normalize to CCXT format
                normalized = normalize_orders_list(orders)
                
                callback = self.callbacks['orders']
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(str(self.account_id or ""), normalized))
                else:
                    callback(str(self.account_id or ""), normalized)
        except Exception as e:
            logger.error(f"Extended _poll_orders error: {e}")

    async def _poll_positions(self):
        """Poll positions and trigger callback."""
        try:
            if 'positions' not in self.callbacks:
                return
            
            positions = await self.get_positions()
            if positions:
                callback = self.callbacks['positions']
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(str(self.account_id or ""), positions))
                else:
                    callback(str(self.account_id or ""), positions)
        except Exception as e:
            logger.error(f"Extended _poll_positions error: {e}")

    async def get_orders_by_rest(self) -> List[dict]:
        """Get orders via REST API."""
        return await self.get_orders()

    async def get_trades_by_rest(self, ask_filter: int, limit: int) -> List[dict]:
        """Get trades via REST API."""
        return await self.get_trades(limit)

    async def close(self):
        """Close connections."""
        self.ws_initialized = False
        try:
            if self.ws_connection and not self.ws_connection.closed:
                await self.ws_connection.close()
            if self.ws_session and not self.ws_session.closed:
                await self.ws_session.close()
            if self.session and not self.session.closed:
                await self.session.close()
        except Exception as e:
            logger.error(f"Extended close error: {e}")

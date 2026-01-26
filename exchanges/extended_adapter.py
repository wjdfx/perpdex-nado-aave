"""
Extended Exchange Adapter

Extended is a hybrid perpetuals exchange running on Starknet.
Authentication uses API Key + Stark signatures for order management.

Mainnet: https://api.starknet.extended.exchange/api/v1
Testnet: https://api.starknet.sepolia.extended.exchange/api/v1

Learned from Nado implementation:
- Price/size precision handling with Decimal
- WebSocket fallback to REST polling
- Initial price fetching before grid initialization
- Robust error handling and logging
"""

import asyncio
import aiohttp
import logging
import math
import os
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd

from .interfaces import ExchangeInterface
from .order_converter import normalize_orders_list

logger = logging.getLogger(__name__)

# Try to import Stark crypto library
try:
    from starknet_py.hash.utils import pedersen_hash
    from starknet_py.net.signer.stark_curve_signer import KeyPair
    STARK_CRYPTO_AVAILABLE = True
except ImportError:
    STARK_CRYPTO_AVAILABLE = False
    logger.warning("starknet_py not available. Stark signing will be disabled.")

# Try fast_stark_crypto (Extended's preferred library)
try:
    from fast_stark_crypto import sign as stark_sign, get_order_msg_hash
    FAST_STARK_AVAILABLE = True
except ImportError:
    FAST_STARK_AVAILABLE = False
    logger.warning("fast_stark_crypto not available. Will try fallback signing.")


@dataclass
class StarknetDomain:
    """Starknet domain for SNIP12 signing."""
    name: str
    version: str
    chain_id: str
    revision: str


# Starknet domain configurations
MAINNET_DOMAIN = StarknetDomain(
    name="Perpetuals",
    version="v0",
    chain_id="SN_MAIN",
    revision="1"
)

TESTNET_DOMAIN = StarknetDomain(
    name="Perpetuals",
    version="v0",
    chain_id="SN_SEPOLIA",
    revision="1"
)


class ExtendedAdapter(ExchangeInterface):
    """
    Extended exchange adapter implementing the ExchangeInterface.
    Uses API Key + Stark signatures for authentication.
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
        self.symbol = symbol or os.getenv("EXTENDED_SYMBOL") or self.MARKET_ID_TO_SYMBOL.get(market_id, "ETH-USD")
        
        # Load credentials from environment
        self.api_key = os.getenv("EXTENDED_API_KEY", "")
        self.stark_private_key = os.getenv("EXTENDED_STARK_PRIVATE_KEY", "")
        self.env = os.getenv("EXTENDED_ENV", "mainnet").lower()
        
        # Set API endpoints based on environment
        if self.env == "mainnet":
            self.base_url = "https://api.starknet.extended.exchange/api/v1"
            self.ws_url = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1"
            self.starknet_domain = MAINNET_DOMAIN
        else:
            self.base_url = "https://api.starknet.sepolia.extended.exchange/api/v1"
            self.ws_url = "wss://api.starknet.sepolia.extended.exchange/stream.extended.exchange/v1"
            self.starknet_domain = TESTNET_DOMAIN
        
        # HTTP session
        self.session: Optional[aiohttp.ClientSession] = None
        
        # WebSocket
        self.ws_session: Optional[aiohttp.ClientSession] = None
        self.ws_connection: Optional[aiohttp.ClientWebSocketResponse] = None
        self.ws_initialized = False
        
        # Callbacks
        self.callbacks: Dict[str, Callable] = {}
        
        # Trading parameters (fetched during initialization)
        # Use Decimal for precision (learned from Nado)
        self.min_order_size: Decimal = Decimal("0.001")
        self.min_price_change: Decimal = Decimal("0.01")
        self.min_size_change: Decimal = Decimal("0.001")
        self.max_leverage: int = 50
        self.collateral_decimals: int = 6
        
        # L2 config for signing
        self.synthetic_id: Optional[str] = None
        self.collateral_id: Optional[str] = None
        self.synthetic_resolution: int = 1
        self.collateral_resolution: int = 1000000  # 6 decimals for USDC
        
        # Account info
        self.account_id: Optional[int] = None
        self.l2_key: Optional[str] = None  # Stark public key
        self.l2_vault: Optional[int] = None  # Position ID
        self.stark_private_key_int: Optional[int] = None
        self.stark_public_key_int: Optional[int] = None
        
        # Parse Stark private key if provided
        if self.stark_private_key:
            try:
                self.stark_private_key_int = int(self.stark_private_key, 16) if self.stark_private_key.startswith("0x") else int(self.stark_private_key, 16)
            except ValueError:
                logger.warning("Invalid Stark private key format")
        
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
                elif response.status == 404:
                    # Not found is sometimes expected (e.g., no balance)
                    logger.debug(f"Extended API 404: {endpoint}")
                    return None
                else:
                    error = result.get("error", {})
                    logger.error(f"Extended API error ({response.status}): {error}, url={url}")
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

    def _round_price(self, price: float) -> Decimal:
        """Round price to valid increment using Decimal for precision."""
        price_decimal = Decimal(str(price))
        if self.min_price_change > 0:
            # Round to nearest increment
            return (price_decimal / self.min_price_change).quantize(Decimal('1'), rounding=ROUND_DOWN) * self.min_price_change
        return price_decimal

    def _round_size(self, size: float) -> Decimal:
        """Round size to valid increment using Decimal for precision."""
        size_decimal = Decimal(str(size))
        if self.min_size_change > 0:
            return (size_decimal / self.min_size_change).quantize(Decimal('1'), rounding=ROUND_DOWN) * self.min_size_change
        return size_decimal

    def _to_stark_amount(self, human_amount: Decimal, resolution: int) -> int:
        """Convert human-readable amount to Stark (quantum) amount."""
        return int(human_amount * Decimal(resolution))

    def _from_stark_amount(self, stark_amount: int, resolution: int) -> Decimal:
        """Convert Stark (quantum) amount to human-readable amount."""
        return Decimal(stark_amount) / Decimal(resolution)

    def _sign_message(self, msg_hash: int) -> Tuple[int, int]:
        """Sign a message hash with Stark private key."""
        if not self.stark_private_key_int:
            raise ValueError("Stark private key not configured")
        
        if FAST_STARK_AVAILABLE:
            return stark_sign(private_key=self.stark_private_key_int, msg_hash=msg_hash)
        elif STARK_CRYPTO_AVAILABLE:
            # Fallback to starknet_py
            key_pair = KeyPair.from_private_key(self.stark_private_key_int)
            signature = key_pair.sign(msg_hash)
            return (signature[0], signature[1])
        else:
            raise ImportError("No Stark crypto library available. Install fast_stark_crypto or starknet_py.")

    def _calculate_order_hash(
        self,
        synthetic_amount: int,
        collateral_amount: int,
        fee_amount: int,
        nonce: int,
        expiration_seconds: int,
    ) -> int:
        """Calculate order hash for signing."""
        if FAST_STARK_AVAILABLE:
            return get_order_msg_hash(
                position_id=self.l2_vault,
                base_asset_id=int(self.synthetic_id, 16) if self.synthetic_id else 0,
                base_amount=synthetic_amount,
                quote_asset_id=int(self.collateral_id, 16) if self.collateral_id else 0,
                quote_amount=collateral_amount,
                fee_amount=fee_amount,
                fee_asset_id=int(self.collateral_id, 16) if self.collateral_id else 0,
                expiration=expiration_seconds,
                salt=nonce,
                user_public_key=self.stark_public_key_int,
                domain_name=self.starknet_domain.name,
                domain_version=self.starknet_domain.version,
                domain_chain_id=self.starknet_domain.chain_id,
                domain_revision=self.starknet_domain.revision,
            )
        else:
            # Simplified hash for testing (not for production)
            logger.warning("Using simplified order hash - not for production use")
            return hash((synthetic_amount, collateral_amount, fee_amount, nonce, expiration_seconds))

    def _generate_nonce(self) -> int:
        """Generate a unique nonce for orders."""
        # Use timestamp + random bits like Nado
        timestamp_bits = int(time.time() * 1000) & ((1 << 40) - 1)
        random_bits = int.from_bytes(os.urandom(4), 'big') & ((1 << 20) - 1)
        return (timestamp_bits << 20) | random_bits

    async def initialize_client(self) -> None:
        """Initialize the exchange client."""
        try:
            # Fetch account info
            account_info = await self._get("/user/account/info")
            if account_info:
                self.account_id = account_info.get("accountId")
                self.l2_key = account_info.get("l2Key")
                self.l2_vault = account_info.get("l2Vault")
                
                # Parse public key
                if self.l2_key:
                    try:
                        self.stark_public_key_int = int(self.l2_key, 16)
                    except ValueError:
                        pass
                
                logger.info(f"Extended account: id={self.account_id}, vault={self.l2_vault}")

            # Fetch market info to get trading parameters
            markets = await self._get("/info/markets", params={"market": self.symbol})
            if markets and len(markets) > 0:
                market = markets[0]
                trading_config = market.get("tradingConfig", {})
                l2_config = market.get("l2Config", {})
                
                # Use Decimal for precision (learned from Nado)
                self.min_order_size = Decimal(str(trading_config.get("minOrderSize", "0.001")))
                self.min_size_change = Decimal(str(trading_config.get("minOrderSizeChange", "0.001")))
                self.min_price_change = Decimal(str(trading_config.get("minPriceChange", "0.01")))
                self.max_leverage = int(trading_config.get("maxLeverage", "50"))
                
                # L2 config for signing
                self.synthetic_id = l2_config.get("syntheticId")
                self.collateral_id = l2_config.get("collateralId")
                self.synthetic_resolution = int(l2_config.get("syntheticResolution", 1))
                self.collateral_resolution = int(l2_config.get("collateralResolution", 1000000))
                
                logger.info(f"Extended market {self.symbol}: min_size={self.min_order_size}, "
                           f"price_change={self.min_price_change}, size_change={self.min_size_change}")
                logger.info(f"L2 config: synthetic_id={self.synthetic_id}, collateral_id={self.collateral_id}")
            
            logger.info(f"Extended client initialized: env={self.env}")
        except Exception as e:
            logger.error(f"Failed to initialize Extended client: {e}", exc_info=True)

    async def create_auth_token(self) -> str:
        """Return API key as auth token."""
        return self.api_key

    def _build_settlement(
        self,
        side: str,
        price: Decimal,
        qty: Decimal,
        fee_rate: Decimal,
        nonce: int,
        expiration_ms: int,
    ) -> Optional[Dict]:
        """Build settlement data with Stark signature."""
        if not self.stark_private_key_int or not self.l2_vault:
            logger.warning("Stark signing not configured, submitting order without settlement")
            return None
        
        try:
            is_buy = side.upper() == "BUY"
            
            # Calculate amounts in Stark (quantum) units
            synthetic_stark = self._to_stark_amount(qty, self.synthetic_resolution)
            collateral_value = qty * price
            collateral_stark = self._to_stark_amount(collateral_value, self.collateral_resolution)
            fee_stark = self._to_stark_amount(collateral_value * fee_rate, self.collateral_resolution)
            
            # Negate amounts based on side (buy = negative collateral, sell = negative synthetic)
            if is_buy:
                collateral_stark = -collateral_stark
            else:
                synthetic_stark = -synthetic_stark
            
            # Calculate expiration in seconds with 14-day buffer
            expiration_seconds = int((expiration_ms / 1000) + (14 * 24 * 60 * 60))
            
            # Calculate order hash
            order_hash = self._calculate_order_hash(
                synthetic_amount=synthetic_stark,
                collateral_amount=collateral_stark,
                fee_amount=fee_stark,
                nonce=nonce,
                expiration_seconds=expiration_seconds,
            )
            
            # Sign the hash
            r, s = self._sign_message(order_hash)
            
            return {
                "signature": {
                    "r": hex(r),
                    "s": hex(s)
                },
                "starkKey": self.l2_key,
                "collateralPosition": str(self.l2_vault)
            }
        except Exception as e:
            logger.error(f"Failed to build settlement: {e}", exc_info=True)
            return None

    async def place_single_order(
        self,
        is_ask: bool,
        price: float,
        amount: float
    ) -> Tuple[bool, str]:
        """
        Place a single limit order with POST_ONLY.
        """
        try:
            side = "SELL" if is_ask else "BUY"
            
            # Round price and amount using Decimal (learned from Nado)
            price_decimal = self._round_price(price)
            qty_decimal = self._round_size(amount)
            
            logger.debug(f"Placing order: {side} {qty_decimal} @ {price_decimal}")
            
            # Generate order ID and nonce
            order_id = str(uuid.uuid4())
            nonce = self._generate_nonce()
            
            # Calculate expiration (30 days for mainnet, max 90 days)
            expiration_ms = int(time.time() * 1000) + (30 * 24 * 60 * 60 * 1000)
            
            # Get fees
            fees_data = await self._get("/user/fees", params={"market": self.symbol})
            maker_fee = Decimal("0.0000")  # Extended has 0% maker fee
            if fees_data and len(fees_data) > 0:
                maker_fee = Decimal(str(fees_data[0].get("makerFeeRate", "0.0000")))
            
            # Build settlement (Stark signature)
            settlement = self._build_settlement(
                side=side,
                price=price_decimal,
                qty=qty_decimal,
                fee_rate=maker_fee,
                nonce=nonce,
                expiration_ms=expiration_ms,
            )
            
            # Build order request
            order_data = {
                "id": order_id,
                "market": self.symbol,
                "type": "LIMIT",
                "side": side,
                "qty": str(qty_decimal),
                "price": str(price_decimal),
                "timeInForce": "GTT",
                "expiryEpochMillis": expiration_ms,
                "fee": str(maker_fee),
                "nonce": str(nonce),
                "postOnly": True,
                "reduceOnly": False,
                "selfTradeProtectionLevel": "ACCOUNT",
            }
            
            if settlement:
                order_data["settlement"] = settlement
            
            result = await self._post("/user/order", data=order_data)
            
            if result:
                order_id = str(result.get("id", order_id))
                logger.info(f"Extended order placed: id={order_id}, {side} {qty_decimal} @ {price_decimal}")
                return True, order_id
            else:
                logger.error(f"Failed to place Extended order: {side} {qty_decimal} @ {price_decimal}")
                return False, ""
                
        except Exception as e:
            logger.error(f"Extended place_single_order error: {e}", exc_info=True)
            return False, ""

    async def place_multi_orders(
        self,
        orders: List[Tuple[bool, float, float]]
    ) -> Tuple[bool, List[str]]:
        """Place multiple orders with rollback on failure (learned from Nado)."""
        order_ids = []
        all_success = True
        
        for i, (is_ask, price, amount) in enumerate(orders):
            success, order_id = await self.place_single_order(is_ask, price, amount)
            if success:
                order_ids.append(order_id)
            else:
                all_success = False
                logger.error(f"place_multi_orders: Failed to place order {i+1}/{len(orders)}")
                # Rollback: cancel previously placed orders (learned from Nado)
                if order_ids:
                    logger.info(f"Rolling back {len(order_ids)} previously placed orders")
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
            qty_decimal = self._round_size(amount)
            
            # For market orders, use aggressive price (0.75% slippage like Extended UI)
            if is_ask:
                market_price = self._round_price(price * 0.9925)
            else:
                market_price = self._round_price(price * 1.0075)
            
            order_id = str(uuid.uuid4())
            nonce = self._generate_nonce()
            expiration_ms = int(time.time() * 1000) + (60 * 1000)  # 1 minute
            
            # Get taker fee
            fees_data = await self._get("/user/fees", params={"market": self.symbol})
            taker_fee = Decimal("0.00025")  # Default taker fee
            if fees_data and len(fees_data) > 0:
                taker_fee = Decimal(str(fees_data[0].get("takerFeeRate", "0.00025")))
            
            # Build settlement
            settlement = self._build_settlement(
                side=side,
                price=market_price,
                qty=qty_decimal,
                fee_rate=taker_fee,
                nonce=nonce,
                expiration_ms=expiration_ms,
            )
            
            order_data = {
                "id": order_id,
                "market": self.symbol,
                "type": "LIMIT",
                "side": side,
                "qty": str(qty_decimal),
                "price": str(market_price),
                "timeInForce": "IOC",  # Immediate or Cancel for market orders
                "expiryEpochMillis": expiration_ms,
                "fee": str(taker_fee),
                "nonce": str(nonce),
                "postOnly": False,
                "reduceOnly": False,
            }
            
            if settlement:
                order_data["settlement"] = settlement
            
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
            int_ids = []
            ext_ids = []
            for oid in order_ids:
                try:
                    int_ids.append(int(oid))
                except ValueError:
                    ext_ids.append(oid)
            
            result = True
            if int_ids:
                r = await self._post("/user/order/massCancel", data={"orderIds": int_ids})
                result = result and (r is not None)
            if ext_ids:
                r = await self._post("/user/order/massCancel", data={"externalOrderIds": ext_ids})
                result = result and (r is not None)
            return result
        except Exception as e:
            logger.error(f"Extended _cancel_orders_by_ids error: {e}")
            return False

    async def modify_grid_order(
        self,
        order_id: str,
        price: float = None,
        amount: float = None
    ) -> Tuple[bool, str]:
        """Modify an existing order (Extended supports cancelId for replace)."""
        try:
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
            new_amount = amount if amount else float(current_order.get('qty', 0))
            
            return await self.place_single_order(is_ask, new_price, new_amount)
            
        except Exception as e:
            logger.error(f"Extended modify_grid_order error: {e}")
            return False, ""

    async def get_orders(self) -> List[dict]:
        """Get current open orders."""
        try:
            orders = await self._get("/user/orders", params={"market": self.symbol})
            if orders:
                # Log sample for debugging (learned from Nado)
                if len(orders) > 0:
                    logger.debug(f"Extended raw order sample: {orders[0]}")
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
                size = Decimal(str(pos.get("size", "0")))
                
                # Make size negative for short positions
                if side == "SHORT":
                    size = -size
                
                result[market] = {
                    "position": float(size),
                    "size": float(abs(size)),
                    "side": side,
                    "entryPrice": float(pos.get("openPrice", "0")),
                    "markPrice": float(pos.get("markPrice", "0")),
                    "unrealizedPnl": float(pos.get("unrealisedPnl", "0")),
                    "realizedPnl": float(pos.get("realisedPnl", "0")),
                    "leverage": float(pos.get("leverage", "1")),
                    "liquidationPrice": float(pos.get("liquidationPrice", "0")),
                    "margin": float(pos.get("margin", "0")),
                    "sign": 1 if side == "LONG" else -1,
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
        """Subscribe to WebSocket streams with REST fallback (learned from Nado)."""
        self.callbacks = callbacks
        
        # First, fetch initial market price (learned from Nado)
        if 'market_stats' in callbacks:
            logger.info("Fetching initial market price...")
            initial_price = await self._poll_market_stats()
            if initial_price:
                logger.info(f"Initial market price: {initial_price}")
            else:
                logger.warning("Could not fetch initial market price")
        
        # Try to initialize WebSocket
        if not self.ws_initialized:
            await self._initialize_ws()
        
        # Always start REST polling as backup (learned from Nado)
        asyncio.create_task(self._rest_polling_task())

    async def _initialize_ws(self):
        """Initialize WebSocket connections with error handling (learned from Nado)."""
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
            
            logger.info("Extended WebSocket initialized successfully")
            
        except aiohttp.ClientResponseError as e:
            logger.error(f"Extended WebSocket connection failed (HTTP {e.status}): {e.message}")
            logger.warning("WebSocket unavailable - will use REST API polling for updates")
            self.ws_initialized = False
        except aiohttp.WSServerHandshakeError as e:
            logger.error(f"Extended WebSocket handshake failed: {e}")
            self.ws_initialized = False
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
        """Poll REST API for updates (backup for WebSocket, learned from Nado)."""
        poll_count = 0
        while True:
            try:
                # Poll market price every cycle
                await self._poll_market_stats()
                
                # Only poll orders and positions if WebSocket is NOT connected
                # This avoids duplicate callbacks when WebSocket is working
                poll_count += 1
                if poll_count % 2 == 0 and not self.ws_initialized:
                    await self._poll_orders()
                    await self._poll_positions()
                
                await asyncio.sleep(3)
            except Exception as e:
                logger.error(f"Extended REST polling error: {e}")
                await asyncio.sleep(5)

    async def _poll_market_stats(self) -> Optional[float]:
        """Poll market statistics and trigger callback."""
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
                
                logger.debug(f"Market price polled: mark_price={mark_price}")
                
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
        """Poll orders and trigger callback (learned from Nado)."""
        try:
            if 'orders' not in self.callbacks or not self.callbacks['orders']:
                return
            
            orders = await self.get_orders()
            if orders:
                # Normalize to CCXT format
                normalized = normalize_orders_list(orders)
                logger.debug(f"Polled {len(orders)} orders, normalized {len(normalized)}")
                
                callback = self.callbacks['orders']
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(str(self.account_id or ""), normalized))
                else:
                    callback(str(self.account_id or ""), normalized)
        except Exception as e:
            logger.error(f"Extended _poll_orders error: {e}")

    async def _poll_positions(self):
        """Poll positions and trigger callback (learned from Nado)."""
        try:
            if 'positions' not in self.callbacks or not self.callbacks['positions']:
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

"""
Nado Exchange Adapter

Implements the ExchangeInterface for nado.xyz perp DEX.
Uses EIP712 signing with private key authentication.
"""

import asyncio
import contextlib
import json
import logging
import os
import time
import random
import re
from typing import List, Tuple, Dict, Callable, Any, Optional
from decimal import Decimal

import aiohttp
import pandas as pd
from eth_account import Account
from eth_account.messages import encode_typed_data

from .interfaces import ExchangeInterface
from .order_converter import normalize_order_to_ccxt, normalize_orders_list

logger = logging.getLogger(__name__)

# Constants for x18 precision
X18 = 10 ** 18


class NadoAdapter(ExchangeInterface):
    """
    Nado exchange adapter implementing the ExchangeInterface.
    Uses EIP712 signing with private key for authentication.
    """

    TARGET_SYMBOL = os.getenv("NADO_SYMBOL", "AAVEUSDT0")

    # Market ID to Nado product_id mapping
    MARKET_ID_TO_PRODUCT = {
        0: None,  # Default resolved by symbol (AAVEUSDT0)
        1: 2,   # BTC-PERP
        2: 8,   # SOL-PERP
        3: 10,  # XRP-PERP
        4: 14,  # BNB-PERP
    }

    # Product ID to symbol mapping（Nado mainnet：2=BTC 约71k, 4=ETH 约2k, 8=SOL, 26=AAVE）
    PRODUCT_ID_TO_SYMBOL = {
        0: "USDT0",
        1: "KBTC",
        2: "BTC-PERP",
        3: "PRODUCT_3",
        4: "ETH-PERP",
        5: "USDC",
        8: "SOL-PERP",
        10: "XRP-PERP",
        14: "BNB-PERP",
        16: "HYPE-PERP",
        18: "ZEC-PERP",
        20: "MON-PERP",
        22: "FARTCOIN-PERP",
        24: "PRODUCT_24",
        26: "AAVEUSDT0",
    }

    @staticmethod
    def _env_int(name: str, default: int, min_value: int = 0) -> int:
        raw = os.getenv(name, "").strip()
        if not raw:
            return default
        try:
            value = int(raw)
            if value < min_value:
                return default
            return value
        except Exception:
            return default

    @staticmethod
    def _env_bool(name: str, default: bool = False) -> bool:
        raw = (os.getenv(name, "").strip() or "").lower()
        if raw == "":
            return default
        if raw in ("1", "true", "yes", "y", "on"):
            return True
        if raw in ("0", "false", "no", "n", "off"):
            return False
        return default

    def __init__(
        self,
        market_id: int = 0,
        product_id: int = None,  # If None, will use market_id mapping
        subaccount_name: str = "default"
    ):
        self.market_id = market_id
        self.target_symbol = self.TARGET_SYMBOL
        env_product_id = os.getenv("NADO_PRODUCT_ID", "").strip()
        env_product_id_int = int(env_product_id) if env_product_id.isdigit() else None
        self.product_id = (
            product_id
            if product_id is not None
            else env_product_id_int
            if env_product_id_int is not None
            else self.MARKET_ID_TO_PRODUCT.get(market_id)
        )
        self.subaccount_name = subaccount_name

        # Load configuration from environment
        self.private_key = os.getenv('NADO_PRIVATE_KEY', '')
        self.env = os.getenv('NADO_ENV', 'testnet').lower()

        # Set endpoints based on environment
        # Mainnet: Ink chain, Testnet: Ink Sepolia
        if self.env == 'mainnet':
            # Mainnet endpoints (Ink)
            self.gateway_rest = os.getenv('NADO_GATEWAY_REST', 'https://gateway.prod.nado.xyz/v1')
            self.gateway_ws = os.getenv('NADO_GATEWAY_WS', 'wss://gateway.prod.nado.xyz/v1/ws')
            self.subscriptions_ws = os.getenv('NADO_SUBSCRIPTIONS_WS', 'wss://gateway.prod.nado.xyz/v1/subscribe')
            self.archive_url = os.getenv('NADO_ARCHIVE_URL', 'https://archive.prod.nado.xyz/v1')
            self.trigger_url = os.getenv('NADO_TRIGGER_URL', 'https://trigger.prod.nado.xyz/v1')
        else:
            # Testnet endpoints (Ink Sepolia)
            self.gateway_rest = os.getenv('NADO_GATEWAY_REST', 'https://gateway.test.nado.xyz/v1')
            self.gateway_ws = os.getenv('NADO_GATEWAY_WS', 'wss://gateway.test.nado.xyz/v1/ws')
            self.subscriptions_ws = os.getenv('NADO_SUBSCRIPTIONS_WS', 'wss://gateway.test.nado.xyz/v1/subscribe')
            self.archive_url = os.getenv('NADO_ARCHIVE_URL', 'https://archive.test.nado.xyz/v1')
            self.trigger_url = os.getenv('NADO_TRIGGER_URL', 'https://trigger.test.nado.xyz/v1')

        # Network/retry tuning
        self.recv_time_offset_ms = self._env_int("NADO_RECV_TIME_OFFSET_MS", 1200, min_value=1)
        self.contracts_query_retry = self._env_int("NADO_CONTRACTS_QUERY_RETRY", 3, min_value=1)
        self.contracts_query_retry_delay_ms = self._env_int(
            "NADO_CONTRACTS_QUERY_RETRY_DELAY_MS", 400, min_value=0
        )

        # Margin mode: some products are isolated-only (error_code=2122)
        self.isolated_margin = self._env_bool("NADO_ISOLATED", default=False)
        # 可选：isolated 初始保证金（USDC，x6 精度编码到 appendix 的高 64 位）
        self.isolated_margin_usdc = float(os.getenv("NADO_ISOLATED_MARGIN_USDC", "0") or 0)
        # 标记 isolated 保证金是否已划拨（首单成功 or 启动时已有持仓），后续开仓不再重复携带
        self._iso_margin_seeded = False

        # Initialize signing account from private key（签名用密钥，可以是主钱包，也可以是 linked signer / 1CT）
        if self.private_key:
            self.account = Account.from_key(self.private_key)
            self.address = self.account.address
        else:
            self.account = None
            self.address = None

        # Subaccount owner address（子账号实际归属地址，用于 sender 字段）
        # - 若使用主钱包私钥直连：可不配 NADO_OWNER_ADDRESS，默认等于签名地址 self.address
        # - 若使用 linked signer / 1CT：必须在 .env 中设置 NADO_OWNER_ADDRESS=主钱包地址，NADO_PRIVATE_KEY=linked signer 私钥
        owner_address_env = os.getenv("NADO_OWNER_ADDRESS", "").strip()
        if owner_address_env:
            normalized = owner_address_env
            if not normalized.startswith("0x"):
                normalized = "0x" + normalized
            normalized = normalized.lower()
            if len(normalized) != 42:
                raise ValueError("NADO_OWNER_ADDRESS 格式错误，应为 0x 开头的 42 位以太坊地址")
            self.owner_address = normalized
        else:
            # 兼容旧配置：未显式设置 owner 时，默认使用签名地址
            self.owner_address = self.address.lower() if self.address else None

        # Session management
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws_session: Optional[aiohttp.ClientSession] = None
        self.ws_connection: Optional[aiohttp.ClientWebSocketResponse] = None
        self.subscriptions_ws_connection: Optional[aiohttp.ClientWebSocketResponse] = None

        # Callbacks for subscriptions
        self.callbacks: Dict[str, Callable] = {}
        self.ws_initialized = False
        self._closing = False
        self._rest_polling_task_handle: Optional[asyncio.Task] = None
        self._ws_listener_task_handle: Optional[asyncio.Task] = None
        self._ws_ping_task_handle: Optional[asyncio.Task] = None

        # Contract info (will be fetched during initialization)
        self.chain_id: Optional[int] = None
        self.endpoint_address: Optional[str] = None
        self.order_digests: Dict[str, str] = {}  # client_order_id -> digest mapping

        # Product trading parameters (will be fetched during initialization)
        self.price_increment: float = 0.1  # Default: $0.1
        self.size_increment: float = 0.001  # Default: 0.001
        self.min_size: float = 100.0  # Default: $100 notional
        self._price_sanity_warned: bool = False

        logger.info(
            f"Nado Adapter initialized with env={self.env}, "
            f"target_symbol={self.target_symbol}, product_id={self.product_id}"
        )

    @staticmethod
    def _extract_product_symbol(product: Dict[str, Any]) -> str:
        """Extract a normalized symbol from a product payload."""
        for key in ("symbol", "instrument", "name", "product_name", "display_name"):
            value = product.get(key)
            if isinstance(value, str) and value:
                return value
        return ""

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return re.sub(r"[^A-Z0-9]", "", symbol.upper())

    def _resolve_product_id_by_symbol(self, perp_products: List[Dict[str, Any]]) -> Optional[int]:
        """Resolve product_id by symbol with strict matching to avoid wrong market selection."""
        target = self.target_symbol.upper().strip()
        target_norm = self._normalize_symbol(target)
        target_token = re.split(r"[-_/]", target)[0] if target else ""
        exact_matches: List[Tuple[int, str]] = []
        token_perp_matches: List[Tuple[int, str]] = []

        for product in perp_products:
            product_id = product.get("product_id")
            if product_id is None:
                continue
            symbol = self._extract_product_symbol(product)
            if not symbol:
                symbol = self.PRODUCT_ID_TO_SYMBOL.get(product_id, "")
            normalized = self._normalize_symbol(symbol)

            if normalized == target_norm:
                exact_matches.append((product_id, symbol))
            if target_token and target_token in normalized and "PERP" in normalized:
                token_perp_matches.append((product_id, symbol))

        if len(exact_matches) == 1:
            return exact_matches[0][0]
        if len(exact_matches) > 1:
            logger.error(
                "目标 %s 存在多个精确产品匹配: %s",
                self.target_symbol,
                exact_matches,
            )
            return None

        if len(token_perp_matches) == 1:
            logger.warning(
                "%s 无精确符号匹配，回退到 token-perp 匹配: %s",
                self.target_symbol,
                token_perp_matches[0],
            )
            return token_perp_matches[0][0]
        if len(token_perp_matches) > 1:
            logger.error(
                "%s 存在多个 token-perp 匹配: %s，请显式设置 NADO_PRODUCT_ID",
                self.target_symbol,
                token_perp_matches,
            )
            return None

        return None

    def _get_sender_bytes32(self) -> str:
        """
        Generate sender bytes32 (address + subaccount identifier).
        Format: address (20 bytes) + subaccount_name padded to 12 bytes
        """
        # 这里使用子账号 owner 地址，而不是签名地址：
        # - 直连主钱包时：owner_address == 签名地址
        # - 使用 linked signer / 1CT 时：owner_address=主钱包地址，签名地址=linked signer 地址
        if not self.owner_address:
            raise ValueError("No owner address available - 请在环境变量中设置 NADO_OWNER_ADDRESS 或 NADO_PRIVATE_KEY")

        # Convert subaccount name to bytes and pad to 12 bytes
        subaccount_bytes = self.subaccount_name.encode('utf-8')[:12].ljust(12, b'\x00')
        subaccount_hex = subaccount_bytes.hex()

        # Address without 0x prefix + subaccount hex
        return f"0x{self.owner_address[2:]}{subaccount_hex}"

    def _gen_order_nonce(self, recv_time_offset_ms: Optional[int] = None) -> int:
        """
        Generate order nonce.
        Most significant 44 bits: recv_time in milliseconds
        Least significant 20 bits: random integer
        """
        timestamp_ms = int(time.time() * 1000)
        offset = self.recv_time_offset_ms if recv_time_offset_ms is None else recv_time_offset_ms
        recv_time = timestamp_ms + offset
        random_int = random.randint(0, (1 << 20) - 1)
        return (recv_time << 20) + random_int

    def _gen_order_verifying_contract(self, product_id: int) -> str:
        """Generate the order verifying contract address based on product ID."""
        be_bytes = product_id.to_bytes(20, byteorder="big", signed=False)
        return "0x" + be_bytes.hex()

    def _to_x18(self, value: float) -> int:
        """Convert a float to x18 precision integer."""
        return int(Decimal(str(value)) * X18)

    def _from_x18(self, value: int) -> float:
        """Convert x18 precision integer to float."""
        return float(Decimal(str(value)) / X18)

    def _round_price_x18(self, price_x18: int) -> int:
        """Round price_x18 to valid increment (ensures divisibility)."""
        # price_increment_x18 = 0.1 * 10^18 = 100000000000000000
        price_increment_x18 = self._to_x18(self.price_increment)
        if price_increment_x18 > 0:
            return (price_x18 // price_increment_x18) * price_increment_x18
        return price_x18

    def _round_size_x18(self, size_x18: int) -> int:
        """Round size_x18 to valid increment."""
        size_increment_x18 = self._to_x18(self.size_increment)
        if size_increment_x18 > 0:
            return (size_x18 // size_increment_x18) * size_increment_x18
        return size_x18

    def _build_appendix(
        self,
        order_type: int = 3,  # POST_ONLY by default
        isolated: bool = False,
        reduce_only: bool = False,
        version: int = 1,
        isolated_margin_x6: int = 0,
    ) -> int:
        """
        Build order appendix.
        Bit layout:
        - Version (8 bits, 0–7): protocol version (currently 1)
        - Isolated (1 bit, 8): whether isolated margin
        - Order Type (2 bits, 9–10): 0=DEFAULT, 1=IOC, 2=FOK, 3=POST_ONLY
        - Reduce Only (1 bit, 11): only decreases existing position
        - Value (64 bits, 64–127): isolated_margin_x6 / TWAP params 等扩展值
        """
        appendix = version  # bits 0-7
        if isolated:
            appendix |= (1 << 8)  # bit 8
        appendix |= (order_type << 9)  # bits 9-10
        if reduce_only:
            appendix |= (1 << 11)  # bit 11
        if isolated and isolated_margin_x6 > 0:
            # 高 64 位承载 value，isolated_margin 采用 x6 精度（USDC）
            appendix |= (int(isolated_margin_x6) << 64)
        return appendix

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=12, connect=5, sock_read=10),
                trust_env=True,
                headers={
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'User-Agent': 'perpdex-nado-aave/1.0 (+aiohttp)'
                }
            )
        return self.session

    async def _fetch_contracts(self) -> Dict:
        """Fetch contract information including chain_id and endpoint address."""
        retriable_status = {429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 525, 526}
        url = f"{self.gateway_rest}/query?type=contracts"
        for attempt in range(1, self.contracts_query_retry + 1):
            try:
                session = await self._get_session()
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json(content_type=None)
                        if data.get('status') == 'success':
                            return data.get('data', {})
                        logger.warning(
                            "Contracts query failed: %s (attempt %s/%s)",
                            data.get('error', 'Unknown error'),
                            attempt,
                            self.contracts_query_retry,
                        )
                    else:
                        logger.warning(
                            "Contracts query HTTP error: %s (attempt %s/%s)",
                            response.status,
                            attempt,
                            self.contracts_query_retry,
                        )
                        if response.status not in retriable_status:
                            break
            except Exception as e:
                logger.error(
                    "Failed to fetch contracts (attempt %s/%s): %s",
                    attempt,
                    self.contracts_query_retry,
                    e,
                )

            if attempt < self.contracts_query_retry:
                await asyncio.sleep((self.contracts_query_retry_delay_ms / 1000.0) * attempt)
        return {}

    def _sign_typed_data(
        self,
        primary_type: str,
        types: Dict,
        message: Dict,
        verifying_contract: str
    ) -> str:
        """Sign EIP712 typed data."""
        if not self.account:
            raise ValueError("No account available - private key not set")

        domain = {
            "name": "Nado",
            "version": "0.0.1",
            "chainId": self.chain_id,
            "verifyingContract": verifying_contract
        }

        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"}
                ],
                **types
            },
            "primaryType": primary_type,
            "domain": domain,
            "message": message
        }

        signable = encode_typed_data(full_message=typed_data)
        signed = self.account.sign_message(signable)
        return signed.signature.hex()

    async def _rest_query(self, query_type: str, params: Dict = None) -> Dict:
        """Execute REST query."""
        if self._closing:
            return {}

        query_params = {"type": query_type}
        if params:
            query_params.update(params)

        url = f"{self.gateway_rest}/query"
        logger.debug(f"REST 查询: {url} params={query_params}")

        retriable_status = {429, 500, 502, 503, 504, 520, 521, 522, 523, 524, 525, 526}
        last_error: Optional[str] = None

        for attempt in range(1, 4):
            try:
                session = await self._get_session()
                async with session.get(url, params=query_params) as response:
                    if response.status != 200:
                        text = await response.text()
                        msg = f"REST query HTTP error {response.status}: {text[:200]}"
                        if response.status in retriable_status and attempt < 3:
                            logger.warning("%s (第 %s/3 次尝试，将重试)", msg, attempt)
                            await asyncio.sleep(0.4 * attempt)
                            continue
                        logger.error(msg)
                        return {}

                    data = await response.json(content_type=None)
                    if data.get('status') == 'success':
                        return data.get('data', {})
                    logger.error(
                        "查询 %s 失败: %s (code: %s)",
                        query_type,
                        data.get('error', '未知错误'),
                        data.get('error_code', 'N/A'),
                    )
                    return {}
            except asyncio.CancelledError:
                raise
            except Exception as e:
                last_error = str(e)
                if attempt < 3 and not self._closing:
                    logger.warning(
                        "REST 查询临时错误 (%s) 第 %s/3 次: %s",
                        query_type,
                        attempt,
                        e,
                    )
                    await asyncio.sleep(0.4 * attempt)
                    continue
                break

        if last_error and not self._closing:
            logger.error(f"REST 查询错误 ({query_type}): {last_error}")
        return {}

    async def _rest_execute(self, payload: Dict) -> Dict:
        """Execute REST execute command."""
        if self._closing:
            return {"status": "failure", "error": "adapter closing"}

        url = f"{self.gateway_rest}/execute"
        last_error: Optional[str] = None
        for attempt in range(1, 4):
            try:
                session = await self._get_session()
                async with session.post(url, json=payload) as response:
                    if response.status != 200:
                        text = await response.text()
                        if response.status >= 500 and attempt < 3:
                            logger.warning(
                                "REST 执行 HTTP 错误 %s (第 %s/3 次): %s",
                                response.status,
                                attempt,
                                text[:200],
                            )
                            await asyncio.sleep(0.4 * attempt)
                            continue
                        return {"status": "failure", "error": f"http {response.status}: {text[:200]}"}
                    data = await response.json(content_type=None)
                    return data
            except asyncio.CancelledError:
                raise
            except Exception as e:
                last_error = str(e)
                if attempt < 3 and not self._closing:
                    logger.warning("REST 执行临时错误 第 %s/3 次: %s", attempt, e)
                    await asyncio.sleep(0.4 * attempt)
                    continue
                break

        logger.error(f"REST 执行错误: {last_error}")
        return {"status": "failure", "error": str(last_error)}

    async def place_single_order(self, is_ask: bool, price: float, amount: float, reduce_only: bool = False) -> Tuple[bool, str, str, object]:
        """
        Place single limit order.
        
        Args:
            is_ask: Whether this is a sell order
            price: Order price
            amount: Order amount
            reduce_only: If True, order can only reduce position (cannot open new position)
        
        Returns: (success, order_id/client_order_id)
        """
        try:
            sender = self._get_sender_bytes32()
            nonce = self._gen_order_nonce()
            expiration = int(time.time()) + 86400 * 30  # 30 days expiration

            # Convert to x18 precision first
            price_x18 = self._to_x18(price)
            amount_x18 = self._to_x18(amount)
            
            # Round to valid increments at x18 level (ensures exact divisibility)
            price_x18 = self._round_price_x18(price_x18)
            amount_x18 = self._round_size_x18(amount_x18)
            
            # Amount: positive for buy, negative for sell
            if is_ask:
                amount_x18 = -amount_x18
            
            logger.debug(f"下单: is_ask={is_ask}, price_x18={price_x18}, amount_x18={amount_x18}, reduce_only={reduce_only}")

            # 只在首次建仓时携带 isolated_margin；仓位已建立后不再重复划拨
            isolated_margin_x6 = 0
            if (self.isolated_margin and not reduce_only
                    and self.isolated_margin_usdc > 0
                    and not self._iso_margin_seeded):
                isolated_margin_x6 = int(self.isolated_margin_usdc * 1_000_000)
                logger.info("首次 isolated 开仓，appendix 携带 margin=%s USDC", self.isolated_margin_usdc)

            appendix = self._build_appendix(
                order_type=3,
                isolated=self.isolated_margin,
                reduce_only=reduce_only,
                isolated_margin_x6=isolated_margin_x6,
            )

            # Order message for signing
            order_message = {
                "sender": sender,
                "priceX18": str(price_x18),
                "amount": str(amount_x18),
                "expiration": str(expiration),
                "nonce": str(nonce),
                "appendix": str(appendix)
            }

            # EIP712 types for Order
            order_types = {
                "Order": [
                    {"name": "sender", "type": "bytes32"},
                    {"name": "priceX18", "type": "int128"},
                    {"name": "amount", "type": "int128"},
                    {"name": "expiration", "type": "uint64"},
                    {"name": "nonce", "type": "uint64"},
                    {"name": "appendix", "type": "uint128"}
                ]
            }

            # Sign the order
            verifying_contract = self._gen_order_verifying_contract(self.product_id)
            signature = self._sign_typed_data(
                "Order",
                order_types,
                {
                    "sender": bytes.fromhex(sender[2:]),
                    "priceX18": price_x18,
                    "amount": amount_x18,
                    "expiration": expiration,
                    "nonce": nonce,
                    "appendix": appendix
                },
                verifying_contract
            )

            # Build request payload
            payload = {
                "place_order": {
                    "product_id": self.product_id,
                    "order": order_message,
                    "signature": f"0x{signature}" if not signature.startswith('0x') else signature
                }
            }

            # Execute order
            response = await self._rest_execute(payload)

            if response.get('status') == 'success':
                digest = response.get('data', {}).get('digest', '')
                client_order_id = str(nonce & ((1 << 20) - 1))
                self.order_digests[client_order_id] = digest
                if isolated_margin_x6 > 0 and not self._iso_margin_seeded:
                    self._iso_margin_seeded = True
                    logger.info("isolated margin 已划拨（首单成功），后续开仓不再携带 margin")
                logger.info(f"订单下单成功: digest={digest}")
                return True, client_order_id, "", None
            else:
                err = response.get('error', '未知错误')
                err_code = response.get('error_code')
                logger.error(f"下单失败: {err}, error_code={err_code}")
                return False, '', str(err), err_code
        except Exception as e:
            logger.error(f"place_single_order 错误: {e}", exc_info=True)
            return False, '', str(e), None

    async def place_multi_orders(self, orders: List[Tuple[bool, float, float]]) -> Tuple[bool, List[str]]:
        """
        Place multiple limit orders.
        orders: [(is_ask, price, amount), ...]
        Returns: (success, order_ids)
        """
        if not orders:
            logger.warning("place_multi_orders: 未提供订单")
            return True, []

        try:
            order_ids = []
            for i, (is_ask, price, amount) in enumerate(orders):
                logger.debug(f"place_multi_orders: 下单 {i+1}/{len(orders)}: is_ask={is_ask}, price={price}, amount={amount}")
                success, order_id, _, _ = await self.place_single_order(is_ask, price, amount)
                if not success:
                    logger.error(f"place_multi_orders: 第 {i+1} 笔下单失败")
                    # Cancel previously placed orders
                    if order_ids:
                        logger.info(f"place_multi_orders: 取消之前已下的 {len(order_ids)} 笔订单")
                        await self.cancel_grid_orders(order_ids)
                    return False, []
                order_ids.append(order_id)

            logger.debug(f"place_multi_orders: 成功下单 {len(order_ids)} 笔")
            return True, order_ids
        except Exception as e:
            logger.error(f"place_multi_orders 错误: {e}")
            return False, []

    async def place_single_market_order(self, is_ask: bool, price: float, amount: float) -> Tuple[bool, str]:
        """
        Place single market order (using IOC order type).
        """
        try:
            sender = self._get_sender_bytes32()
            nonce = self._gen_order_nonce()
            expiration = int(time.time()) + 60  # 1 minute expiration for market order

            # For market orders, use a very high/low price depending on side
            market_price = price * 1.1 if not is_ask else price * 0.9
            
            # Convert to x18 and round
            price_x18 = self._round_price_x18(self._to_x18(market_price))
            amount_x18 = self._round_size_x18(self._to_x18(amount))
            
            if is_ask:
                amount_x18 = -amount_x18

            # Build appendix with IOC order type + isolated margin
            appendix = self._build_appendix(order_type=1, isolated=self.isolated_margin)  # IOC

            order_message = {
                "sender": sender,
                "priceX18": str(price_x18),
                "amount": str(amount_x18),
                "expiration": str(expiration),
                "nonce": str(nonce),
                "appendix": str(appendix)
            }

            order_types = {
                "Order": [
                    {"name": "sender", "type": "bytes32"},
                    {"name": "priceX18", "type": "int128"},
                    {"name": "amount", "type": "int128"},
                    {"name": "expiration", "type": "uint64"},
                    {"name": "nonce", "type": "uint64"},
                    {"name": "appendix", "type": "uint128"}
                ]
            }

            verifying_contract = self._gen_order_verifying_contract(self.product_id)
            signature = self._sign_typed_data(
                "Order",
                order_types,
                {
                    "sender": bytes.fromhex(sender[2:]),
                    "priceX18": price_x18,
                    "amount": amount_x18,
                    "expiration": expiration,
                    "nonce": nonce,
                    "appendix": appendix
                },
                verifying_contract
            )

            payload = {
                "place_order": {
                    "product_id": self.product_id,
                    "order": order_message,
                    "signature": f"0x{signature}" if not signature.startswith('0x') else signature
                }
            }

            response = await self._rest_execute(payload)

            if response.get('status') == 'success':
                digest = response.get('data', {}).get('digest', '')
                client_order_id = str(nonce & ((1 << 20) - 1))
                self.order_digests[client_order_id] = digest
                return True, client_order_id
            else:
                logger.error(f"市价单下单失败: {response.get('error', '未知错误')}")
                return False, ''
        except Exception as e:
            logger.error(f"place_single_market_order 错误: {e}", exc_info=True)
            return False, ''

    async def cancel_grid_orders(self, order_ids: List[str]) -> bool:
        """
        Batch cancel orders.
        """
        if not order_ids:
            logger.warning("cancel_grid_orders: 未提供订单ID")
            return True

        try:
            sender = self._get_sender_bytes32()
            nonce = self._gen_order_nonce()

            # Resolve digests from in-memory mapping
            resolved_pairs: List[Tuple[str, str]] = []
            unresolved_order_ids: List[str] = []
            for order_id in order_ids:
                digest = self.order_digests.get(order_id)
                if digest:
                    resolved_pairs.append((order_id, digest))
                else:
                    unresolved_order_ids.append(order_id)

            # Self-heal once: rebuild mapping from current open orders, then retry unresolved ids
            if unresolved_order_ids:
                await self._rebuild_order_digest_mapping()
                still_unresolved: List[str] = []
                for order_id in unresolved_order_ids:
                    digest = self.order_digests.get(order_id)
                    if digest:
                        resolved_pairs.append((order_id, digest))
                    else:
                        still_unresolved.append(order_id)
                unresolved_order_ids = still_unresolved

            # Allow direct digest cancellation if caller passed digest-format id
            for order_id in list(unresolved_order_ids):
                if isinstance(order_id, str) and order_id.startswith("0x") and len(order_id) == 66:
                    resolved_pairs.append((order_id, order_id))
                    unresolved_order_ids.remove(order_id)

            if unresolved_order_ids:
                logger.warning(
                    "cancel_grid_orders: unresolved order_ids without digest: %s",
                    unresolved_order_ids,
                )

            # De-duplicate digests
            seen_digests = set()
            resolved_unique_pairs: List[Tuple[str, str]] = []
            for oid, dg in resolved_pairs:
                if dg in seen_digests:
                    continue
                seen_digests.add(dg)
                resolved_unique_pairs.append((oid, dg))
            digests = [dg for _, dg in resolved_unique_pairs]

            if not digests:
                # Do not fallback to product-wide cancellation when digest mapping is missing.
                # A broad cancel can wipe unrelated grid orders and desync local state.
                logger.error(
                    "cancel_grid_orders: no digests resolved for requested order_ids, "
                    "skip cancellation to avoid product-wide cancel"
                )
                return False

            # Cancellation message
            cancel_message = {
                "sender": sender,
                "productIds": [self.product_id] * len(digests),
                "digests": digests,
                "nonce": str(nonce)
            }

            # EIP712 types for Cancellation
            cancel_types = {
                "Cancellation": [
                    {"name": "sender", "type": "bytes32"},
                    {"name": "productIds", "type": "uint32[]"},
                    {"name": "digests", "type": "bytes32[]"},
                    {"name": "nonce", "type": "uint64"}
                ]
            }

            # Sign the cancellation
            signature = self._sign_typed_data(
                "Cancellation",
                cancel_types,
                {
                    "sender": bytes.fromhex(sender[2:]),
                    "productIds": [self.product_id] * len(digests),
                    "digests": [bytes.fromhex(d[2:]) if d.startswith('0x') else bytes.fromhex(d) for d in digests],
                    "nonce": nonce
                },
                self.endpoint_address
            )

            payload = {
                "cancel_orders": {
                    "tx": cancel_message,
                    "signature": f"0x{signature}" if not signature.startswith('0x') else signature
                }
            }

            response = await self._rest_execute(payload)

            if response.get('status') == 'success':
                # Remove cancelled orders from tracking (only resolved ones)
                for order_id, _ in resolved_unique_pairs:
                    self.order_digests.pop(order_id, None)
                logger.info(
                    "成功取消 %s 笔订单 (请求=%s, 未解析=%s)",
                    len(digests),
                    len(order_ids),
                    len(unresolved_order_ids),
                )
                return True
            else:
                logger.error(f"取消订单失败: {response.get('error', '未知错误')}")
                return False
        except Exception as e:
            logger.error(f"cancel_grid_orders 错误: {e}", exc_info=True)
            return False

    async def _cancel_product_orders(self) -> bool:
        """Cancel all orders for the current product."""
        try:
            sender = self._get_sender_bytes32()
            nonce = self._gen_order_nonce()

            cancel_message = {
                "sender": sender,
                "productIds": [self.product_id],
                "nonce": str(nonce)
            }

            cancel_types = {
                "CancellationProducts": [
                    {"name": "sender", "type": "bytes32"},
                    {"name": "productIds", "type": "uint32[]"},
                    {"name": "nonce", "type": "uint64"}
                ]
            }

            signature = self._sign_typed_data(
                "CancellationProducts",
                cancel_types,
                {
                    "sender": bytes.fromhex(sender[2:]),
                    "productIds": [self.product_id],
                    "nonce": nonce
                },
                self.endpoint_address
            )

            payload = {
                "cancel_product_orders": {
                    "tx": cancel_message,
                    "signature": f"0x{signature}" if not signature.startswith('0x') else signature
                }
            }

            response = await self._rest_execute(payload)
            return response.get('status') == 'success'
        except Exception as e:
            logger.error(f"_cancel_product_orders 错误: {e}")
            return False

    async def modify_grid_order(self, order_id: str, new_price: float, new_amount: float) -> bool:
        """
        Modify order - Nado does not support order modification directly.
        Need to cancel and create new order.
        """
        logger.warning("Nado 不支持修改订单，请先取消再创建新订单")
        return False

    async def get_orders(self) -> List[dict]:
        """Get current orders."""
        try:
            sender = self._get_sender_bytes32()
            params = {
                "sender": sender,
                "product_id": self.product_id
            }
            data = await self._rest_query("subaccount_orders", params)
            orders = data.get('orders', [])
            
            # Log raw order format for debugging (only first order)
            if orders and len(orders) > 0:
                logger.debug(f"Nado 原始订单示例: {orders[0]}")
            
            return orders
        except Exception as e:
            logger.error(f"get_orders 错误: {e}", exc_info=True)
            return []

    async def get_linked_signer(self) -> Optional[str]:
        """
        查询当前子账号绑定的 linked signer 地址。
        用于验证配置：若返回的地址与你填在 NADO_PRIVATE_KEY 对应的地址一致，则可用该私钥下单。
        文档: https://docs.nado.xyz/developer-resources/api/gateway/queries/linked-signer
        """
        try:
            sender = self._get_sender_bytes32()
            data = await self._rest_query("linked_signer", {"subaccount": sender})
            signer = data.get("linked_signer") or data.get("signer")
            return signer
        except Exception as e:
            logger.error("查询 linked signer 失败: %s", e, exc_info=True)
            return None

    @staticmethod
    def _client_order_id_from_nonce(nonce: Any) -> str:
        """Derive client_order_id (low 20 bits) from nonce."""
        try:
            return str(int(nonce) & ((1 << 20) - 1))
        except Exception:
            return ""

    async def _rebuild_order_digest_mapping(self) -> int:
        """Rebuild in-memory client_order_id -> digest mapping from current open orders."""
        try:
            orders = await self.get_orders()
            if not isinstance(orders, list):
                return 0

            restored = 0
            for order in orders:
                digest = str(order.get("digest", "") or "").strip()
                client_order_id = self._client_order_id_from_nonce(order.get("nonce"))
                if digest and client_order_id and client_order_id not in self.order_digests:
                    self.order_digests[client_order_id] = digest
                    restored += 1

            if restored > 0:
                logger.info("从活跃订单重建 %s 个 order digest 映射", restored)
            return restored
        except Exception as e:
            logger.error(f"_rebuild_order_digest_mapping 错误: {e}", exc_info=True)
            return 0

    async def get_trades(self, limit: int = 1) -> List[dict]:
        """Get recent trades."""
        # Note: Nado uses indexer API for historical trades
        # For recent fills, we can use the fills stream or query
        try:
            # This would require the archive/indexer API
            # For now, return empty list as trades are handled via subscriptions
            return []
        except Exception as e:
            logger.error(f"get_trades 错误: {e}")
            return []

    async def get_positions(self) -> Dict[str, dict]:
        """Get positions."""
        try:
            sender = self._get_sender_bytes32()
            params = {"subaccount": sender}
            data = await self._rest_query("subaccount_info", params)

            positions = {}
            perp_balances = data.get('perp_balances', [])
            perp_products = data.get('perp_products', [])

            for balance in perp_balances:
                product_id = balance.get('product_id')
                amount = self._from_x18(int(balance.get('balance', {}).get('amount', '0')))
                v_quote = self._from_x18(int(balance.get('balance', {}).get('v_quote_balance', '0')))

                # Find product info
                product_info = next((p for p in perp_products if p.get('product_id') == product_id), {})
                oracle_price = self._from_x18(int(product_info.get('oracle_price_x18', '0')))

                if amount != 0:
                    symbol = self._extract_product_symbol(product_info) or self.PRODUCT_ID_TO_SYMBOL.get(product_id, f"PRODUCT_{product_id}")
                    positions[symbol] = {
                        'instrument': symbol,
                        'product_id': product_id,
                        'size': amount,
                        'sign': 1 if amount > 0 else (-1 if amount < 0 else 0),
                        'notional': abs(amount * oracle_price),
                        'entry_price': -v_quote / amount if amount != 0 else 0,
                        'mark_price': oracle_price,
                        'unrealized_pnl': amount * oracle_price + v_quote if amount != 0 else 0
                    }

            return positions
        except Exception as e:
            logger.error(f"get_positions 错误: {e}", exc_info=True)
            return {}

    async def get_account(self) -> dict:
        """Get account info including collateral."""
        try:
            sender = self._get_sender_bytes32()
            params = {"subaccount": sender}
            data = await self._rest_query("subaccount_info", params)

            healths = data.get('healths', [{}])
            unweighted_health = healths[2] if len(healths) > 2 else {}

            total_equity = self._from_x18(int(unweighted_health.get('assets', '0')))
            total_liability = self._from_x18(int(unweighted_health.get('liabilities', '0')))

            return {
                'total_equity': total_equity,
                'collateral': total_equity - total_liability,
                'free_collateral': self._from_x18(int(healths[0].get('health', '0'))) if healths else 0
            }
        except Exception as e:
            logger.error(f"get_account 错误: {e}", exc_info=True)
            return {}

    async def candle_stick(self, market_id: int, resolution: str, count_back: int = 200) -> pd.DataFrame:
        """Get candlestick data."""
        try:
            # Nado uses indexer API for candlesticks
            # Map resolution to granularity in seconds
            resolution_map = {
                '1m': 60,
                '5m': 300,
                '15m': 900,
                '1h': 3600,
                '4h': 14400,
                '1d': 86400
            }
            granularity = resolution_map.get(resolution, 60)

            product_id = self.MARKET_ID_TO_PRODUCT.get(market_id, self.product_id)
            if product_id is None:
                product_id = self.product_id

            # Use archive API for candlesticks
            # Note: This would require the archive endpoint to be configured
            # For now, return empty DataFrame
            logger.warning("K线数据需要 archive/indexer API")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"candle_stick 错误: {e}")
            return pd.DataFrame()

    async def modify_order(self, order_id: int, new_price: float, new_amount: float) -> bool:
        """Modify order - NOT SUPPORTED by Nado API."""
        logger.warning("Nado 不支持修改订单")
        return False

    async def get_orders_by_rest(self) -> List[dict]:
        """Get orders via REST API."""
        return await self.get_orders()

    async def get_trades_by_rest(self, ask_filter: int, limit: int) -> List[dict]:
        """Get trades via REST API."""
        return await self.get_trades(limit)

    async def subscribe(self, callbacks: Dict[str, Callable[[str, Any], None]], proxy: str = None) -> None:
        """
        Subscribe to events.
        callbacks: {
            'market_stats': func(market_id, stats),
            'orders': func(account_id, orders),
            'positions': func(account_id, positions)
        }
        """
        self.callbacks = callbacks
        self.proxy = proxy

        if proxy:
            logger.warning(f"Nado 适配器: 代理支持 ({proxy}) 可能未完全实现")

        # First, poll market price immediately to get initial price
        if 'market_stats' in callbacks:
            logger.info("获取初始市场价格...")
            initial_price = await self._poll_market_price()
            if initial_price:
                logger.info(f"初始市场价格: {initial_price}")
            else:
                logger.warning("无法获取初始市场价格")

        # Try to initialize WebSocket connection
        if not self.ws_initialized:
            await self._initialize_ws()

        # If WebSocket is available, subscribe to streams
        if self.ws_initialized and self.subscriptions_ws_connection:
            if 'market_stats' in callbacks:
                await self._subscribe_market_stats()

            if 'orders' in callbacks:
                await self._subscribe_orders()
                # Also subscribe to fills to detect order executions
                await self._subscribe_fills()

            if 'positions' in callbacks:
                await self._subscribe_positions()

            logger.info(f"Nado 订阅: 已注册 WebSocket 回调 {list(callbacks.keys())}")
        else:
            # WebSocket not available, start REST polling task
            logger.info("WebSocket 不可用，启动 REST API 轮询获取行情")
        
        # Always start REST polling as a backup (even if WebSocket works)
        # This ensures we always have price updates
        if self._rest_polling_task_handle is None or self._rest_polling_task_handle.done():
            self._rest_polling_task_handle = asyncio.create_task(self._rest_polling_task())

    async def _rest_polling_task(self):
        """Poll REST API for updates (backup for WebSocket)."""
        poll_count = 0
        while not self._closing:
            try:
                # Poll market price every cycle
                await self._poll_market_price()
                
                # ALWAYS poll orders to detect fills
                # WebSocket fill events depend on order_digests which is lost on restart
                # REST polling ensures we detect order changes even after restart
                poll_count += 1
                if poll_count % 2 == 0:
                    await self._poll_orders()
                    await self._check_for_fills()
                    await self._poll_positions()
                
                await asyncio.sleep(5)  # 5秒 x 2次 = 10秒检查一次订单，和策略报告同步
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._closing:
                    break
                logger.error(f"REST 轮询错误: {e}")
                await asyncio.sleep(5)

    async def _poll_orders(self):
        """Poll orders and trigger callback."""
        try:
            if 'orders' not in self.callbacks or not self.callbacks['orders']:
                return
            
            orders = await self.get_orders()
            if orders:
                # Normalize orders to CCXT format
                normalized_orders = normalize_orders_list(orders)
                logger.debug(f"轮询到 {len(orders)} 笔订单，已标准化 {len(normalized_orders)} 笔")
                
                callback = self.callbacks['orders']
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(self.address, normalized_orders))
                else:
                    callback(self.address, normalized_orders)
        except Exception as e:
            logger.error(f"轮询订单错误: {e}")

    async def _poll_positions(self):
        """Poll positions and trigger callback."""
        try:
            if 'positions' not in self.callbacks or not self.callbacks['positions']:
                return
            
            positions = await self.get_positions()
            if positions:
                callback = self.callbacks['positions']
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(self.address, positions))
                else:
                    callback(self.address, positions)
        except Exception as e:
            logger.error(f"轮询仓位错误: {e}")

    async def _check_for_fills(self):
        """Check for order fills by comparing filled amounts.
        
        NOTE: We no longer use "order disappeared" detection because it
        cannot distinguish between cancelled orders and filled orders.
        Instead, we rely on WebSocket fill events (_handle_fill) for fill detection.
        
        This method now only updates the order tracking dict for consistency.
        """
        try:
            if 'orders' not in self.callbacks or not self.callbacks['orders']:
                return
            
            # Initialize tracking dict if not exists
            if not hasattr(self, '_last_known_orders'):
                self._last_known_orders = {}
            
            # Get current orders
            current_orders = await self.get_orders()
            if current_orders is None:
                return
            
            # Build current orders dict: digest -> order_info
            current_order_digests = {}
            for order in current_orders:
                digest = order.get('digest', '')
                if digest:
                    current_order_digests[digest] = order
            
            # Update tracking (for WebSocket fill handler to use if needed)
            self._last_known_orders = current_order_digests
            
            # NOTE: We don't trigger callbacks for "disappeared" orders here
            # because we can't distinguish between cancelled and filled orders.
            # Fill detection is handled by WebSocket _handle_fill() method.
                    
        except Exception as e:
            logger.error(f"_check_for_fills 错误: {e}", exc_info=True)

    async def _poll_market_price(self):
        """Poll market price once and trigger callback."""
        try:
            # Use market_price (singular) query
            prices = await self._rest_query("market_price", {"product_id": self.product_id})
            if prices:
                # Nado returns bid_x18 and ask_x18
                bid_x18 = prices.get('bid_x18', '0')
                ask_x18 = prices.get('ask_x18', '0')
                
                bid = self._from_x18(int(bid_x18)) if bid_x18 else 0
                ask = self._from_x18(int(ask_x18)) if ask_x18 else 0
                
                # Calculate mark price as mid price
                mark_price = (bid + ask) / 2 if bid and ask else bid or ask
                
                if mark_price > 0:
                    if (
                        not self._price_sanity_warned
                        and self.target_symbol.upper().startswith("AAVE")
                        and mark_price < 10
                    ):
                        self._price_sanity_warned = True
                        logger.warning(
                            "Market price sanity warning: symbol=%s product_id=%s mark_price=%s bid_x18=%s ask_x18=%s. "
                            "This may indicate wrong product_id; consider setting NADO_PRODUCT_ID explicitly.",
                            self.PRODUCT_ID_TO_SYMBOL.get(self.product_id, self.target_symbol),
                            self.product_id,
                            mark_price,
                            bid_x18,
                            ask_x18,
                        )
                    stats = {
                        'mark_price': mark_price,
                        'best_bid': bid,
                        'best_ask': ask
                    }
                    logger.debug(f"行情轮询: mark_price={mark_price}, bid={bid}, ask={ask}")
                    
                    if 'market_stats' in self.callbacks and self.callbacks['market_stats']:
                        callback = self.callbacks['market_stats']
                        if asyncio.iscoroutinefunction(callback):
                            asyncio.create_task(callback(str(self.market_id), stats))
                        else:
                            callback(str(self.market_id), stats)
                    return mark_price
            return None
        except Exception as e:
            logger.error(f"轮询行情错误: {e}")
            return None

    async def _initialize_ws(self):
        """Initialize WebSocket connection."""
        try:
            if self.ws_session is None or self.ws_session.closed:
                self.ws_session = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=20, connect=8, sock_read=15),
                    trust_env=True,
                    headers={
                        "User-Agent": "perpdex-nado-aave/1.0 (+aiohttp)",
                        "Accept-Encoding": "gzip, deflate, br",
                    },
                )

            # Connect to subscriptions WebSocket
            logger.info(f"连接 WebSocket: {self.subscriptions_ws}")
            self.subscriptions_ws_connection = await self.ws_session.ws_connect(
                self.subscriptions_ws,
                compress=15  # Enable permessage-deflate
            )

            self.ws_initialized = True
            logger.info("Nado WebSocket 初始化成功")

            # Start listening for messages
            self._ws_listener_task_handle = asyncio.create_task(self._ws_listener())

            # Start ping task to keep connection alive
            self._ws_ping_task_handle = asyncio.create_task(self._ws_ping_task())

        except aiohttp.ClientResponseError as e:
            logger.error(f"WebSocket 连接失败 (HTTP {e.status}): {e.message}")
            logger.warning("WebSocket 不可用，将使用 REST API 轮询更新")
            self.ws_initialized = False
        except aiohttp.WSServerHandshakeError as e:
            logger.error(f"WebSocket 握手失败: {e}")
            logger.warning("WebSocket 不可用，将使用 REST API 轮询更新")
            self.ws_initialized = False
        except Exception as e:
            logger.error(f"WebSocket 初始化失败: {e}")
            logger.warning("WebSocket 不可用，将使用 REST API 轮询更新")
            self.ws_initialized = False

    async def _ws_ping_task(self):
        """Send ping frames every 30 seconds to keep connection alive."""
        while (not self._closing) and self.ws_initialized and self.subscriptions_ws_connection:
            try:
                await asyncio.sleep(25)
                if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                    await self.subscriptions_ws_connection.ping()
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._closing:
                    break
                logger.error(f"WebSocket ping 错误: {e}")
                break

    async def _ws_listener(self):
        """Listen for WebSocket messages."""
        while (not self._closing) and self.ws_initialized and self.subscriptions_ws_connection:
            try:
                msg = await self.subscriptions_ws_connection.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    await self._handle_ws_message(data)
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    logger.warning("WebSocket 连接已关闭")
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket 错误: {msg.data}")
                    break
            except Exception as e:
                if self._closing:
                    break
                logger.error(f"WebSocket 监听器错误: {e}")
                break

    async def _handle_ws_message(self, data: dict):
        """Handle incoming WebSocket messages."""
        event_type = data.get('type')

        if event_type == 'best_bid_offer':
            await self._handle_market_stats(data)
        elif event_type == 'order_update':
            await self._handle_order_update(data)
        elif event_type == 'position_change':
            await self._handle_position_change(data)
        elif event_type == 'fill':
            await self._handle_fill(data)

    async def _handle_market_stats(self, data: dict):
        """Handle market stats updates from WebSocket."""
        if 'market_stats' in self.callbacks and self.callbacks['market_stats']:
            # WebSocket best_bid_offer stream returns bid_x18 and ask_x18
            bid_x18 = data.get('bid_x18', data.get('bid_price', data.get('best_bid', '0')))
            ask_x18 = data.get('ask_x18', data.get('ask_price', data.get('best_ask', '0')))
            
            bid = self._from_x18(int(bid_x18)) if bid_x18 else 0
            ask = self._from_x18(int(ask_x18)) if ask_x18 else 0
            mark_price = (bid + ask) / 2 if bid and ask else bid or ask
            
            stats = {
                'mark_price': mark_price,
                'best_bid': bid,
                'best_ask': ask
            }
            
            logger.debug(f"WebSocket 行情: {stats}")
            
            callback = self.callbacks['market_stats']
            if asyncio.iscoroutinefunction(callback):
                asyncio.create_task(callback(str(self.market_id), stats))
            else:
                callback(str(self.market_id), stats)

    async def _handle_order_update(self, data: dict):
        """Handle order updates."""
        if 'orders' in self.callbacks and self.callbacks['orders']:
            orders = [data.get('order', data)]
            normalized_orders = normalize_orders_list(orders)
            callback = self.callbacks['orders']
            if asyncio.iscoroutinefunction(callback):
                asyncio.create_task(callback(self.address, normalized_orders))
            else:
                callback(self.address, normalized_orders)

    async def _handle_position_change(self, data: dict):
        """Handle position changes."""
        if 'positions' in self.callbacks and self.callbacks['positions']:
            positions = await self.get_positions()
            callback = self.callbacks['positions']
            if asyncio.iscoroutinefunction(callback):
                asyncio.create_task(callback(self.address, positions))
            else:
                callback(self.address, positions)

    async def _handle_fill(self, data: dict):
        """Handle fill events (order filled)."""
        logger.info(f"收到成交事件: {data}")
        
        if 'orders' in self.callbacks and self.callbacks['orders']:
            # Extract fill information from the actual fill event format
            # Fill event fields: order_digest, price, filled_qty, remaining_qty, original_qty, is_bid, etc.
            digest = data.get('order_digest', '')
            price_x18 = data.get('price', '0')
            filled_qty = data.get('filled_qty', '0')
            original_qty = data.get('original_qty', '0')
            remaining_qty = data.get('remaining_qty', '0')
            is_bid = data.get('is_bid', True)
            product_id = data.get('product_id', self.product_id)
            
            # IMPORTANT: Find the client_order_id from digest
            # buy_orders/sell_orders use client_order_id as key, not digest
            # 时序：fill 可能早于 place_order 返回，先延时再重试，最后兜底
            client_order_id = ''
            for cid, d in self.order_digests.items():
                if d == digest:
                    client_order_id = cid
                    break

            if not client_order_id:
                # 1. 延时：给 place_order 存储映射留时间（REST 响应可能较慢）
                await asyncio.sleep(1.0)
                for cid, d in self.order_digests.items():
                    if d == digest:
                        client_order_id = cid
                        break

            if not client_order_id:
                # 2. 重试 1-2 次
                for _ in range(2):
                    await asyncio.sleep(1.0)
                    for cid, d in self.order_digests.items():
                        if d == digest:
                            client_order_id = cid
                            break
                    if client_order_id:
                        break

            if not client_order_id:
                # 3. 兜底：使用 digest
                logger.warning(f"延迟重试后仍无法根据 digest {digest} 找到 client_order_id，使用 digest 作为 ID")
                client_order_id = digest
            
            # Determine if fully filled or partially filled
            status = 'filled' if remaining_qty == '0' else 'open'
            
            # Amount sign: positive for buy (bid), negative for sell (ask)
            amount_sign = 1 if is_bid else -1
            amount = str(int(original_qty) * amount_sign)
            unfilled = remaining_qty
            
            # Parse price from x18 format
            X18 = 10 ** 18
            price = int(price_x18) / X18
            amount_float = abs(int(original_qty)) / X18
            filled_float = abs(int(original_qty) - int(remaining_qty)) / X18
            
            # Create order in CCXT format directly (bypass converter issues)
            ccxt_order = {
                'id': client_order_id,  # Use client_order_id to match buy_orders/sell_orders
                'clientOrderId': client_order_id,
                'status': 'closed' if status == 'filled' else 'open',  # CCXT uses 'closed' for filled
                'symbol': self.PRODUCT_ID_TO_SYMBOL.get(product_id, f'PRODUCT_{product_id}'),
                'side': 'buy' if is_bid else 'sell',
                'price': price,
                'amount': amount_float,
                'filled': filled_float,
                'remaining': abs(int(remaining_qty)) / X18,
                'cost': filled_float * price,
                'info': data,
            }
            
            logger.info(f"处理成交: client_order_id={client_order_id}, digest={digest}, side={'买' if is_bid else '卖'}, price={price}, amount={amount_float}, status={ccxt_order['status']}")
            
            callback = self.callbacks['orders']
            if asyncio.iscoroutinefunction(callback):
                asyncio.create_task(callback(self.address, [ccxt_order]))
            else:
                callback(self.address, [ccxt_order])

    async def _subscribe_market_stats(self):
        """Subscribe to best bid/offer stream."""
        try:
            message = {
                "method": "subscribe",
                "stream": {
                    "type": "best_bid_offer",
                    "product_id": self.product_id
                },
                "id": 1
            }
            if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                await self.subscriptions_ws_connection.send_json(message)
                logger.info(f"已订阅 product {self.product_id} 的 best_bid_offer")
        except Exception as e:
            logger.error(f"订阅行情失败: {e}")

    async def _subscribe_orders(self):
        """Subscribe to order updates."""
        try:
            sender = self._get_sender_bytes32()
            message = {
                "method": "subscribe",
                "stream": {
                    "type": "order_update",
                    "subaccount": sender,
                    "product_id": self.product_id
                },
                "id": 2
            }
            if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                await self.subscriptions_ws_connection.send_json(message)
                logger.info(f"已订阅 product {self.product_id} 的 order_update")
        except Exception as e:
            logger.error(f"订阅订单失败: {e}")

    async def _subscribe_positions(self):
        """Subscribe to position changes."""
        try:
            sender = self._get_sender_bytes32()
            message = {
                "method": "subscribe",
                "stream": {
                    "type": "position_change",
                    "subaccount": sender,
                    "product_id": self.product_id
                },
                "id": 3
            }
            if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                await self.subscriptions_ws_connection.send_json(message)
                logger.info(f"已订阅 product {self.product_id} 的 position_change")
        except Exception as e:
            logger.error(f"订阅仓位失败: {e}")

    async def _subscribe_fills(self):
        """Subscribe to fill events (order executions)."""
        try:
            sender = self._get_sender_bytes32()
            message = {
                "method": "subscribe",
                "stream": {
                    "type": "fill",
                    "subaccount": sender,
                    "product_id": self.product_id
                },
                "id": 4
            }
            if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                await self.subscriptions_ws_connection.send_json(message)
                logger.info(f"已订阅 product {self.product_id} 的 fill 事件")
        except Exception as e:
            logger.error(f"订阅成交失败: {e}")

    async def close(self):
        """Close connections."""
        self._closing = True
        self.ws_initialized = False
        try:
            for task in (
                self._rest_polling_task_handle,
                self._ws_listener_task_handle,
                self._ws_ping_task_handle,
            ):
                if task and not task.done():
                    task.cancel()
            with contextlib.suppress(Exception):
                await asyncio.gather(
                    *(t for t in (
                        self._rest_polling_task_handle,
                        self._ws_listener_task_handle,
                        self._ws_ping_task_handle,
                    ) if t),
                    return_exceptions=True,
                )

            if self.subscriptions_ws_connection and not self.subscriptions_ws_connection.closed:
                await self.subscriptions_ws_connection.close()
            if self.ws_session and not self.ws_session.closed:
                await self.ws_session.close()
            if self.session and not self.session.closed:
                await self.session.close()
        except Exception as e:
            logger.error(f"关闭连接时发生错误: {e}")

    async def _probe_isolated_position(self) -> None:
        """启动时探测是否已持有当前 product 的仓位，有则标记 isolated margin 已划拨。"""
        try:
            positions = await self.get_positions()
            for pos in positions.values():
                if pos.get('product_id') == self.product_id and pos.get('size', 0) != 0:
                    self._iso_margin_seeded = True
                    logger.info(
                        "isolated margin 已划拨（启动时检测到持仓）: product_id=%s, size=%s",
                        self.product_id, pos['size'],
                    )
                    return
            logger.info(
                "isolated margin 尚未划拨（无持仓）: product_id=%s, 首笔开仓单将携带 %s USDC",
                self.product_id, self.isolated_margin_usdc,
            )
        except Exception as e:
            logger.warning("探测 isolated 持仓失败（首单仍携带 margin）: %s", e)

    async def initialize_client(self) -> None:
        """Initialize the exchange client."""
        try:
            # Fetch contract information from API
            contracts = await self._fetch_contracts()
            self.chain_id = contracts.get('chain_id')
            self.endpoint_address = contracts.get('endpoint')

            if not self.chain_id or not self.endpoint_address:
                logger.warning("无法从 API 获取合约信息，使用硬编码默认值")
                # Use hardcoded values based on environment
                if self.env == 'mainnet':
                    # Mainnet: Ink chain
                    self.chain_id = 57073  # Ink mainnet chain_id
                    self.endpoint_address = "0x05ec92D78ED421f3D3Ada77FFdE167106565974E"
                else:
                    # Testnet: Ink Sepolia
                    self.chain_id = 763373  # Ink Sepolia chain_id
                    self.endpoint_address = "0x698D87105274292B5673367DEC81874Ce3633Ac2"

            # Fetch product trading parameters
            await self._fetch_product_params()
            if self.product_id is None:
                raise ValueError(
                    f"product_id is not set for target symbol {self.target_symbol}. "
                    "Set NADO_PRODUCT_ID in .env."
                )

            logger.info(f"Nado 客户端已初始化: env={self.env}, chain_id={self.chain_id}, endpoint={self.endpoint_address}")
            logger.info(f"产品 {self.product_id} 参数: price_increment={self.price_increment}, size_increment={self.size_increment}, min_size={self.min_size}")

            # 检查是否已有 isolated 持仓，若有则标记保证金已划拨
            if self.isolated_margin and self.isolated_margin_usdc > 0:
                await self._probe_isolated_position()
        except Exception as e:
            logger.error(f"客户端初始化失败: {e}", exc_info=True)
            # Set fallback values based on environment
            if self.env == 'mainnet':
                self.chain_id = 57073
                self.endpoint_address = "0x05ec92D78ED421f3D3Ada77FFdE167106565974E"
            else:
                self.chain_id = 763373
                self.endpoint_address = "0x698D87105274292B5673367DEC81874Ce3633Ac2"

    async def _fetch_product_params(self) -> None:
        """Fetch product trading parameters (price/size increments, min_size)."""
        try:
            # Query all_products to get trading parameters
            data = await self._rest_query("all_products", {})
            if not data:
                logger.warning("无法获取产品参数，使用默认值")
                return

            # Find our product in perp_products
            perp_products = data.get('perp_products', [])
            for product in perp_products:
                pid = product.get("product_id")
                symbol = self._extract_product_symbol(product)
                if symbol and pid is not None:
                    self.PRODUCT_ID_TO_SYMBOL[pid] = symbol

            # If product_id is not explicitly provided, resolve it by target symbol
            if self.product_id is None:
                resolved = self._resolve_product_id_by_symbol(perp_products)
                if resolved is None:
                    raise ValueError(
                        f"Cannot resolve product_id for target symbol '{self.target_symbol}'. "
                        f"Please set NADO_PRODUCT_ID explicitly."
                    )
                self.product_id = resolved
                logger.info(
                    f"已将目标符号 {self.target_symbol} 解析为 product_id={self.product_id}"
                )

            for product in perp_products:
                if product.get('product_id') == self.product_id:
                    symbol = self._extract_product_symbol(product)
                    if symbol:
                        self.PRODUCT_ID_TO_SYMBOL[self.product_id] = symbol
                    book_info = product.get('book_info', {})
                    
                    # price_increment_x18
                    price_inc_x18 = int(book_info.get('price_increment_x18', '100000000000000000'))
                    self.price_increment = self._from_x18(price_inc_x18)
                    
                    # size_increment (in base units)
                    size_inc = int(book_info.get('size_increment', '1000000000000000'))
                    self.size_increment = self._from_x18(size_inc)
                    
                    # min_size (notional value)
                    min_size = int(book_info.get('min_size', '100000000000000000000'))
                    self.min_size = self._from_x18(min_size)
                    
                    logger.info(
                        f"已获取产品 {self.product_id} ({self.PRODUCT_ID_TO_SYMBOL.get(self.product_id, '未知')}) "
                        f"参数: price_inc={self.price_increment}, size_inc={self.size_increment}, min_size={self.min_size}"
                    )
                    return

            logger.warning(f"产品 {self.product_id} 在 perp_products 中未找到")
        except Exception as e:
            logger.error(f"获取产品参数失败: {e}")

    async def create_auth_token(self) -> Tuple[str, str]:
        """
        Create authentication token.
        For Nado, authentication is done via EIP712 signatures.
        """
        if self.account and self.address:
            return self.address, None
        return "", "No private key configured"

    async def get_account_info(self) -> dict:
        """Get detailed account information including positions."""
        try:
            account = await self.get_account()
            positions = await self.get_positions()
            account['positions'] = positions
            return account
        except Exception as e:
            logger.error(f"get_account_info 错误: {e}", exc_info=True)
            return {}


async def run():
    """Test function for the Nado adapter."""
    from dotenv import load_dotenv
    load_dotenv()

    from common.logging_config import setup_logging
    setup_logging()

    adapter = NadoAdapter(market_id=0)
    await adapter.initialize_client()

    # Test get account info
    account_info = await adapter.get_account_info()
    print("Account Info:", json.dumps(account_info, ensure_ascii=False, indent=2, default=str))

    # Test get orders
    orders = await adapter.get_orders()
    print("Orders:", json.dumps(orders, ensure_ascii=False, indent=2, default=str))

    # Test get positions
    positions = await adapter.get_positions()
    print("Positions:", json.dumps(positions, ensure_ascii=False, indent=2, default=str))

    await adapter.close()


if __name__ == "__main__":
    asyncio.run(run())

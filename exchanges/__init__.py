from typing import Optional
import lighter
from common.config import BASE_URL, API_KEY_PRIVATE_KEY, ACCOUNT_INDEX, API_KEY_INDEX
from .interfaces import ExchangeInterface
from .lighter_adapter import LighterAdapter

# Import GRVT adapter if available
try:
    from .grvt_adapter import GrvtAdapter
    GRVT_AVAILABLE = True
except ImportError:
    GRVT_AVAILABLE = False

# Import Nado adapter if available
try:
    from .nado_adapter import NadoAdapter
    NADO_AVAILABLE = True
except ImportError:
    NADO_AVAILABLE = False

# Import Extended adapter if available
try:
    from .extended_adapter import ExtendedAdapter
    EXTENDED_AVAILABLE = True
except ImportError:
    EXTENDED_AVAILABLE = False


def create_exchange_adapter(exchange_type: str = "lighter", **kwargs) -> Optional[ExchangeInterface]:
    """
    Factory function to create exchange adapter based on type.

    Args:
        exchange_type: Type of exchange ("lighter", "grvt", or "nado")
        **kwargs: Additional parameters for adapter initialization

    Returns:
        Exchange adapter instance or None if unsupported type
    """
    if exchange_type.lower() == "lighter":
        # Create signer client
        private_keys = {API_KEY_INDEX: API_KEY_PRIVATE_KEY}
        signer_client = lighter.SignerClient(
            url=BASE_URL,
            api_private_keys=private_keys,
            account_index=ACCOUNT_INDEX,
        )

        market_id = kwargs.get('market_id', 0)

        return LighterAdapter(
            signer_client=signer_client,
            account_index=ACCOUNT_INDEX,
            api_key_index=API_KEY_INDEX,
            market_id=market_id,
        )
    elif exchange_type.lower() == "grvt":
        if not GRVT_AVAILABLE:
            raise ImportError("GRVT adapter not available. Please install grvt-pysdk.")

        market_id = kwargs.get('market_id', 0)
        symbol = kwargs.get('symbol', 'ETH_USDT_Perp')

        return GrvtAdapter(
            market_id=market_id,
            symbol=symbol,
        )
    elif exchange_type.lower() == "nado":
        if not NADO_AVAILABLE:
            raise ImportError("Nado adapter not available. Please install required dependencies (eth-account, aiohttp).")

        market_id = kwargs.get('market_id', 0)
        product_id = kwargs.get('product_id', None)
        subaccount_name = kwargs.get('subaccount_name', 'default')

        return NadoAdapter(
            market_id=market_id,
            product_id=product_id,
            subaccount_name=subaccount_name,
        )
    elif exchange_type.lower() == "extended":
        if not EXTENDED_AVAILABLE:
            raise ImportError("Extended adapter not available. Please install required dependencies (aiohttp).")

        market_id = kwargs.get('market_id', 0)
        symbol = kwargs.get('symbol', 'ETH-USD')

        return ExtendedAdapter(
            market_id=market_id,
            symbol=symbol,
        )
    else:
        return None
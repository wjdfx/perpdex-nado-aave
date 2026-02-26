from typing import Optional
from .interfaces import ExchangeInterface

# Import Nado adapter if available
try:
    from .nado_adapter import NadoAdapter
    NADO_AVAILABLE = True
except ImportError:
    NADO_AVAILABLE = False


def create_exchange_adapter(exchange_type: str = "nado", **kwargs) -> Optional[ExchangeInterface]:
    """
    Factory function to create exchange adapter based on type.

    Args:
        exchange_type: Type of exchange ("nado")
        **kwargs: Additional parameters for adapter initialization

    Returns:
        Exchange adapter instance or None if unsupported type
    """
    if exchange_type.lower() != "nado":
        return None
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

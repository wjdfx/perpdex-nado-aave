from typing import Optional
from .interfaces import ExchangeInterface

# Import Nado adapters if available
try:
    from .nado_adapter import NadoAdapter
    NADO_AVAILABLE = True
except ImportError:
    NADO_AVAILABLE = False

try:
    from .nado_spot_adapter import NadoSpotAdapter
    NADO_SPOT_AVAILABLE = True
except ImportError:
    NADO_SPOT_AVAILABLE = False


def create_exchange_adapter(exchange_type: str = "nado_perp", **kwargs) -> Optional[ExchangeInterface]:
    """
    Factory function to create exchange adapter based on type.

    Args:
        exchange_type: "nado_perp" 合约 | "nado_spot" 现货（二选一）
        **kwargs: Additional parameters for adapter initialization

    Returns:
        Exchange adapter instance or None if unsupported type
    """
    t = (exchange_type or "").strip().lower()
    if t not in ("nado_perp", "nado_spot"):
        return None

    if t == "nado_spot":
        if not NADO_SPOT_AVAILABLE:
            raise ImportError("Nado Spot adapter not available. Check nado_spot_adapter.py.")
        market_id = kwargs.get('market_id', 0)
        product_id = kwargs.get('product_id', None)
        subaccount_name = kwargs.get('subaccount_name', 'default')
        return NadoSpotAdapter(
            market_id=market_id,
            product_id=product_id,
            subaccount_name=subaccount_name,
        )

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

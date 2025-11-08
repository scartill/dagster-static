"""dagster-static: Trait-based static validation for Dagster assets."""

from .traits import (
    asset,
    with_traits,
    provide,
    request,
    resolve,
    DEFAULT_NS,
)

__version__ = '0.1.0'
__all__ = [
    'asset',
    'with_traits',
    'provide',
    'request',
    'resolve',
    'DEFAULT_NS',
]

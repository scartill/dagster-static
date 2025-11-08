"""Trait-based static validation system for Dagster assets.

This module provides a mechanism for defining and validating traits (capabilities)
that assets provide and require, enabling compile-time verification of asset
dependencies and capabilities.
"""

from functools import wraps
from pprint import pformat
from typing import Callable, TypedDict
import logging

import dagster as dg

type Trait = str
"""A trait name (capability identifier) as a string."""

type TraitSpec = str
"""Comma-separated list of trait names, optionally with inheritance syntax."""

type Defined = set[Trait]
"""Set of traits that an asset provides."""

type DefinedDict = dict[dg.AssetKey, Defined]
"""Dictionary mapping asset keys to their provided traits."""


logger = logging.getLogger('dagster-static')


class Requested(TypedDict):
    """Represents trait requirements from an upstream asset.

    Attributes:
        traits: Set of trait names that are required.
        by: The AssetKey of the downstream asset making the request.
    """
    traits: set[Trait]
    by: dg.AssetKey


type RequestedDict = dict[dg.AssetKey, Requested]
"""Dictionary mapping asset keys to their trait requirements."""


class Namespace(TypedDict):
    """Container for trait definitions and requests in a namespace.

    Attributes:
        defined: Dictionary of assets and their provided traits.
        requested: Dictionary of assets and their requested traits.
    """
    defined: DefinedDict
    requested: RequestedDict


type NamespaceDict = dict[str, Namespace]
"""Dictionary mapping namespace names to their trait information."""


namespaces: NamespaceDict = dict()


DEFAULT_NS = 'default'
"""Default namespace identifier for trait definitions."""


def _get_namespace(ns: str):
    namespace = namespaces.get(ns)

    if not namespace:
        namespaces[ns] = {
            'defined': {},
            'requested': {},
        }

    return namespaces[ns]


def provide(
    asset_key: dg.AssetKey,
    provides: TraitSpec,
    ns: str = DEFAULT_NS,
):
    """Register that an asset provides certain traits.

    This function registers that a specific asset provides one or more traits
    (capabilities). Traits can be specified as a comma-separated string, and
    can include upstream trait inheritance using the '+' syntax.

    Args:
        asset_key: The Dagster AssetKey identifying the asset.
        provides: Comma-separated list of traits this asset provides. Can
            include upstream inheritance syntax like '+upstream_key:trait' to
            inherit all traits from an upstream asset and add a new one.
        ns: Namespace identifier for organizing assets (default: 'default').

    Returns:
        The set of traits that the asset now provides (after processing).

    Raises:
        ValueError: If attempting to add traits to an asset that already
            provides different traits (unless using '+' syntax), or if
            upstream asset referenced in '+' syntax doesn't exist.

    Example:
        Basic usage:
        >>> from dagster import AssetKey
        >>> provide(AssetKey(['data', 'raw']), 'raw_data')

        Multiple traits:
        >>> provide(AssetKey(['data', 'multi']), 'trait1,trait2,trait3')

        Inheriting from upstream:
        >>> provide(AssetKey(['upstream']), 'base_trait')
        >>> provide(AssetKey(['downstream']), '+upstream:new_trait')
        # downstream now provides both 'base_trait' and 'new_trait'
    """
    namespace = _get_namespace(ns)
    asset_key_str = asset_key.to_user_string()
    existing = namespace['defined'].get(asset_key_str)

    if not existing:
        existing: Defined = set()
        namespace['defined'][asset_key_str] = existing
        new = True
    else:
        new = False

    new_traits = provides.split(',')

    for trait in new_traits:
        if trait.startswith('+'):
            upspec = trait.lstrip('+').split(':')
            upstream_key_str = upspec[0]
            added_trait = upspec[1]
            upstream_defined = namespace['defined'].get(upstream_key_str)

            if not upstream_defined:
                raise ValueError(
                    f'Asset {upstream_key_str} does not provide anything'
                )

            to_add = set(upstream_defined)
            to_add.add(added_trait)
            append = True
        else:
            to_add = set([trait])
            append = False

        if trait not in existing and not append and not new:
            raise ValueError(
                f'Asset {asset_key_str} already provides "{', '.join(existing)}" '
                f'but requested "{provides}"'
            )

        namespace['defined'][asset_key_str].update(to_add)

    return namespace['defined'][asset_key_str]


def request(
    asset_key: dg.AssetKey,
    traits: TraitSpec,
    by: dg.AssetKey,
    ns: str = DEFAULT_NS,
):
    """Register that an asset requires certain traits from another asset.

    This function records that a downstream asset (identified by 'by') requires
    specific traits from an upstream asset (identified by 'asset_key'). These
    requirements are validated when resolve() is called.

    Args:
        asset_key: The Dagster AssetKey of the upstream asset that should
            provide the requested traits.
        traits: Comma-separated list of trait names that are required.
        by: The Dagster AssetKey of the downstream asset making the request.
        ns: Namespace identifier for organizing assets (default: 'default').

    Example:
        >>> from dagster import AssetKey
        >>> request(
        ...     AssetKey(['data', 'raw']),
        ...     'raw_data',
        ...     by=AssetKey(['data', 'processed'])
        ... )

        Multiple traits:
        >>> request(
        ...     AssetKey(['data', 'source']),
        ...     'trait1,trait2',
        ...     by=AssetKey(['data', 'consumer'])
        ... )
    """
    namespace = _get_namespace(ns)
    asset_key_str = asset_key.to_user_string()
    requested = namespace['requested'].get(asset_key_str)

    if not requested:
        requested = {
            'traits': set(),
            'by': by.to_user_string()
        }

        namespace['requested'][asset_key_str] = requested

    new_traits = traits.split(',')
    requested['traits'].update(new_traits)


def resolve(ns: str = DEFAULT_NS):
    """Validate that all requested traits are provided by their assets.

    This function checks that every trait requested by downstream assets is
    actually provided by the corresponding upstream assets. If any mismatches
    are found, a ValueError is raised with detailed error messages.

    Args:
        ns: Namespace identifier to validate (default: 'default').

    Raises:
        ValueError: If any requested traits are not provided, or if assets
            that are requested don't provide any traits. The error message
            includes detailed information about which assets have mismatches.

    Example:
        >>> from dagster import AssetKey
        >>> from dagster_static import provide, request, resolve
        >>>
        >>> # Define what assets provide
        >>> provide(AssetKey(['data', 'raw']), 'raw_data')
        >>>
        >>> # Request traits from assets
        >>> request(AssetKey(['data', 'raw']), 'raw_data', by=AssetKey(['processed']))
        >>>
        >>> # Validate - this will raise ValueError if traits don't match
        >>> resolve()

    Note:
        This function is typically called automatically by the @with_traits
        decorator, but can be called manually for explicit validation.
    """
    namespace = _get_namespace(ns)
    errors = []

    for asset_key_str, requested in namespace['requested'].items():
        if asset_key_str not in namespace['defined']:
            errors.append(
                f'Asset "{ns}:{asset_key_str}" provides nothing '
                f'but requested "{requested["traits"]} by {requested["by"]}"'
            )

            continue

        defined = namespace['defined'][asset_key_str]

        requested_traits = requested['traits']
        requested_by = requested['by']

        if not defined.issuperset(requested_traits):
            errors.append(
                f'Asset {ns}:{asset_key_str} provides only "{', '.join(defined)}" '
                f'but expected "{', '.join(requested_traits)}" '
                f'requested by {requested_by}'
            )

    if errors:
        print(f'Namespace {ns} has errors: {errors}, dumping namespace')
        print(f'Namespace dump: {pformat(namespace)}')
        raise ValueError(f'dagster-static: Errors in namespace {ns}: {errors}')


def asset(
    *,
    key: dg.AssetKey,
    provides: TraitSpec,
    traits: dict[dg.AssetKey, TraitSpec] = dict(),
    ns: str = DEFAULT_NS,
    **kwargs,
) -> Callable[[Callable], dg.AssetsDefinition]:
    """Decorator for defining Dagster assets with trait-based validation.

    This decorator extends Dagster's @asset decorator with trait-based static
    validation. It allows you to declare what capabilities (traits) an asset
    provides and what traits it requires from upstream assets.

    Args:
        key: The Dagster AssetKey for this asset (required).
        provides: Comma-separated list of traits this asset provides. Can
            include upstream inheritance using '+upstream_key:trait' syntax.
        traits: Dictionary mapping upstream AssetKeys to comma-separated lists
            of traits required from those assets. The keys are the upstream
            asset keys, and values are the required traits.
        ns: Namespace identifier for organizing assets (default: 'default').
        **kwargs: Additional keyword arguments passed directly to Dagster's
            @asset decorator (e.g., deps, metadata, group_name, etc.).

    Returns:
        A decorator function that wraps the asset function and registers it
        with trait validation metadata.

    Raises:
        ValueError: If trait definitions are invalid (e.g., trying to add
            traits to an asset that already provides different traits, or
            referencing non-existent upstream assets).

    Example:
        Basic asset with traits:
        >>> from dagster import AssetKey
        >>> from dagster_static import asset
        >>>
        >>> @asset(
        ...     key=AssetKey(['data', 'raw']),
        ...     provides='raw_data'
        ... )
        >>> def raw_data_asset():
        ...     return {'data': 'raw'}

        Asset requiring traits from upstream:
        >>> @asset(
        ...     key=AssetKey(['data', 'processed']),
        ...     provides='processed_data',
        ...     traits={AssetKey(['data', 'raw']): 'raw_data'}
        ... )
        >>> def processed_data_asset(raw_data_asset):
        ...     return {'data': 'processed'}

        Inheriting traits from upstream:
        >>> @asset(
        ...     key=AssetKey(['data', 'enriched']),
        ...     provides='+data/raw:enriched_data'
        ... )
        >>> def enriched_data_asset(raw_data_asset):
        ...     # This asset provides both 'raw_data' (inherited) and 'enriched_data'
        ...     return {'data': 'enriched'}

    Note:
        The trait information is automatically added to the asset's metadata
        under 'dgst-provides' and 'dgst-traits' keys for debugging purposes.
    """
    defined = provide(key, provides, ns)

    for asset_key, trait in traits.items():
        request(asset_key, trait, key, ns)

    sd_metadata = {
        'dgst-provides': list(defined),
        'dgst-traits': list(map(
            lambda kv: f'{kv[0].to_user_string()}@{kv[1]}',
            traits.items()
        ))
    }

    if def_metadata := kwargs.get('metadata'):
        def_metadata.update(sd_metadata)
    else:
        kwargs['metadata'] = sd_metadata

    def decorator(func) -> Callable:
        @dg.asset(key=key, **kwargs)
        @wraps(func)
        def wrapper(*args, **kwargs) -> Callable:
            return func(*args, **kwargs)

        return wrapper

    return decorator


def with_traits(
    *,
    ns: str = DEFAULT_NS
):
    """Decorator that validates trait dependencies before function execution.

    This decorator performs static validation of all trait dependencies in the
    specified namespace before the decorated function is executed. It ensures
    that all requested traits are actually provided by their corresponding
    assets.

    Args:
        ns: Namespace identifier to validate (default: 'default').

    Returns:
        A decorator function that validates traits and wraps the target
        function.

    Raises:
        ValueError: If trait validation fails. The error includes detailed
            information about which assets have mismatched trait dependencies.

    Example:
        Validate traits before running a pipeline:
        >>> from dagster_static import with_traits
        >>>
        >>> @with_traits(ns='default')
        >>> def run_pipeline():
        ...     # All trait dependencies are validated before this executes
        ...     dagster_context.materialize(assets)

        Validate a specific namespace:
        >>> @with_traits(ns='custom_namespace')
        >>> def run_custom_pipeline():
        ...     pass

    Note:
        This decorator should be applied to functions that set up or execute
        your Dagster pipeline. It performs validation at decoration time,
        so any trait mismatches will be caught when the module is imported,
        not when the function is called.
    """
    resolve(ns)

    def decorator(func):
        @wraps(func)
        def wrapper(**kwargs):
            return func(**kwargs)

        return wrapper
    return decorator

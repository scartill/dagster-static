from functools import wraps
from pprint import pformat
from typing import Callable, TypedDict
import logging

import dagster as dg

type Trait = str
type TraitSpec = str
type Defined = set[Trait]
type DefinedDict = dict[dg.AssetKey, Defined]


logger = logging.getLogger('dagster-static')


class Requested(TypedDict):
    traits: set[Trait]
    by: dg.AssetKey


type RequestedDict = dict[dg.AssetKey, Requested]


class Namespace(TypedDict):
    defined: DefinedDict
    requested: RequestedDict


type NamespaceDict = dict[str, Namespace]


namespaces: NamespaceDict = dict()


DEFAULT_NS = 'default'


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
    resolve(ns)

    def decorator(func):
        @wraps(func)
        def wrapper(**kwargs):
            return func(**kwargs)

        return wrapper
    return decorator

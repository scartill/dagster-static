# dagster-static

A trait-based static validation system for Dagster assets that enables load-time verification of asset dependencies and capabilities.

## Overview

`dagster-static` provides a mechanism for defining and validating traits (capabilities) that assets provide and require. This allows you to catch dependency mismatches at definition time rather than runtime, making your Dagster pipelines more robust and easier to maintain.

## Features

- **Trait-based Asset Definition**: Declare what capabilities each asset provides
- **Static Validation**: Verify that requested traits are provided by upstream assets
- **Namespace Support**: Organize assets into separate namespaces for isolation
- **Upstream Trait Inheritance**: Inherit traits from upstream assets with the `+` syntax
- **Rich Error Messages**: Clear error messages when trait mismatches are detected

## Installation

```bash
pip install dagster-static
```

Or using `uv`:

```bash
uv add dagster-static
```

## Requirements

- Python >= 3.13
- Dagster >= 1.12.0

## Quick Start

### Basic Usage

```python
from dagster import AssetKey
from dagster_static import asset, with_traits

# Define an asset that provides a trait
@asset(
    key=AssetKey(['data', 'raw']),
    provides='raw_data'
)
def raw_data_asset():
    return {'data': 'raw'}

# Define an asset that requires a trait from another asset
@asset(
    key=AssetKey(['data', 'processed']),
    provides='processed_data',
    traits={AssetKey(['data', 'raw']): 'raw_data'}
)
def processed_data_asset(raw_data_asset):
    return {'data': 'processed'}

# Validate all trait dependencies
@with_traits()
def main():
    # Your Dagster pipeline code here
    pass
```

### Using Namespaces

You can organize assets into separate namespaces to avoid conflicts:

```python
from dagster_static import asset, with_traits, DEFAULT_NS

# Assets in the default namespace
@asset(
    key=AssetKey(['data', 'raw']),
    provides='raw_data',
    ns=DEFAULT_NS
)
def raw_data_asset():
    return {'data': 'raw'}

# Assets in a custom namespace
@asset(
    key=AssetKey(['data', 'raw']),
    provides='raw_data',
    ns='custom'
)
def custom_raw_data_asset():
    return {'data': 'custom_raw'}

# Validate a specific namespace
@with_traits(ns='custom')
def validate_custom_namespace():
    pass
```

### Inheriting Traits from Upstream Assets

You can inherit traits from upstream assets using the `+` syntax:

```python
@asset(
    key=AssetKey(['data', 'raw']),
    provides='raw_data'
)
def raw_data_asset():
    return {'data': 'raw'}

@asset(
    key=AssetKey(['data', 'enriched']),
    provides='+data/raw:enriched_data,raw_data'
)
def enriched_data_asset(raw_data_asset):
    # This asset provides both 'enriched_data' and 'raw_data'
    # (inherited from the upstream asset)
    return {'data': 'enriched'}
```

### Multiple Traits

Assets can provide and request multiple traits:

```python
@asset(
    key=AssetKey(['data', 'multi']),
    provides='trait1,trait2,trait3'
)
def multi_trait_asset():
    return {'multi': True}

@asset(
    key=AssetKey(['data', 'consumer']),
    provides='result',
    traits={
        AssetKey(['data', 'multi']): 'trait1,trait2'
    }
)
def consumer_asset(multi_trait_asset):
    return {'result': True}
```

## API Reference

### `asset` Decorator

The main decorator for defining assets with traits.

**Parameters:**
- `key` (AssetKey): The asset key (required)
- `provides` (str): Comma-separated list of traits this asset provides (required)
- `traits` (dict[AssetKey, str]): Dictionary mapping upstream asset keys to their required traits (optional)
- `ns` (str): Namespace for this asset (default: `'default'`)
- `**kwargs`: Additional arguments passed to Dagster's `@asset` decorator

**Returns:**
- Decorated function that creates a Dagster asset with trait validation

### `with_traits` Decorator

Validates all trait dependencies in a namespace before execution.

**Parameters:**
- `ns` (str): Namespace to validate (default: `'default'`)

**Returns:**
- Decorator that validates traits before executing the wrapped function

**Raises:**
- `ValueError`: If trait dependencies cannot be resolved

### `provide` Function

Manually register that an asset provides certain traits.

```python
from dagster_static import provide
from dagster import AssetKey

provide(AssetKey(['data', 'raw']), 'raw_data')
```

### `request` Function

Manually register that an asset requests certain traits from another asset.

```python
from dagster_static import request
from dagster import AssetKey

request(
    AssetKey(['data', 'raw']),
    'raw_data',
    by=AssetKey(['data', 'processed'])
)
```

### `resolve` Function

Manually validate trait dependencies in a namespace.

```python
from dagster_static import resolve

resolve('default')  # Validates the default namespace
```

**Raises:**
- `ValueError`: If trait dependencies cannot be resolved

## Error Messages

When trait validation fails, `dagster-static` provides detailed error messages:

```
dagster-static: Errors in namespace default: [
    'Asset "default:data/processed" provides nothing but requested "{\'raw_data\'} by data/consumer"',
    'Asset default:data/raw provides only "raw" but expected "raw_data" requested by data/processed'
]
```

These messages help you quickly identify which assets have mismatched trait dependencies.

## Development

### Setup

1. Clone the repository:
```bash
git clone https://github.com/scartill/dagster-static
cd dagster-static
```

2. Install dependencies using `uv`:
```bash
uv sync
```

### Code Style

This project follows PEP 8 with a maximum line length of 160 characters. It uses:
- `flake8` for linting
- `ruff` for formatting (line length: 120, single quotes)

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Author

Boris Resnick (boris.resnick@gmail.com)

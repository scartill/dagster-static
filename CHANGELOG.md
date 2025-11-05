# 0.1.0

- Initial release of `dagster-static`
- Trait-based asset definition system with `@asset` decorator
- Static validation of asset dependencies using `@with_traits` decorator
- Support for multiple traits per asset
- Namespace support for organizing assets into separate groups
- Upstream trait inheritance using `+` syntax (e.g., `+upstream:new_trait`)
- Manual trait registration via `provide()` and `request()` functions
- Manual validation via `resolve()` function

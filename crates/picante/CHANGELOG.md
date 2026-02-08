# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/bearcove/picante/compare/picante-v0.1.0...picante-v0.2.0) - 2026-02-08

### Added

- make jit feature opt-in instead of default
- in-flight tracked-query deduplication across snapshots ([#28](https://github.com/bearcove/picante/pull/28))
- reduce DerivedIngredient compile time via type erasure ([#26](https://github.com/bearcove/picante/pull/26))

### Fixed

- update type-erased callbacks to take owned bytes

### Other

- Cool it on the info/debug spans
- Update to latest facet, fix Option/Result equality checks
- Update configs
- Mark decode_to_heap_value as unsafe fn
- Lift type-erased serialize/deserialize to standalone #[inline(never)] functions ([#53](https://github.com/bearcove/picante/pull/53))
- Use type-erased facet APIs for persistence callbacks ([#52](https://github.com/bearcove/picante/pull/52))
- Add get_erased helper to reduce get/touch monomorphization ([#50](https://github.com/bearcove/picante/pull/50))
- Reduce monomorphization in DynIngredient::touch and snapshot_cells_deep ([#48](https://github.com/bearcove/picante/pull/48))
- Reduce monomorphization in derived ingredient ([#47](https://github.com/bearcove/picante/pull/47))
- Convert dodeca config from KDL to YAML
- Upgrade deps
- rewrite picante spec as semantics-only
- address review feedback and clarify semantics
- Add tracey annotations for remaining rules
- Add tracey annotations for snapshot rules
- Add comprehensive tracey spec and code annotations
- facet v0.40.0
- type-erase InputIngredient persistence methods
- Switch from facet-postcard to facet-format-postcard with JIT ([#44](https://github.com/bearcove/picante/pull/44))
- Switch from facet-dev to captain
- type-erase PersistableIngredient methods for DerivedIngredient
- Remove tokio macros feature to eliminate syn dependency from tests ([#43](https://github.com/bearcove/picante/pull/43))
- Fix misleading comment about WAL compaction temporary file naming ([#41](https://github.com/bearcove/picante/pull/41))
- Fix WAL compaction temporary file naming and collision bug ([#40](https://github.com/bearcove/picante/pull/40))
- Address Copilot PR feedback on WAL compaction ([#39](https://github.com/bearcove/picante/pull/39))
- Address PR feedback for WAL implementation ([#38](https://github.com/bearcove/picante/pull/38))
- Implement incremental persistence with Write-Ahead Log (WAL) ([#37](https://github.com/bearcove/picante/pull/37))
- Add debugging and observability tools ([#15](https://github.com/bearcove/picante/pull/15)) ([#36](https://github.com/bearcove/picante/pull/36))
- Improve facet_eq to handle containers with non-PartialEq elements ([#35](https://github.com/bearcove/picante/pull/35))
- Replace facet-diff with vtable-based equality ([#34](https://github.com/bearcove/picante/pull/34))
- Add cache persistence convenience methods to #[picante::db] macro ([#33](https://github.com/bearcove/picante/pull/33))
- Replace facet-assert with facet-diff in production code ([#31](https://github.com/bearcove/picante/pull/31))
- stress tests
- Fix snapshot revalidation + lift cells to upper db
- Queries in snapshots are now promoted to the main database
- Improve documentation for ingredient implementations
- Add database snapshot support ([#22](https://github.com/bearcove/picante/pull/22))
- :input: support singleton inputs ([#6](https://github.com/bearcove/picante/pull/6)) ([#11](https://github.com/bearcove/picante/pull/11))
- :db: generate combined Db trait ([#7](https://github.com/bearcove/picante/pull/7)) ([#10](https://github.com/bearcove/picante/pull/10))
- implement #[picante::db] ([#4](https://github.com/bearcove/picante/pull/4))
- pointer-identity fast path for Arc/Rc/Box ([#3](https://github.com/bearcove/picante/pull/3))
- Add picante proc-macros + docs site refresh ([#2](https://github.com/bearcove/picante/pull/2))
- Avoid no-op input bumps; restore dep graph on load
- Add InternedIngredient
- Fix doctests: add IngredientLookup impl and wrap bare URLs
- Add IngredientLookup impl to test and bench code
- DynIngredient etc.
- Move facet-assert to dev-dependencies
- Add custom facet-dev templates for README generation
- Add README etc.
- Fix doctest syntax in code example
- async runtime, persistence, CI, docs

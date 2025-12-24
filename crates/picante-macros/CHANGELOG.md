# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/bearcove/picante/releases/tag/picante-macros-v0.1.0) - 2025-12-24

### Added

- in-flight tracked-query deduplication across snapshots ([#28](https://github.com/bearcove/picante/pull/28))

### Other

- Implement incremental persistence with Write-Ahead Log (WAL) ([#37](https://github.com/bearcove/picante/pull/37))
- Add cache persistence convenience methods to #[picante::db] macro ([#33](https://github.com/bearcove/picante/pull/33))
- Queries in snapshots are now promoted to the main database
- Add database snapshot support ([#22](https://github.com/bearcove/picante/pull/22))
- Include Has*Query traits in DbTrait to fix bounds explosion ([#20](https://github.com/bearcove/picante/pull/20))
- :db: add _ingredient suffix to generated field names ([#12](https://github.com/bearcove/picante/pull/12))
- :input: support singleton inputs ([#6](https://github.com/bearcove/picante/pull/6)) ([#11](https://github.com/bearcove/picante/pull/11))
- :db: generate combined Db trait ([#7](https://github.com/bearcove/picante/pull/7)) ([#10](https://github.com/bearcove/picante/pull/10))
- implement #[picante::db] ([#4](https://github.com/bearcove/picante/pull/4))
- Add picante proc-macros + docs site refresh ([#2](https://github.com/bearcove/picante/pull/2))

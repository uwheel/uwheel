# µWheel Changelog

This file contains all notable changes to µWheel.


## 0.2.1  (2024-08-22)

### Fixed

* Fix split wheel range bug with certain dates [#147](https://github.com/uwheel/uwheel/pull/147) by [@Max-Meldrum](https://github.com/Max-Meldrum)
* Remove assert in combine_range favouring ``None`` instead [#143](https://github.com/uwheel/uwheel/pull/143) by [@Max-Meldrum](https://github.com/Max-Meldrum)
* Fix return type for combine_range_and_lower [#140](https://github.com/uwheel/uwheel/pull/140) by [@Max-Meldrum](https://github.com/Max-Meldrum)

### Added

* Add initial test cases using proptest [#147](https://github.com/uwheel/uwheel/pull/147) by [@Max-Meldrum](https://github.com/Max-Meldrum)
* Add group_by benches [#144](https://github.com/uwheel/uwheel/pull/144) by [@Max-Meldrum](https://github.com/Max-Meldrum)


## 0.2.0 (2024-07-13)

### Added

* Add full ``group_by`` query [#139](https://github.com/uwheel/uwheel/pull/139) by [@Max-Meldrum](https://github.com/Max-Meldrum)
* Add full ``serde`` compatibility [#137](https://github.com/uwheel/uwheel/pull/137) by [@Max-Meldrum](https://github.com/Max-Meldrum)
* Add ``Session Windowing`` support [#136](https://github.com/uwheel/uwheel/pull/136) by [@Max-Meldrum](https://github.com/Max-Meldrum)
* Add ``MinMax`` Aggregator [b47b825](https://github.com/uwheel/uwheel/commit/b47b825267a55ae60cc79ad0c9bd698a1455e2e7) by [@Max-Meldrum](github.com/Max-Meldrum)
* Add ``Bitpacking`` encoding example [#131](https://github.com/uwheel/uwheel/pull/131) by [@Max-Meldrum](https://github.com/Max-Meldrum)

### Changed

* Increase number of test cases [#60](https://github.com/uwheel/uwheel/commit/6559b17cf999c5f07b16dc32919de8563ac84b13) by [@Max-Meldrum](https://github.com/Max-Meldrum)
* Improved query performance for compressed aggregates [#130](https://github.com/uwheel/uwheel/pull/130) by [@Max-Meldrum](https://github.com/Max-Meldrum)

### Breaking Changes

* Rework the ``Window`` API to be more extensible [#136](https://github.com/uwheel/uwheel/pull/136) by [@Max-Meldrum](https://github.com/Max-Meldrum)

# Hierarchical Aggregation Wheel (HAW)

Hierarchical Aggregation Wheel is data structure that pre-computes and maintains aggregates
across stream event time. The overall goal is to cover both Streaming + OLAP needs in a single data structure and not to outperform handtuned algorithms (e.g., sliding window algorithms)

## Features

- Fast insertions
- Compact and highly compressible
- Event-time driven using low watermarking
- Bounded query latency
- Roll-ups & drill-downs with the ``drill_down`` feature enabled
- Compatible with `#[no_std]`

## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.
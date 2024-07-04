Here is an example showing how to configure an Aggregator with compression support.

This example uses [pco](https://github.com/mwlon/pcodec) and [bitpacking](https://docs.rs/bitpacking/latest/bitpacking/) to compress aggregates at the seconds granularity and prints the wheel size of compression vs. non-compression.

```sh
cargo run --release -p compression
```

Here is an example showing how to use a Top-N Aggregator.

The example uses [tinystr](https://docs.rs/tinystr/latest/tinystr/) for fixed-sized keys and uses a U32 Sum Aggregator to calculate top 5 sums

```sh
cargo run --release -p top-n
```
# μWheel

The benchmarks in this repo have been executed in a Linux environment, more specfically debian-based linux.

In addition, it is best to execute the benchmarks on an architecture where [TSC](https://en.wikipedia.org/wiki/Time_Stamp_Counter) is available to enable recording of fine-grained latencies.
Otherwise, the recorded percentiles may look strange.

## Install Rust

1. Go to https://rustup.rs/.
2. Follow the installation instructions for your operating system.
3. Verify the installation with rustc --version and cargo --version.

## Dependecies

You need to ensure that your system has the correct deps in order to run the evaluation

### Downloading datasets

1. wget

```bash
apt install wget
```

### Compilation 

1. rust
2. g++
3. git
4. cmake + make


```bash
apt install cmake make git g++
```

## Plotting

1. Python
2. Jupyter Notebook

```bash
apt install python3 # if not installed

apt install python3-pip

pip3 install jupyter
```

## Running Benchmarks

The benchmarks for both **Online Stream Aggregation** and **Offline Analytical Aggregation** may be found in the benchmarks directory.

To run the benchmarks, see the scripts within the directory. Note that it may take a while depending on your configured hardware.
If a run is killed, then you may not have enough DRAM (see paper on hardware requirements).

To run all types of benchmarks, run the following:

```bash
./run.sh
# Or run detached (recommended if using ssh to a server)
nohup ./run.sh &
```

## Visualizing the results

```bash
jupyter notebook analysis.ipynb
```


## License

Licensed under either of

* Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

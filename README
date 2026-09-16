# fio KV Plugin

Key-Value (KV) plugin for [fio](https://github.com/axboe/fio). Allows you to generate Key-value workloads for KV-SSDs with variable key/value distributions.

---

## Installation & Setup

### 1. Clone the Repository
```bash
git clone https://github.com/constantin9845/fio-kv-plugin.git
cd fio-kv-plugin
```

### 2. Build the Plugin
```bash
cd app/fio_plugin
make
```

The build will generate `fio-3.3`

---

## Usage

Workflow regardless of CLI or job file

```bash
cd fio-kv-plugin

# prepare drive
./run.sh 

cd app/fio_plugin
```

### Command Line 
Minimum **required** options:
```bash
fio --name=kv_test \
    --ioengine=./unvme2_fio_plugin \
    --json_path=./unvme2_config.json \
    --bs=64
    --direct=1
    --filename=0000.00.05.0
    --variable_key_size=1
    --variable_value_size=1
    --key_ratio=32.128_90.10
    --value_ratio=32.128.2048_15.25.60
```

### Job File Example
Create a file named `kv_test.fio`:

```ini
[global]

ioengine=./unvme2_fio_plugin
json_path=./unvme2_config.json


size=5G
bs=64 # fixed at 64!

thread=1
direct=1
filename=0000.00.05.0
exitall_on_error=1


variable_key_size=1

# Format : {size}.{size}..._{ratio}.{ratio}... (max 6)
key_ratio=32.64_20.80

variable_value_size=1

# Format : {size}.{size}..._{ratio}.{ratio}... (max 5)
value_ratio=128.1024_50.50


[load]
stonewall
rw=write
iodepth=128
numjobs=1

[warmup]
stonewall
rw=randwrite
iodepth=128
randseed=100
random_distribution=pareto:0.9

[run]
stonewall
rw=randrw
rwmixread=90
iodepth=32
numjobs=1
randseed=100
random_distribution=pareto:0.9

```

Run the job file:
```bash
sudo ./fio-3.3 kv_test.fio
```

---

## Important: Changed/New options

| Option | Description | Default / Example |
| :--- | :--- | :--- |
| `ioengine` | Compiled plugin | `./unvme2_fio_plugin` |
| `json_path` | Drive description | `./unvme2_config.json` |
| `filename` | Target drive set up in `run.sh` | `0000.00.05.0` |
| `bs` | Block size is fixed at 64, so when providing a `size` fio will assume a block size of 64 was used (thereby issuing much more IO if actual average block size is larger).  | `64` |
| `size` | Need to manually convert size to match actual block size: 1. Calculate average block size based on key/value size distribution. 2. in bytes: (Target size / average bs) * 64 | `/` |
| `variable_key_size` | Use variable key size | `1` |
| `variable_value_size` | Use variable value size | `1` |
| `key_ratio` | Specify distribution of key sizes, example sets 20% of keys to 32 bytes and 80% of keys to 64 bytes **(max 6 values)** | `32.64_20.80` |
| `value_ratio` | Specify distribution of value sizes, example sets 50% of keys to 128 bytes and 50% of keys to 1024 bytes **(max 5 values)** | `value_ratio=128.1024_50.50` |


---

## License

Distributed under the MIT License. See `LICENSE` for more information.

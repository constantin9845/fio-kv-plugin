#!/bin/bash

# Check if all 3 arguments are provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <size> <value_ratio> <distribution>"
    echo "key_ratio format  : Example: key_ratio=8.16.32.64.128.256_10.20.20.20.20.10 --> key ratio={size.size..}_{ratio.ratio..} (max 6)"
    echo "value_ratio format: Example: value_ratio=64.128.256.512.1024_70.15.10.4.1 --> value ratio={size.size..}_{ratio.ratio..} (max 5)"
    echo "Example: $0 1650M value_ratio=64.128.256.512.1024_70.15.10.4.1 pareto:0.9"
    exit 1
fi

# Assign arguments to variables for clarity
VAR_SIZE=$1
VAR_VR=$2
VAR_DIST=$3

echo "Executing FIO: SIZE=$VAR_SIZE RATIO=$VAR_VR, DIST=$VAR_DIST"
sleep 10

sudo SIZE=$VAR_SIZE \
     RATIO=$VAR_VR \
     DISTRIBUTION=$VAR_DIST \
     ./fio-3.3 run_anykey/pre.fio

sleep 120

sudo SIZE=$VAR_SIZE \
     RATIO=$VAR_VR \
     DISTRIBUTION=$VAR_DIST \
     ./fio-3.3 run_anykey/workload.fio
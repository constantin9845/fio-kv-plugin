#!/bin/bash

# Check if all 3 arguments are provided
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <size> <value_ratio> <distribution>"
    echo "Example: $0 <size1>M <size2>M 64.76:128.10:256.5 pareto:0.9"
    exit 1
fi

# Assign arguments to variables for clarity
VAR_SIZE1=$1
VAR_SIZE2=$2
VAR_VR=$3
VAR_DIST=$4

echo "Executing FIO: SIZE=$VAR_SIZE1 RATIO=$VAR_VR, DIST=$VAR_DIST"
sleep 10

sudo SIZE=$VAR_SIZE1 \
     RATIO=$VAR_VR \
     DISTRIBUTION=$VAR_DIST \
     ./fio-3.3 run_anykey/pre2.fio

sleep 120

echo "Executing FIO: SIZE=$VAR_SIZE2 RATIO=$VAR_VR, DIST=$VAR_DIST"

sudo SIZE=$VAR_SIZE2 \
     RATIO=$VAR_VR \
     DISTRIBUTION=$VAR_DIST \
     ./fio-3.3 run_anykey/workload2.fio
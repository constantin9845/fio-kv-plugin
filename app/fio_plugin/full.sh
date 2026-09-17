#!/bin/bash

# Assign arguments to variables for clarity
VAR_SIZE=$1
VAR_DIST=$2

echo "Executing FIO: SIZE=$VAR_SIZE, DIST=$VAR_DIST"
sleep 10

sudo SIZE=$VAR_SIZE \
     DISTRIBUTION=$VAR_DIST \
     ./fio-3.3 run_anykey/pre.fio

sleep 120

sudo SIZE=$VAR_SIZE \
     DISTRIBUTION=$VAR_DIST \
     ./fio-3.3 run_anykey/workload.fio
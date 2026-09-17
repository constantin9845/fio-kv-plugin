#!/bin/bash

# Assign arguments to variables for clarity
VAR_SIZE=$1
KEY=$2
VALUE=$3
VAR_DIST=$4

echo "Executing FIO: SIZE=$VAR_SIZE, KEY_RATIO=$KEY, VALUE_RATIO=$VALUE, DIST=$VAR_DIST"
sleep 10

sudo SIZE=$VAR_SIZE \
     KEY_RATIO=$KEY \
     VALUE_RATIO=$VALUE \
     DISTRIBUTION=$VAR_DIST \
     ./fio-3.3 run_anykey/pre.fio

sleep 120

sudo SIZE=$VAR_SIZE \
     KEY_RATIO=$KEY \
     VALUE_RATIO=$VALUE \
     DISTRIBUTION=$VAR_DIST \
     ./fio-3.3 run_anykey/workload.fio
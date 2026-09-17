#!/bin/bash

# Assign arguments to variables for clarity
VAR_DIST=$1

echo "Executing FIO: DIST=$VAR_DIST"
sleep 5

sudo DISTRIBUTION=$VAR_DIST ./fio-3.3 run_anykey/pre.fio

sleep 120

sudo DISTRIBUTION=$VAR_DIST ./fio-3.3 run_anykey/workload.fio
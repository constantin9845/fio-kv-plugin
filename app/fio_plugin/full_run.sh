#!/usr/bin/env bash

HUGE_PAGES=1024

cleanup_memory(){
	echo "*** Clean SPDK environment ***"

	# kill existing fio processes
	sudo killall -9 fio 2>/dev/null || true

	# Remove unreleased hugepage map files
	sudo rm -rf /dev/hugepages/*

	# Reset hugepage allocation
	echo 0 | sudo tee /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages
	echo $HUGE_PAGES | sudo tee /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages

	# wait till pages are ready
	while true; do
		READY=$(cat /sys/kernel/mm/hugepages/hugepages-2048kB/nr_hugepages)
		if [ "$READY" -eq "$HUGE_PAGES" ]; then
			echo "$READY pages ready"
			break
		fi
		echo "$READY/$HUGE_PAGES ready..."
		sleep 0.5
	done

	# Clear remaining shared memory segments
	sudo ipcs -m | awk '$6==0 {print $2}' | xargs -r sudo ipcrm -m 2>/dev/null
}

# Uses ASAN optionally to report on segmentation errors
#FIO_COMMAND="sudo LD_PRELOAD=$(gcc -print-file-name=libasan.so) ASAN_OPTIONS=detect_leaks=0 ./fio-3.3"

FIO_COMMAND="sudo ./fio-3.3"


# *** Phase 1 : Load ***
sleep 2
echo "Phase 1: Load\n"
$FIO_COMMAND load.fio

# *** Phase 2 : Warmup ***
cleanup_memory
sleep 10
echo "Phase 2: Warmup\n"
$FIO_COMMAND warmup.fio

# *** Phase 3 : Test run ***
cleanup_memory
sleep 60
echo "Phase 3: Test\n"
$FIO_COMMAND run.fio

cleanup_memory

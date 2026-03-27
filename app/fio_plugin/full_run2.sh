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

usage() {
    echo "Usage: $0 -r [W1|W2|W3|W4|W5|W6] -d [uniform|zipf|pareto|normal]"
    exit 1
}

while getopts ":r:d:" opt; do
  case $opt in
    r) 
		case "$OPTARG" in
			W1|W2|W3|W4|W5|W6) RATIO="$OPTARG" ;;
			*) echo "Error: Invalid distribution '$OPTARG'."; usage ;;
		esac
		;;
	d)
		case "$OPTARG" in
			uniform|zipf|pareto|normal) DIST="$OPTARG" ;;
			*) echo "Error: Invalid distribution '$OPTARG'."; usage ;;
		esac
		;;
	\?)
      	echo "Error: Invalid option -$OPTARG"
      	usage
      	;;
	:)
      	echo "Error: Option -$OPTARG requires an argument."
      	usage
      	;;
  esac
done

if [[ -z "$RATIO" || -z "$DIST" ]]; then
    echo "Error: Both -r and -d are required."
    usage
fi

echo "Running Simulation: Ratio $RATIO with $DIST distribution..."

# Uses ASAN optionally to report on segmentation errors
#FIO_COMMAND="sudo LD_PRELOAD=$(gcc -print-file-name=libasan.so) ASAN_OPTIONS=detect_leaks=0 ./fio-3.3"

FIO_COMMAND="sudo ./fio-3.3"


# *** Run ***
sleep 10
echo "Phase 1+2: Test --> run_pink/${DIST}/pre_${RATIO}.fio\n"
$FIO_COMMAND "run_pink/${DIST}/pre_${RATIO}.fio"

sleep 120
echo "Phase 3: Test --> run_pink/${DIST}/${RATIO}.fio\n"
$FIO_COMMAND "run_pink/${DIST}/${RATIO}.fio"

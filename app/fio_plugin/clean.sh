echo "*** Clean SPDK environment ***"

HUGE_PAGES=1024

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

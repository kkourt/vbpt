#!/bin/bash

cores=$(nproc)
samples=10

if [ -z "$1" ]; then
	echo "Usage: $0 <prog>"
	exit 1
fi
prog=$1

ifgetips="./scripts/ifgetips"

function machine_info() {
	echo "DATE: $(date +%Y%m%d.%H%M%S)"
	echo "MACHINE INFO"
	uname -a
	LC_ALL=C $ifgetips
}

function gethname() {
	h=$(LC_ALL=C $ifgetips | egrep -oe '[^[:space:]]+\.in\.barrelfish\.org' | sed -e 's/\.in\.barrelfish\.org//')
	if [ -z "$h" ]; then
		h=$(hostname)
	fi
	echo $h
}

logfname="/dev/shm/$prog-$(gethname)-log"

machine_info | sed -e 's/^/# /' | tee $logfname
for t in $(seq 1 $(nproc)); 
do
	echo "***** $t *****";
	cmd="env MT_NCPUS=$t $prog /dev/shm/A 48000 4096"
	echo "# Running $cmd (x${samples})"
	for i in $(seq 1 $samples);
	do
		sync;
		#echo 3 > /proc/sys/vm/drop_caches ;
		$cmd;
	done;
done | tee -a $logfname

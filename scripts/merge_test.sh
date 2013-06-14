#!/bin/bash

for i in $(seq 1 $(nproc))
do
	MT_NCPUS=$i ./vbpt_merge_test | tee merge_test_log | grep \^nthreads
done


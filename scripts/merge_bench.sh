for tx_keys in 64 256 1024 4096
do
	for threads in $(seq 1 $(nproc))
	do
		echo MT_NCPUS=$threads ./vbpt_merge_mt_test ,,${tx_keys},
		     MT_NCPUS=$threads ./vbpt_merge_mt_test ,,${tx_keys},
	done
done

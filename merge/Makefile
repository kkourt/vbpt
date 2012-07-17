.PHONY: all clean

CC=gcc
CFLAGS=-Wall -O2  -g  -D_GNU_SOURCE -I. -std=c99 #-fprofile-arcs -ftest-coverage
LIBS=-lpthread
hdrs=$(wildcard *.h)

all:  vbpt-test vbpt-merge-test

vbpt.o: vbpt.c $(hdrs)
	$(CC) $(CFLAGS) $< -c -o $@

vbpt_merge.o: vbpt_merge.c $(hdrs)
	$(CC) $(CFLAGS) $< -c -o $@

vbpt_log.o: vbpt_log.c $(hdrs)
	$(CC) $(CFLAGS) $< -c -o $@

vbpt-test: vbpt.c vbpt_gv.c phash.c $(hdrs)
	$(CC) $(CFLAGS) $(LIBS) -lgraph -DVBPT_TEST vbpt.c vbpt_gv.c phash.c -o $@

vbpt-merge-test: vbpt.c vbpt_gv.c phash.c vbpt_merge.c vbpt_log.c $(hdrs)
	$(CC) $(CFLAGS) $(LIBS) -lgraph -DVBPT_MERGE_TEST \
	                        vbpt_merge.c vbpt_log.c vbpt.c vbpt_gv.c phash.c -o $@

vbpt_gv.o: vbpt_gv.c $(hdrs)
	$(CC) $(CFLAGS) $< -c -o $@

%.pdf: %.dot
	 dot -Tpdf $< -o $@

clean: 
	rm -f *.o vbpt-cur-test vbpt-test *.gcno *.gcov *.gcda

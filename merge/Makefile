.PHONY: all clean

include scripts/utils.mk

CC      = gcc
CFLAGS  = -Wall -O2 -g -D_GNU_SOURCE -I. -std=c99 #-fprofile-arcs -ftest-coverage
#CFLAGS += $(EXTRA_WARNINGS)
LIBS=-lpthread
hdrs=$(wildcard *.h)

all:  vbpt-test vbpt_merge_test

vbpt-test.o: vbpt.c $(hdrs)
	$(CC) $(CFLAGS) -DVBPT_TEST $< -c -o $@

vbpt-test: vbpt_gv.o vbpt-test.o ver.o phash.o
	$(CC) $(CFLAGS) $(LIBS) -lgraph $^ -o $@

vbpt_merge_test: vbpt_merge_test.o vbpt_merge.o vbpt_log.o vbpt.o ver.o phash.o
	$(CC) $(CFLAGS) $(LIBS) -lgraph -DVBPT_MERGE_TEST $^ -o $@

%.o: %.c $(hdrs)
	$(CC) $(CFLAGS) $< -c -o $@

%.s: %.c
	$(CC) $(CFLAGS)-S -fverbose-asm $<
%.i: %.c
	$(CC)  $(CFLAGS) -E $< | indent -kr > $@

%.pdf: %.dot
	 dot -Tpdf $< -o $@

clean: 
	rm -f *.o vbpt_merge-test vbpt-cur-test vbpt-test *.gcno *.gcov *.gcda *.i

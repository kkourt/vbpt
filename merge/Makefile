.PHONY: all clean

include scripts/utils.mk

CC      = gcc
CFLAGS  = -Wall -O2 -g -D_GNU_SOURCE -I. -std=c99 #-DNDEBUG #-fprofile-arcs -ftest-coverage
#CFLAGS += -Wconversion
#CFLAGS += $(EXTRA_WARNINGS)
LIBS=-lpthread
hdrs=$(wildcard *.h)
vbpt_objs  = vbpt_merge.o vbpt_log.o vbpt.o ver.o phash.o mt_lib.o
vbpt_tests = vbpt-test vbpt_merge_test ver_test vbpt_file_test
fbenches   = fbench-nofiles fbench-sepfiles fbench-samefile fbench-vbpt

# profiling
# XXX: Does not seem to be working correctly
#CFLAGS    += -pg

all:  $(vbpt_tests) $(fbenches)

vbpt-test.o: vbpt.c $(hdrs)
	$(CC) $(CFLAGS) -DVBPT_TEST $< -c -o $@

vbpt-test: vbpt-test.o ver.o vbpt_log.o phash.o
	$(CC) $(CFLAGS) $^ -o $@ $(LIBS)

vbpt_file.o: vbpt_file.c $(hdrs)
	$(CC) $(CFLAGS) $< -c -o $@

vbpt_file_test.o: vbpt_file.c $(hdrs)
	$(CC) $(CFLAGS) -DVBPT_FILE_TEST $< -c -o $@

vbpt_file_test: vbpt_file_test.o $(vbpt_objs)
	$(CC) $(CFLAGS) $^ -o $@ $(LIBS)

vbpt_merge_test: vbpt_merge_test.o $(vbpt_objs)
	$(CC) $(CFLAGS) -DVBPT_MERGE_TEST $^ -o $@ $(LIBS)

ver_test: ver_test.c ver.c $(hdrs)
	$(CC) $(CFLAGS) ver_test.c ver.c -o ver_test $(LIBS)

fbench-samefile.o: fbench.c
	$(CC) $(CFLAGS) -DSAME_FILE $< -lpthread -c -o $@

fbench-sepfiles.o: fbench.c
	$(CC) $(CFLAGS) -DSEP_FILES $< -lpthread -c -o $@

fbench-nofiles.o: fbench.c
	$(CC) $(CFLAGS) -DNO_FILES $< -lpthread -c -o $@

fbench-vbpt.o: fbench.c
	$(CC) $(CFLAGS) -DVBPT_FILE $< -lpthread -c -o $@

%: %.o $(vbpt_objs) vbpt_file.o
	$(CC) $(CFLAGS) $^ -o $@ $(LIBS)

%.o: %.c $(hdrs)
	$(CC) $(CFLAGS) $< -c -o $@
%.s: %.c
	$(CC) $(CFLAGS)-S -fverbose-asm $<
%.i: %.c
	$(CC)  $(CFLAGS) -E $< | indent -kr > $@

%.pdf: %.dot
	 dot -Tpdf $< -o $@

clean:
	rm -f *.o *.gcno *.gcov *.gcda *.i $(vbpt_tests) $(fbenches)

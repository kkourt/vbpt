.PHONY: all clean

include scripts/utils.mk

USE_TCMALLOC  = 1
USE_LOG_RANGE = 1

CC         = gcc
CFLAGS     = -Wall -O2 -g -D_GNU_SOURCE -I. -std=c99 #-fprofile-arcs -ftest-coverage
CFLAGS    += -DNDEBUG
#CFLAGS   += -Wconversion
#CFLAGS   += $(EXTRA_WARNINGS)
LIBS       = -lpthread
hdrs       = $(wildcard *.h)
vbpt_objs  = vbpt_merge.o vbpt.o ver.o phash.o mt_lib.o vbpt_mm.o vbpt_stats.o vbpt_mtree.o vbpt_kv.o
vbpt_tests = vbpt_file_test vbpt_merge_serial_test vbpt_merge_mt_test
fbenches   = fbench-nofiles fbench-sepfiles fbench-samefile fbench-vbpt
tbenches   = tbench-vbpt
progs      = ver_test vbpt-test $(vbpt_tests) $(fbenches) $(tbenches)
vbpt_fobjs = vbpt.o

ifeq (1,$(USE_TCMALLOC))
	CFLAGS    += -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -ltcmalloc
	# check for custom installation of gperftools
	GPERFDIR   = /home/netos/tools/akourtis/gperftools/install
	LDFLAGS    = -ltcmalloc
	ifeq ($(strip $(wildcard $(GPERFDIR))),$(GPERFDIR))
		LDFLAGS += -L$(GPERFDIR)/lib -Xlinker -rpath=$(GPERFDIR)/lib
	endif
endif

ifeq  (1,$(USE_LOG_RANGE))
	CFLAGS   += -DVBPT_LOG_RANGE
	vbpt_log  = vbpt_log_range.o
else
	CFLAGS   += -DVBPT_LOG_PHASH
	vbpt_log  = vbpt_log.o
endif

vbpt_objs    += $(vbpt_log)

# profiling
# XXX: Does not seem to be working correctly
#CFLAGS    += -pg

all: $(progs)

vbpt-test.o: vbpt.c $(hdrs)
	$(CC) $(CFLAGS) -DVBPT_TEST $< -c -o $@

vbpt-test: vbpt-test.o ver.o phash.o vbpt_mm.o $(vbpt_log)
	$(CC) $(LDFLAGS) $^ -o $@ $(LIBS)

vbpt_file_test.o: vbpt_file.c $(hdrs)
	$(CC) $(CFLAGS) -DVBPT_FILE_TEST $< -c -o $@

ver_test: ver_test.c ver.c $(hdrs)
	$(CC) $(CFLAGS) $(LDFLAGS) ver_test.c ver.c -o ver_test $(LIBS)

fbench-samefile.o: fbench.c $(hdrs)
	$(CC) $(CFLAGS) -DSAME_FILE $< -lpthread -c -o $@

fbench-sepfiles.o: fbench.c $(hdrs)
	$(CC) $(CFLAGS) -DSEP_FILES $< -lpthread -c -o $@

fbench-nofiles.o: fbench.c $(hdrs)
	$(CC) $(CFLAGS) -DNO_FILES $< -lpthread -c -o $@

fbench-vbpt.o: fbench.c $(hdrs)
	$(CC) $(CFLAGS) -DVBPT_FILE $< -lpthread -c -o $@

tbench-vbpt.o: tbench.c $(hdrs)
	$(CC) $(CFLAGS) $< -lpthread -c -o $@

%.o: %.c $(hdrs)
	$(CC) $(CFLAGS) $< -c -o $@
%.s: %.c
	$(CC) $(CFLAGS)-S -fverbose-asm $<
%.i: %.c
	$(CC)  $(CFLAGS) -E $< | indent -kr > $@

$(vbpt_tests): % : %.o $(vbpt_objs)
	$(CC) $(LDFLAGS) $^ $(LIBS) -o $@

$(fbenches): %  : %.o $(vbpt_objs) vbpt_file.o
	$(CC) $(LDFLAGS) $^ $(LIBS) -o $@

$(tbenches): %  : %.o $(vbpt_objs)
	$(CC) $(LDFLAGS) $^ $(LIBS) -o $@

%.pdf: %.dot
	 dot -Tpdf $< -o $@

clean:
	rm -f *.o *.gcno *.gcov *.gcda *.i $(progs)

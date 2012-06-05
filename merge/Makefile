.PHONY: all clean

CC=gcc
CFLAGS=-Wall -O2  -g  -D_GNU_SOURCE -I. -std=c99
LIBS=-lpthread
hdrs=$(wildcard *.h)

all:  vbpt.o vbpt_gv.o vbpt-test

vbpt.o: vbpt.c $(hdrs)
	$(CC) $(CFLAGS) $< -c -o $@

vbpt-test: vbpt.c vbpt_gv.c $(hdrs)
	$(CC) $(CFLAGS) $(LIBS) -lgraph -DVBPT_TEST vbpt.c vbpt_gv.c -o $@

vbpt_gv.o: vbpt_gv.c $(hdrs)
	$(CC) $(CFLAGS) $< -c -o $@

clean: 
	rm *.o

.PHONY: all clean

CC=gcc
CFLAGS=-Wall -O2  -g  -D_GNU_SOURCE -I. -std=c99
hdrs=$(wildcard *.h)

all:  vbpt.o vbpt_gv.o

vbpt.o: vbpt.c $(hdrs)
	$(CC) $(CFLAGS) $< -c -o $@

vbpt_gv.o: vbpt_gv.c $(hdrs)
	$(CC) $(CFLAGS) $< -c -o $@

clean: 
	rm *.o

/*
 * mt_lib.c -- Parse MT options
 *
 * Copyright (C) 2007-2011, Computing Systems Laboratory (CSLab), NTUA
 * Copyright (C) 2007-2011, Kornilios Kourtis
 * All rights reserved.
 *
 * This file is distributed under the BSD License. See LICENSE.txt for details.
 */
#include <stdlib.h>
#include <stdio.h>
#include <sched.h>
#include <string.h>

#include "mt_lib.h"

#define MT_CONF  "MT_CONF"
#define MT_NCPUS "MT_NCPUS"

#define MIN(a,b) ((a<b) ? a : b)

void setaffinity_oncpu(unsigned int cpu)
{
	cpu_set_t cpu_mask;
	int err;

	CPU_ZERO(&cpu_mask);
	CPU_SET(cpu, &cpu_mask);

	err = sched_setaffinity(0, sizeof(cpu_set_t), &cpu_mask);
	if (err) {
		perror("sched_setaffinity");
		exit(1);
	}
}

static long parse_int(char *s)
{
	long ret;
	char *endptr;

	ret = strtol(s, &endptr, 10);
	if (*endptr != '\0') {
		printf("parse error: '%s' is not a number\n", s);
		exit(1);
	}

	return ret;
}

void
mt_get_options_default(unsigned int *ncpus_ptr, unsigned int **cpus_ptr)
{
	cpu_set_t mask;
	int err = sched_getaffinity(0, sizeof(cpu_set_t), &mask);
	if (err < 0) {
		perror("sched_getaffinity");
		exit(1);
	}

	char *e = getenv(MT_NCPUS);
	unsigned int ncpus, ncpus_max = CPU_COUNT(&mask);
	if (!e) {
		ncpus = ncpus_max;
		//printf("%s empty: using all %u cpus\n", MT_NCPUS, ncpus);
	} else {
		ncpus = MIN(parse_int(e), ncpus_max);
		//printf("using %u cpus\n", ncpus);
	}

	unsigned int *cpus = malloc(sizeof(unsigned int)*ncpus);
	if (!cpus) {
		perror("malloc");
		exit(1);
	}


	unsigned int cpu_cur = 0;
	for (unsigned int i=0; i<ncpus; i++) {
		while (1) {
			unsigned int c = cpu_cur++;
			if (CPU_ISSET(c, &mask)) {
				cpus[i] = c;
				break;
			}
		}
	}

	*cpus_ptr = cpus;
	*ncpus_ptr = ncpus;
}

void mt_get_options(unsigned int *nr_cpus, unsigned int **cpus)
{
	unsigned int i;
	char *s,*e,*token;

	e = getenv(MT_CONF);
	if (!e) {
		mt_get_options_default(nr_cpus, cpus);
		return;
	}

	s = malloc(strlen(e)+1);
	if (!s) {
		fprintf(stderr, "mt_get_options: malloc failed\n");
		exit(1);
	}

	memcpy(s, e, strlen(e)+1);
	*nr_cpus = 1;
	for (i = 0; i < strlen(s); i++) {
		if (s[i] == ',') {
			*nr_cpus = *nr_cpus+1;
		}
	}

	i = 0;
	*cpus = malloc(sizeof(unsigned int)*(*nr_cpus));
	if ( !(*cpus) ){
		fprintf(stderr, "mt_get_options: malloc failed\n");
		exit(1);
	}

	token = strtok(s, ",");
	do {
		(*cpus)[i++] = (unsigned int)parse_int(token);
	} while ( (token = strtok(NULL, ",")) );

	free(s);
	return;
}

/*
 * Copyright (c) 2012-2015, ETH Zurich.
 *
 * Released under a dual BSD 3-clause/GPL 2 license. When using or
 * redistributing this file, you may do so under either license.
 *
 * http://opensource.org/licenses/BSD-3-Clause
 * http://opensource.org/licenses/GPL-2.0
 */

#include <stdio.h>
#include <inttypes.h>
#include <time.h>

#include "xdist.h"

int main(int argc, const char *argv[])
{
	struct xdist_desc xd = {
		.r_start = 0,
		.r_len   = 128,
		.nr      = 32,
		.seed    = 1
	};

	uint64_t x;
	xdist_for_each(&xd, x) {
		printf("x=%"PRIu64"\n", x);
	}

	return 0;
}

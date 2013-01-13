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

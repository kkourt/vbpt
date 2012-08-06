#include "ver.h"

void
vbpt_log_destroy(vbpt_log_t *log)
{
	assert(false); // shouldn't be called
}

int main(int argc, const char *argv[])
{
	ver_t *v0 = ver_create();
	ver_t *v1 = ver_branch(v0);
	ver_t *v2 = ver_branch(v1);

	assert(ver_ancestor(v0, v0));
	assert(ver_ancestor(v0, v1));
	assert(ver_ancestor(v0, v2));

	assert(ver_ancestor_limit(v0, v1, 1));
	assert(ver_ancestor_limit(v0, v2, 2));
	assert(ver_ancestor_limit(v0, v0, 1));
	assert(!ver_ancestor_limit(v0, v1, 0));

	assert(ver_ancestor_strict_limit(v0, v1, 1));
	assert(ver_ancestor_strict_limit(v0, v2, 2));
	assert(!ver_ancestor_strict_limit(v0, v0, 1));
	assert(!ver_ancestor_strict_limit(v0, v2, 1));

	return 0;
}


#include "refcnt.h"
#include "ver.h"
#include "misc.h"

#define VERS_MM

#if defined(VERS_VERSIONED) && !defined(VERS_MM)
#error "NYI"
#endif


#if defined(VERS_MM)

#if defined(VERS_VERSIONED)
// on free(), update (atomically) ver_seq_max = MAX(ver_seq_max, v->v_seq +1)
// Curently, we don't use thos since we dont free() versions
static uint64_t ver_seq_max = 0;
#endif

static __thread struct {
	ver_t     *vers;      // chain of free versions
	size_t    vers_nr;    // number of free versions
} Ver_mm;
#endif // VERS_MM

ver_t *
ver_mm_alloc(void)
{
	ver_t *ret;
	#if defined(VERS_MM)
	if (Ver_mm.vers_nr == 0) {
		ret = xmalloc(sizeof(*ret));
		#if defined(VERS_VERSIONED)
		ret->v_seq = ver_seq_max;
		#endif
	} else {
		ret = Ver_mm.vers;
		Ver_mm.vers = ret->parent;
		Ver_mm.vers_nr--;
		#if defined(VERS_VERSIONED)
		ret->v_seq++;
		#endif
	}
	#else // !VERS_MM
	ret = xmalloc(sizeof(*ret));
	#endif

	return ret;
}

void
ver_mm_free(ver_t *ver)
{
	#if defined(VERS_MM)
	ver->parent = Ver_mm.vers;
	Ver_mm.vers = ver;
	Ver_mm.vers_nr++;
	#else // !VERS_MM
	free(ver);
	#endif
}

void
ver_debug_init(ver_t *ver)
{
	#ifndef NDEBUG
	static size_t id = 0;
	spinlock_t *lock_ptr = NULL;
	spinlock_t lock;

	if (lock_ptr == NULL) { // XXX: race
		spinlock_init(&lock);
		lock_ptr = &lock;
	}
	spin_lock(lock_ptr);
	ver->v_id = id++;
	spin_unlock(lock_ptr);
	#endif
}

void
ver_chain_print(ver_t *ver)
{
	printf("=== Printing chain ============================== %p\n", ver);
	while (ver != NULL) {
		printf(" %s\n", ver_fullstr(ver));
		ver = ver->parent;
	}
	printf("=========================================================\n");
}

/**
 * see ver_join()
 *
 * We can avoid the O(n^2) thing by keeping a hash table for all the versions on
 * the global path, so that we can iterate the pver path and check membership.
 */
ver_t *
ver_join_slow(ver_t *gver, ver_t *pver, ver_t **prev_pver,
              uint16_t *gdist, uint16_t *pdist)
{
	ver_t *gv = gver;
	for (unsigned gv_i = 0; gv_i < VER_JOIN_LIMIT; gv_i++) {
		ver_t *pv = pver;
		for (unsigned pv_i=0 ; pv_i < VER_JOIN_LIMIT; pv_i++) {
			if (pv->parent == gv->parent) {
				if (prev_pver)
					*prev_pver = pv;
				assert(pv->parent != NULL);
				*gdist = gv_i + 1;
				*pdist = pv_i + 1;
				return pv->parent;
			}

			if ((pv = pv->parent) == NULL)
				break;
		}
		if ((gv = gv->parent) == NULL)
			break;
	}

	// gdist, pdist won't get used if VER_JOIN_FAIL is returned, but the
	// compiler can't seem to be able to figure that out and complains about
	// uninitialized values
	*gdist = *pdist = ~0;
	return VER_JOIN_FAIL;
}


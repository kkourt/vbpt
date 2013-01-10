#ifndef VER_H__
#define VER_H__

#include <stdbool.h>
#include <inttypes.h>

#include "refcnt.h"
#include "container_of.h"
#include "misc.h"

#include "vbpt_stats.h"

// Versions form a tree (partial order) as defined by the ->parent pointer.
// This partial order is queried when performing merge operations.

// We use a ->parent pointer to track the partial order to enable for
// concurrency (instead of using children pointers)

// For implementation convenience we add the operation log to the version, so we
// need to include the appropriate header
#include "vbpt_log_internal.h"

/**
 * Garbage collecting versions
 *
 * versions are referenced by:
 *  - other versions (ver_t's ->parent)
 *  - trees          (vbpt_tree_t's ->ver)
 *  - tree nodes     (vbpt_hdr_t's ->ver)
 *
 *     ...
 *      |
 *      o
 *      |
 *      o Vj
 *      |\
 *      . .
 *      .  .
 *      .   .
 *      |    \
 *   Vg o     o Vp
 *
 * Since children point to parents, we can't use typical reference counting,
 * because reference counts will never reach zero. Instead, we need to collect
 * versions which form a chain that reaches the end (NULL) and all refcounts in
 * the chain are 1.
 *
 * The above is enabled by the way we query the partial order when doing merges.
 * As an optimization, we only check until the Vj (i.e., we only traverse the
 * Vg->Vj paths and Vp->Vj paths), and if we don't find the version we are
 * looking for, we assume that it is before Vj (see ver_join() and
 * ver_leq_limit()). So, if no branches exist in a chain that reaches NULL, we
 * know that the nodes in the chain won't be traversed by the partial order
 * queries -- i.e., they can be removed by the tree. We term versions that are
 * not going to be traversed by the partial order queries stale versions.
 *
 * One (i.e., me in several different occasions) might think that this enable us
 * to not count references from nodes.  However, this is not the case. The
 * problem is that if a node points to a version (vx) that is free()d, it is
 * possible that a new version (vy) will be allocated on the same address, which
 * means that nodes with version vx, will be mistakely assumed to be nodes with
 * version vy. So, we can remove stale versions from the tree, but we can't
 * deallocate them, because they are still referenced in individual nodes of the
 * tree.
 *
 * A possible solution would be to "version" the versions with a unique
 * identifier, and add it to the version node references, so that we can quickly
 * check if this a version that was reallocated. Note, however, that in this
 * case it is not safe to dereference version references in nodes. The only
 * valid operation is to compare them with a version from the version tree.
 *
 * Alternatively, we use two reference counts: one for keeping a version to the
 * version tree (rfcnt_children), and one for reclaiming the version
 * (rfcnt_total). We use rfcnt_children to remove a version from the tree, and
 * rfcnt_total to reclaim the version. Removing versions eagerly from the tree
 * avoids having to search long version chains.
 *
 * There are two options on how the two reference counts are kept: (a)
 * rfcnt_total includes references from children and (b) rfcnt_total does not
 * includes references from children. Option (b) has the advantage that when you
 * find a chain of stale versions you can just remove them by just unlinking
 * them from the tree. A disadvantage of (b) is that it needs special handling
 * in case ->rfcnt_total becomes zero before the node was detected as stale
 * (i.e., it's still a part of the chain), since in that case it can't be
 * reclaimed without being removed from the chain. Option (a) does not have this
 * problem, but it requires traversing the list of stale versions and decrement
 * their refcount, which raises concurrency issues. For simplicity, we follow
 * (a) and require ver_tree_gc() to be protetected from reentrancy.
 *
 * Finally, we need to ensure that no user will try to branch off a new version
 * from stale versions. To achieve this property, we let the user define a
 * frontier above which it is guaranteed that no new version will be branched.
 * This frontier is defined by "pinning" versions. IOW, when a user pins a
 * version using ver_pin(), the user guarantees that no version reachable via
 * the ->parent pointer of the pinned version will be used as a branch point.
 *
 * The typical scenario for this is when the user maintains a "blessed" version,
 * and all branches happen on this version (or in the case of nesting, on one
 * of its children).
 *
 * NOTE: The following are currently deprecated, since we end up serializing
 * ver_tree_gc() anyway.
 * There are two possible implementations for pinning a version:
 *  1. We can just run ver_tree_gc() when a version is pinned, but we need to
 *     make sure that no other ver_tree_gc() instance will run starting from an
 *     ancestor of the pinned version.
 *  2. If we can't guarantee the above property, we can increase the
 *     rfcnt_children for the pinned version, so that it won't dissapear by
 *     another run of the garbage collector. This might be usefull, for example,
 *     if we want to run the collector after a rebase(). Obviously, we also need
 *     to make sure that the rfcnt_children is decreased for the previous pinned
 *     version.
 * For now, we follow the first approach.
 *
 * To avoid keeping old versions around for too long, we could apply another
 * optimization where we update stale version references to the pinned version.
 * We could for example, keep a set of stale versions and check against it when
 * iterating the tree. Another possible optimization would be to  set ->ver ==
 * NULL for nodes that have the same version as their parent on the tree.
 */

//#define VERS_VERSIONED
//#define VREFS_ALWAYS_VALID // for debugging purposes

/**
 * Interface overview
 *
 * ver_create():    create a new version out of nothing
 * ver_branch():    branch a version from another version
 *
 * ver_getref():    get a version reference
 * ver_putref():    put a version reference
 *
 * ver_rebase():    change parent
 *
 * ver_pin():       pin a version
 *
 * ver_eq():        check two versions for equality
 * ver_leq_limit(): query partial order
 * ver_join():      find join point
 *
 */

struct ver {
	struct ver *parent;
	refcnt_t   rfcnt_children;
	refcnt_t   rfcnt_total;
	#ifndef NDEBUG
	size_t     v_id;
	#endif
	vbpt_log_t v_log;
	#if defined (VERS_VERSIONED)
	uint64_t   v_seq;
	#endif
};
typedef struct ver ver_t;

void ver_debug_init(ver_t *ver);

static inline void
ver_init__(ver_t *ver)
{
	refcnt_init(&ver->rfcnt_total, 1);
	refcnt_init(&ver->rfcnt_children, 0);
	#ifndef NDEBUG
	ver_debug_init(ver);
	#endif
	// XXX: ugly but useful
	ver->v_log.state = VBPT_LOG_UNINITIALIZED;
}

static inline char *
ver_str(ver_t *ver)
{
	#define VERSTR_BUFF_SIZE 128
	#define VERSTR_BUFFS_NR   16
	static int i=0;
	static char buff_arr[VERSTR_BUFFS_NR][VERSTR_BUFF_SIZE];
	char *buff = buff_arr[i++ % VERSTR_BUFFS_NR];
	#ifndef NDEBUG
	/*
	snprintf(buff, VERSTR_BUFF_SIZE,
	         " [%p: ver:%3zd rfcnt_children:%3u rfcnt_total:%3u] ",
		 ver,
	         ver->v_id,
		 refcnt_get(&ver->rfcnt_children),
		 refcnt_get(&ver->rfcnt_total));
	*/
	snprintf(buff, VERSTR_BUFF_SIZE, " [ver:%3zd] ", ver->v_id);
	#else
	snprintf(buff, VERSTR_BUFF_SIZE, " (ver:%p ) ", ver);
	#endif
	return buff;
	#undef VERSTR_BUFF_SIZE
	#undef VERSTR_BUFFS_NR
}

static inline char *
ver_fullstr(ver_t *ver)
{
	#define VERSTR_BUFF_SIZE 128
	#define VERSTR_BUFFS_NR   16
	static int i=0;
	static char buff_arr[VERSTR_BUFFS_NR][VERSTR_BUFF_SIZE];
	char *buff = buff_arr[i++ % VERSTR_BUFFS_NR];
	#ifndef NDEBUG
	snprintf(buff, VERSTR_BUFF_SIZE,
	         " [%p: ver:%3zd rfcnt_children:%3u rfcnt_total:%3u] ",
		 ver,
	         ver->v_id,
		 refcnt_get(&ver->rfcnt_children),
		 refcnt_get(&ver->rfcnt_total));
	#else
	snprintf(buff, VERSTR_BUFF_SIZE,
	         " [%12p: rfcnt_children:%3u rfcnt_total:%3u] ",
		 ver,
		 refcnt_get(&ver->rfcnt_children),
		 refcnt_get(&ver->rfcnt_total));
	#endif
	return buff;
	#undef VERSTR_BUFF_SIZE
	#undef VERSTR_BUFFS_NR
}

static inline void
ver_path_print(ver_t *v, FILE *fp)
{
	fprintf(fp, "ver path: ");
	do {
		fprintf(fp, "%s ->", ver_str(v));
	} while ((v = v->parent) != NULL);
	fprintf(fp, "NULL\n");
}

static inline ver_t *
refcnt_total2ver(refcnt_t *rcnt)
{
	return container_of(rcnt, ver_t, rfcnt_total);
}

/**
 * get a node reference
 */
static inline ver_t *
ver_getref(ver_t *ver)
{
	refcnt_inc(&ver->rfcnt_total);
	return ver;
}

static void ver_release(refcnt_t *);

/**
 * release a node reference
 */
static inline void
ver_putref(ver_t *ver)
{
	refcnt_dec(&ver->rfcnt_total, ver_release);
}


/**
 * release a version
 */
static void
ver_release(refcnt_t *refcnt)
{
	ver_t *ver = refcnt_total2ver(refcnt);
	#if 0
	// this is special case where a version is no longer references in a
	// tree, but is a part of the version tree. I think the best solution is
	// to have the tree grab a reference. Want to test the current gc sceme
	// before applying this change though, so for know just a warning.
	if (refcnt_get(&ver->rfcnt_children) != 0) {
		//assert(0 && "FIXME: need to handle this case");
		//printf("We are gonna leak one version\n");
		return;
	}
	#endif

	ver_t *parent = ver->parent;
	if (parent != NULL) {
		refcnt_dec__(&parent->rfcnt_children);
		refcnt_dec(&parent->rfcnt_total, ver_release);
	}
	if (ver->v_log.state != VBPT_LOG_UNINITIALIZED)
		vbpt_log_destroy(&ver->v_log);
	free(ver);
}

/* create a new version */
static inline ver_t *
ver_create(void)
{
	ver_t *ret = xmalloc(sizeof(ver_t));
	ret->parent = NULL;
	ver_init__(ret);
	return ret;
}

void ver_chain_print(ver_t *ver);

/**
 * garbage collect the tree from @ver's parent and above
 *  Find the longer chain that ends at NULL and all nodes have a rfcnt_children
 *  of 1. This chain can be detached from the version tree.
 *
 * This function is not reentrant. The caller needs to make sure that it won't
 * be run on the same chain.
 */
static void inline
ver_tree_gc(ver_t *ver)
{
	//VBPT_START_TIMER(ver_tree_gc);
	ver_t *ver_p = ver->parent;
	#if defined(VBPT_STATS)
	uint64_t count = 0;
	#endif
	while (true) {
		// reached bottom
		if (ver_p == NULL)
			break;

		uint32_t children;
		#if 0
		// try to get a the refcount. If it's not possible somebody else
		// is using the reference count lock (is that possible?)
		if (!refcnt_try_get(&ver_p->rfcnt_children, &children)) {
			VBPT_STOP_TIMER(ver_tree_gc);
			goto end;
		}
		#else
		children = refcnt_(&ver_p->rfcnt_children);
		#endif
		assert(children > 0);

		// found a branch, reset the head of the chain
		if (children > 1)
			ver = ver_p;

		ver_p = ver_p->parent;
		#if defined(VBPT_STATS)
		count++;
		#endif
	}
	//VBPT_STOP_TIMER(ver_tree_gc);
	//VBPT_XCNT_ADD(ver_tree_gc_iters, count);
	//tmsg("count=%lu ver->parent=%p\n", count, ver->parent);

	// poison ->parent pointers of stale versions
	ver_t *v = ver->parent;
	while (v != NULL) {
		ver_t *tmp = v->parent;
		v->parent = NULL;
		refcnt_dec__(&v->rfcnt_children);
		refcnt_dec(&v->rfcnt_total, ver_release);
		v = tmp;
	}

	// everything below ver->parent is stale, remove them from the tree
	// ASSUMPTION: this assignment is atomic.
	ver->parent = NULL;
}


/**
 * pin a version from the tree:
 *  just run the garbage collector
 *  See comment at begining of file for details
 */
static inline void
ver_pin(ver_t *pinned_new, ver_t *pinned_old)
{
	refcnt_inc(&pinned_new->rfcnt_total);
	if (pinned_old) {
		refcnt_dec(&pinned_old->rfcnt_total, ver_release);
	}
	//ver_tree_gc(pinned_new);
}

static inline void
ver_unpin(ver_t *ver)
{
	refcnt_dec(&ver->rfcnt_total, ver_release);
}

// grab a child reference
static inline void
ver_get_child_ref(ver_t *ver)
{
	refcnt_inc__(&ver->rfcnt_children); // do not check if it's zero
	refcnt_inc(&ver->rfcnt_total);
}

static inline void
ver_put_child_ref(ver_t *ver)
{
	refcnt_dec__(&ver->rfcnt_children);
	refcnt_dec(&ver->rfcnt_total, ver_release);
}

/**
 * set parent without checking for previous parent
 */
static inline void
ver_setparent__(ver_t *v, ver_t *parent)
{
	ver_get_child_ref(parent);
	v->parent = parent;
}

/**
 * prepare a version rebase.  at the new parent won't be removed from the
 * version chain under our nose
 */
static inline void
ver_rebase_prepare(ver_t *new_parent)
{
	ver_get_child_ref(new_parent);
}

static inline void
ver_rebase_commit(ver_t *ver, ver_t *new_parent)
{
	if (ver->parent)
		ver_put_child_ref(ver->parent);
	ver->parent = new_parent;
}

static inline void
ver_rebase_abort(ver_t *new_parent)
{
	ver_put_child_ref(new_parent);
}

/**
 * rebase: set a new parent to a version.
 *  If previous parent is not NULL, refcount will be decreased
 *  Will get a new referece of @new_parent
 */
static inline void __attribute__((deprecated))
ver_rebase(ver_t *ver, ver_t *new_parent)
{
	if (ver->parent) {
		ver_put_child_ref(ver->parent);
		//ver_tree_gc(ver);
	}
	ver_setparent__(ver, new_parent);
}

/**
 * detach: detach version from the chain
 *   sets parent to NULL
 */
static inline void
ver_detach(ver_t *ver)
{
	if (ver->parent) {
		ver_put_child_ref(ver->parent);
		//ver_tree_gc(ver);
	}
	ver->parent = NULL;
}

/* branch (i.e., fork) a version */
static inline ver_t *
ver_branch(ver_t *parent)
{
	/* allocate and initialize new version */
	ver_t *ret = xmalloc(sizeof(ver_t));
	ver_init__(ret);
	/* increase the reference count of the parent */
	ver_setparent__(ret, parent);
	return ret;
}

/*
 * versions form a partial order, based on the version tree that arises from
 * ->parent: v1 < v2 iff v1 is an ancestor of v2
 */

static inline bool
ver_eq(ver_t *ver1, ver_t *ver2)
{
	return ver1 == ver2;
}

/**
 * check if ver1 <= ver2 -- i.e., if ver1 is ancestor of ver2 or ver1 == ver2
 *  moves upwards (to parents) from @ver2, until it encounters @ver1 or NULL
 */
static inline bool
ver_leq(ver_t *ver1, ver_t *ver2)
{
	for (ver_t *v = ver2; v != NULL; v = v->parent)
		if (v == ver1)
			return true;
	return false;
}

/**
 * check if @v_p is an ancestor of v_ch.
 *   if @v_p == @v_ch the function returns true
 */
static inline bool
ver_ancestor(ver_t *v_p, ver_t *v_ch)
{
	for (ver_t *v = v_ch; v != NULL; v = v->parent) {
		if (v == v_p)
			return true;
	}
	return false;
}

/**
 * check if @v_p is an ancestor of @v_ch, assuming they have no more of @max_d
 * distance.
 *  if @v_p == @v_ch the function returns true
 */
static inline bool
ver_ancestor_limit(ver_t *v_p, ver_t *v_ch, uint16_t max_d)
{
	ver_t *v = v_ch;
	for (uint16_t i=0; v != NULL && i < max_d + 1; v = v->parent, i++) {
		if (v == v_p)
			return true;
	}
	return false;
}

/**
 * check if @v_p is a strict ancestor of @v_ch
 *   if @v_p == @v_ch, the function returns false
 */
static inline bool
ver_ancestor_strict(ver_t *v_p, ver_t *v_ch)
{
	for (ver_t *v = v_ch->parent; v != NULL; v = v->parent) {
		if (v == v_p)
			return true;
	}
	return false;
}

/* check if @v_p is an ancestor of @v_ch, assuming they have no more of @max_d
 * distance.
 *  if @v_p == @v_ch the function returns false
 */
static inline bool
ver_ancestor_strict_limit(ver_t *v_p, ver_t *v_ch, uint16_t max_d)
{
	if (v_p == v_ch)
		return false;
	ver_t *v = v_ch->parent;
	for (uint16_t i=0; v != NULL && i < max_d; v = v->parent, i++) {
		if (v == v_p)
			return true;
	}
	return false;
}

#define VER_JOIN_FAIL ((ver_t *)(~((uintptr_t)0)))
#define VER_JOIN_LIMIT 64

ver_t *
ver_join_slow(ver_t *gver, ver_t *pver, ver_t **prev_pver,
              uint16_t *gdist, uint16_t *pdist);

/**
 * find the join point (largest common ancestor) of two versions
 *
 * The main purpose of this is to find the common ancestor of two versions.
 * Given our model, however, it gets a bit more complicated than that.
 * We assume that the join is performed between two versions:
 *  - @gver: the current version of the object (read-only/globally viewable)
 *  - @pver: a diverged version, private to the transaction
 *
 * The actual join operation (find the common ancestor) is symmetric, but we
 * neeed to distinguish between the two versions because after we (possibly)
 * merge the two versions, we need to modify the version tree (see merge
 * operation), which requires more information than the common ancestor.
 * Specifically, we need to move @pver under @gver, so we need last node in the
 * path from @pver to the common ancestor (@prev_v). This node is returned in
 * @prev_v, if @prev_v is not NULL.
 *
 *        (join_v)    <--- return value
 *       /        \
 *  (prev_v)      ...
 *     |           |
 *    ...        (gver)
 *     |
 *   (pver)
 *
 * Furthermore, another useful property for the merge algorithm is to now the
 * distance of each version from the join point. This allows to have more
 * efficient checks on whether a version found in the tree is before or after
 * the join point. The distance of the join point from @gver (@pver) is returned
 * in @gdist (@pdist).
 */
static inline ver_t *
ver_join(ver_t *gver, ver_t *pver, ver_t **prev_v, uint16_t *gdist, uint16_t *pdist)
{
	/* this is the most common case, do it first */
	if (gver->parent == pver->parent) {
		assert(pver->parent != NULL);
		if (prev_v)
			*prev_v = pver;
		*gdist = *pdist = 1;
		return pver->parent;
	}
	return ver_join_slow(gver, pver, prev_v, gdist, pdist);

}

/**
 *  check if a version chain at @start and ending at @end has branches.
 *  returns false if all refcounts between @start and @end are 1
 *  aimed for debugging
 */
static inline bool
ver_chain_has_branch(ver_t *tail, ver_t *head)
{
	ver_t *v = tail;
	while (true) {
		if (refcnt_get(&v->rfcnt_children) > 1)
			return true;
		if (v == head)
			break;
		assert(v != NULL);
		v = v->parent;
	}
	return false;
}


static inline ver_t *
ver_parent(ver_t *ver)
{
	return ver->parent;
}

/*
 * Log helpers
 */
static inline ver_t *
vbpt_log2ver(vbpt_log_t *log)
{
	return container_of(log, ver_t, v_log);
}

/**
 * return the parent log
 *  Assumption: this is a log embedded in a version
 */
static inline vbpt_log_t *
vbpt_log_parent(vbpt_log_t *log)
{
	ver_t *ver = vbpt_log2ver(log);
	ver_t *ver_p = ver_parent(ver);
	return (ver_p != NULL) ? &ver_p->v_log : NULL;
}

/**
 * Version references.
 */

// a reference to a version
struct vref {
	struct ver *ver_;
	#if defined(VERS_VERSIONED)
	uint64_t    ver_seq;
	#endif

	#ifndef NDEBUG
	size_t      vid;
	#endif
};
typedef struct vref vref_t;

static inline vref_t
vref_get(ver_t *ver)
{
	vref_t ret;

	#if defined(VERS_VERSIONED)
	ret.ver_    = ver;
	ret.ver_seq = ver->v_seq;
	#else
	ret.ver_    = ver_getref(ver);
	#endif

	#if !defined(NDEBUG)
	ret.vid = ver->v_id;
	#endif
	return ret;
}

// the difference with vref_get() is that if VERS_VERSIONED is _not_ defined, we
// dont't increase the refcnt of ver.
static inline vref_t
vref_get__(ver_t *ver)
{
	vref_t ret;

	#if defined(VERS_VERSIONED)
	ret.ver_    = ver;
	ret.ver_seq = ver->v_seq;
	#else
	ret.ver_    = ver;
	#endif

	#if !defined(NDEBUG)
	ret.vid = ver->v_id;
	#endif
	return ret;
}

static inline void
vref_put(vref_t vref)
{
	#if !defined(VERS_VERSIONED)
	ver_putref(vref.ver_);
	#endif
}

static inline bool
vref_eq(vref_t vref1, vref_t vref2)
{
	bool ret = (vref1.ver_ == vref2.ver_);
	#if defined(VERS_VERSIONED)
	ret = ret && (vref1.ver_seq == vref2.ver_seq);
	#endif
	return ret;
}

static inline bool
vref_eqver(vref_t vref, ver_t *ver)
{
	bool ret = (vref.ver_ == ver);
	#if defined (VERS_VERSIONED)
	ret = ret && (vref.ver_seq == ver->v_seq);
	#endif
	return ret;
}

// return true only if we surely know that the version is valid
static inline bool
vref_valid(vref_t ver)
{
#if defined(VREFS_ALWAYS_VALID)
	return true;
#else
	return false;
#endif
}

static inline char *
vref_str(vref_t vref)
{
	#define VREFSTR_BUFF_SIZE 128
	#define VREFSTR_BUFFS_NR   16
	static int i=0;
	static char buff_arr[VREFSTR_BUFFS_NR][VREFSTR_BUFF_SIZE];
	char *buff = buff_arr[i++ % VREFSTR_BUFFS_NR];
	#ifndef NDEBUG
	snprintf(buff, VREFSTR_BUFF_SIZE, " [ver:%3zd] ", vref.vid);
	#else
	snprintf(buff, VREFSTR_BUFF_SIZE, " (ver:%p ) ", vref.ver_);
	#endif
	return buff;
	#undef VREFSTR_BUFF_SIZE
	#undef VREFSTR_BUFF_NR
}

/**
 * check if @vref_p is an ancestor of @v_ch, assuming they have no more of
 * @max_d distance.
 *  if @vref_p == @v_ch the function returns true
 */
static inline bool
vref_ancestor_limit(vref_t vref_p, ver_t *v_ch, uint16_t max_d)
{
	ver_t *v = v_ch;
	for (uint16_t i=0; v != NULL && i < max_d + 1; v = v->parent, i++) {
		if (vref_eqver(vref_p, v))
			return true;
	}
	return false;
}

#endif

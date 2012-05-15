#define _GNU_SOURCE /* for pthread_spinlock */

/* pthread spinlock wrappers */
#define spinlock_t       pthread_spinlock_t
#define spinlock_init(x) pthread_spin_init(x, 0)
#define spin_lock(x)     pthread_spin_lock(x)
#define spin_unlock(x)   pthread_spin_unlock(x)

#include <pthread.h>

#include "ccan/list/list.h"

/**
 * Note about nesting (transactions):
 * We try to support nested transactions conceptually, but the implementation is
 * not optimized for deep nesting, and in some cases if the nesting is too deep,
 * we might choose to  fail to keep the algorithms simpler
 * (e.g., see VER_JOIN_LIMIT) */

/**
 * We assume that versions are short-lived -- i.e. they exist only in the
 * context of a transaction. This means that:
 *  - they can be stored in RAM, using pointers/etc (?)
 * Versions need to be garbage collected:
 *  - we keep a reference count for each version
 *  - when a fork happens, the reference count of the forked version is
 *    increased
 *  - the reference count of p is decreased when a a child of p merges back
 *  - versions older than the "blessed" one with zero reference count can
 *    (should) be collected
 */
struct ver {
	struct ver *parent;   // ->parrent == NULL if root of the version tree
	atomic_t   ref_count; // reference count for this version
	void       *vobj      // the (versioned) object for this version
};

static inline ver_t *
ver_parent(ver_t *v)
{
	return v->parent;
}

#define VER_FAILURE (~((uintptr_t)0))
#define VER_JOIN_LIMIT 3

/**
 * The main purpose of this is to find the common ancestor of two versions.
 * Given our object model, however, it gets more complicated than that.
 * We assume that the join is performed between two versions:
 *  - the current version of the mutable object (mver)
 *  - a diverged version, private to the transaction (tver)
 * The actual join operation (find the common ancestor) is symmetric, but we
 * neeed to distinguish between the two versions because after we (possibly)
 * merge the two versions, we need to modify the version tree, which requires
 * more information than the common ancestor.
 */
ver_t *
ver_join(ver_t *v1_orig, ver_t *v2_orig)
{
	/* this is the most common case, do it first */
	if (v1_orig->parent == v2_orig->parent)
		return v1_orig->parent

	int v1_i = 0, v2_i = 0;
	ver_t *v1 = v1_orig;
	ver_t *ret = VER_FAILURE;
	for (;;) {
		/* check current v1 with all v2 */
		ver_t *v2 = v2_orig;
		do {
		}
		if (v1->parent == NULL || v1_i >= VER_JOIN_LIMIT)
			break;
		v1_i++;
	}

	return ret;
}

/**
 * merge mver to tver
 *  @tver: transaction-private version
 *  @mver: mobj's version (no changes are allowed in mver)
 *
 * The merging happens  *in-place* in tver. If successful 0 is returned. If not
 * VER_FAILURE is returned tver is invalid.
 *
 * If successful, new @mver should be @nver's ancestor (which is the whole point
 * of doing the merge: to make it appear as if the changes in tver happened
 * after mver). This has some implications (see below).
 *
 * The merge finds first common ancestor/join point of the two versions vj. In
 * the general case the version tree might look like:
 *
 *        (vj)             (vj)
 *       /    \             |
 *    ...     ...    ==>   ...
 *    /         \           |
 * (tver)    (mver)       (tver)
 *                          |
 *                         ...
 *                          |
 *                        (mver') == (nver)
 *
 * If the versions between vj (common ancestor) and mver have a refcount of 1,
 * we can just move the path (vj) -- (mver) under (tver). If not all refcounts
 * in the path are 1, there is a branched vresion somewhere in the path that is
 * not yet merged and if we move the path the join point might change. OTOH, if
 * we copy the path, we need to change all the versions in the tree nodes (or
 * provide some indirection), so that the versioning comparison will work for
 * future versions.
 */
int
ver_merge(ver_t *tver, ver_t *mver);
{
	ver_t *vj = ver_join(); //
	obj_merge(tver, mver, vj);
}


/**
 * Mutable objects -- pointing to a "blessed" version
 */
struct mobj {
	ver_t  *ver;
	spinlock_t *lock;
}
typedef struct mobj mobj_t;

/**
 * In the general case, A (global) state can be viewed as a set of mutable
 * objects. Each transaction operates on private versions, forked from the
 * versions of the global state.
 *
 * We want to investegate if the global state needs to change atomically with
 * respect to transactions.
 *
 * For example, consider the following scenario:
 *  state = <A0,B0>     # (state consists of 2 objects: A,B)
 *  T1 forks A1 from A0
 *  T1 forks B1 from B0
 *  T2 forks B2 from B0
 *  T1 commits: state = <A1,B1> (trivial update)
 *  T2 forks A2 from A1
 *  T2 tries to commit state <A2,B2>:
 *    - A2 forked from A1 => OK
 *    - B2 forked from B0 => needs rebase
 *
 * In this case, the change is not updated atomically, since T2 obeserves
 * <A1,B0>. However, I don't think this is an issue, since we end up rebasing
 * the changes of T2 so that it would appear (if possible) to happen _after_
 * T1. Maybe we can make a more formal argument based on some kind of
 * monotonicity property.
 *
 * If, however, we want to ensure transaction atomicity, here are some thoughts:
 * If we consider the state consisting of multiple objects, then one way would
 * be to protect the state with locks (e.g., a single lock or one-lock per
 * object), and before starting the transaction, the locks for _all_ the objecs
 * involved in the transaction are taken, their current versions recorded, and
 * the locks released. State would change using the same locking mechanism
 * (e.g., via 2PC),
 * This is problematic for two reasons: i) initializing a transaction requires
 * locking and may cause contention ii) it might not be known beforehand what
 * the objects the transaction will access are. In this case, versions for all
 * objects would be required.
 * An alternative approach would be to create a hierarchy of objects. For
 * example, a (versioned) tree (let's call it A) could hold a mapping between
 * address and location for each object. The location would point to a
 * _specific_ version (immutable) for this object. If we need to update an
 * object (say O), we create another version of O, and a new version of A where
 * O's address points to O's new version. Note that all references to objects
 * must be in terms of addresses in A. In that case, the only mutable state
 * neded is the "blessed" version of A.  This approach is very suitable for a
 * typical file-system design, where the A tree is the tree that maps inodes to
 * files or directories.
 *
 */

/**
 * Transactional objects
 */
struct vtxobj {
	ver_t  *ver;           /* current version */
	vtx_t  *tx;            /* owning transaction */
	mobj_t *mo;            /* corresponding mutable object (blessed version) */
	struct list_node tx_l; /* transaction list node member */
};
typedef struct vtxobj vtxobj_t;

vtxobj_t *
__vtxobj_alloc(void)
{
	vtxobj_t *vtxo = malloc(sizeof(vtxobj_t));
	if (!vtxo) {
		assert(0);
	}
	return vtxo;
}

vtxobj_t *
vtxobj_alloc(mobj_t *mobj, ver_t *ver, vtx_t *tx)
{
	vtxojb_t vtxo = __vtobj_alloc();
	vtxo->mo = mobj;
	vtxo->ver = ver;
	vtxo->tx = tx;
	if (tx != NULL)
		list_add(&tx->obj_l, &new->tx_l);
}

void
vtx_access(vtxobj_t **vtxobj_ptr, tx_t *tx)
{
	vtxobj_t *vtxobj = *txobj_ptr, *new;
	if (txobj->tx != NULL && txobj->tx == tx)
		return;
	/* allocate and initialize object */
	vtxobj_alloc(vtxobj->mo, ver_branch(txobj->ver), tx);
	/* return the new object */
	*txobj_ptr = new;
}

/**
 * Transaction handler
 * Transactions are temporary execution contexts.
 *  - They include a set of versioned objects
 *  - These objects are accessed via versions private to the transaction
 */
struct vtx {
	vtx_t  *parent_tx;      /* parent transaction (for nesting support) */
	struct list_head obj_l; /* list of objects owned by transaction */
};
typedef struct vtx vtx_t;

vtx_t *
vtx_begin(vtx_t *parent_tx)
{
	// alocate and initialize transaction
}

int
vtx_try_commit(vtx_t *tx)
{
	if (tx->parent_tx) {
		// TODO: nested transactions
		assert(false);
	}

	// TODO: What happens if a child transaction has not commited yet?
	//  - can we detect it?
	//   => we can check the reference count of the versions
	//         - what if a child transaction uses other objects?
	//  - do we wait for it? fail?

	txobj_t *txo;
	list_for_each(&tx->obj_l, txo, tx_l) {
		if (ver_parent(txo->ver) == txo->mo->ver)
			continue;

		// try to merge...
		// XXX: How the above property would effect versioned trees
		//      (e.g., GC)?
		ver_merge();
	}

	/* Note, that here we need to change the state atomically, since an
	 * object might have changed under our nose, and we can't merge it now.
	 * So, we take the locks in-order, and check versions. If we manage to
	 * take all the locks, we update the state.*/
	int err = 0;
	list_for_each(&tx->obj_l, txo, tx_l) { // XXX: assumption: list is ordered
		spin_lock(txo->mobj->lock);
		if (ver_parent(txo->ver) != txo->mo->ver) {
			err = -1;
			break;
		}
	}

	/* no error: update mutable objects */
	if (err == 0) {
		list_for_each(&tx->obj_l, txo, tx_l) {
			txo->mo->ver = txo->ver;
			txo->mo->vobj = txo->vobj;
			/* TODO: decrease/increase reference counts */
		}
	}

	/* release locks starting from txo and going backwards */
	//TODO

	return err;
}

int
vtx_end(vtx_t *tx, unsigned int retries)
{
	int err;
	do {
		err = vtx_try_commit(vtx_t *tx);
	} while (err != 0 && retries-- > 0);
	return err;
}


/* TODO: add a SLAB */
ver_t *
__ver_alloc(void)
{
	 ver_t *v = malloc(sizeof(ver_t));
	 if (v == NULL) {
		assert(0);
	 }
	atomic_set(&v->ref_count, 1);
	return v;
}

/* TODO: SLAB */
void
ver_free(vbfs_ver_t *v)
{
	free(v);
}

static mobj_t *
mobj_create(void *vobj)
{
	int err;
	mobj_t *mobj = malloc(sizeof(mobj_t));
	if (mobj == NULL) {
		assert(0);
	}
	mobj->ver = ver_create(vobj);
	err = spinlock_init(&mobj->lock);
	return mobj;
}

/* create a version from a versioned object */
ver_t *
ver_create(void *vobj)
{
	ver_t *ret = __ver_alloc();
	ret->parent = NULL;
	ret->vobj = vobj;
	return ret;
}

ver_t *
ver_branch(ver_t *parent)
{
	/* allocate and initialize new version */
	ver_t *v = __ver_alloc();
	/* increase the reference count of the parent */
	v->parent = ver_getref(parent);
	/* TODO copy object */
	// v->vobj = copy(parent->obj)
	return v;
}

/**
 * Test:
 *   This is a simple test for the transaction/object functionality:
 *     - objects are counters (integer)
 *     - merging is a simple reduction
 */
int main(int argc, const char *argv[])
{
	int O_nr = 4;       /* number of objects */
	int threads_nr = 4; /* number of threads */
	int ops_nr = 4096;  /* total number of operations -- in this case additions */

	/* objects:
	 *  O[i] : user object i
	 * mO[i] : mutable object i */
	struct O {
		int cnt;
	} __attribute__ ((aligned(128))) O[O_nr];
	mobj_t *mO[O_nr];

	/* operations (additions) */
	struct op {
		int Oi; /* object index */
		int n;  /* number to add */
	} Ops[ops_nr];

	/* initialize operations */
	srand(666);
	for (int i=0; i<ops_nr; i++) {
		Ops[i].Oi = rand() % O_nr;
		Ops[i].n  = rand();
	}

	/* initialize objects (Os) and create mutable objects (mOs) */
	for (int i=0; i<O_nr; i++) {
		O[i].cnt =  0;
		mO[i] = mobj_create(&O[i]);
	}

	/* threads  */
	struct work {
		struct op *ops;
		int ops_nr;
	};

	void *thread_fn(void *arg) {
		struct work *w = arg;

		vtx_t *vtx = vtx_begin(NULL); // start a transaction
		/* eagerly allocate transaction objects */
		vtxobj_t *vtxO[O_nr];
		for (int i=0; i<O_nr; i++) {
			ver_t *ver = ver_branch(mO[i]->ver);
			vtxO[i] = vtxobj_alloc(mO[i], ver, vtx);
		}

		/* apply the operations to the objects */
		for (int i=0; i<w->ops_nr; i++) {
			int Oi      = w->ops[i].Oi;
			int n       = w->ops[i].On;
			struct O *o = vtxO[Oi]->ver->vobj
			o->cnt += n;
		}

		vtx_end(vtx, 1); //end transaction
	}

	return 0;
}

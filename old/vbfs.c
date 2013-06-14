/**
 * NOTES
 * ======
 * The tree maps keys (vbfs_key) to values (vbfs_val)
 *   - internal nodes map keys to addresses that contain a node
 *   - leafs map keys to values
 *
 * In the tree, it would be nice if we could efficiently support different
 * combinations of:
 *  - key size
 *  - node size
 *  - constant/variable-sized values
 * with some sort of a generics interface
 *
 * For now, we assume:
 *  - key size of vbfs_key
 *  - variable sized values
 *
 * The tree needs to allocate nodes from the free pool that comes from the
 * initial linear space.
 *
 * It would be interesting to see if we could implement free space management
 * (allocator) using the tree itself. Btrfs, seems to do something like this,
 * but I'm not sure about the details. An interesting issue with  using the
 * trees for memory allocation is concurrency. The typical model is to use the
 * COW tree for having a different version per transaction. If there is a
 * conflict transaction fails. So we need to ensure that different threads work
 * on different areas of the tree (partition, randomized), or create per-thread
 * pools (trees) and have them somehow balance the space (steal?).
 */

/* address on the physical device */
typedef uint64_t addr_t;

enum vbfs_type {
	VBFS_NODE = 0x0,
	VBFS_LEAF = 0x1
};

/* block header */
struct vbfs_hdr {
	enum vbfs_type type;
	uint16_t       nritems;
} __attribute__ ((__packed__));

union vbfs_key {
	addr_t addr;
} __attribute__ ((__packed__));

/* key-address pairs */
struct vbfs_ka {
	union vbfs_key key;
	addr_t addr;
} __attribute__ ((__packed__));

/*
 * vbfs_node: tree (internal) node
 */
struct vbfs_node {
	struct vbfs_hdr hdr;
	struct {
		union vbfs_key key;
		addr_t         addr;
	} kas[]; /* key-address pairs */
} __attribute__ ((__packed__));

int
vbfs_node_find(struct vbfs_node *n, union vbfs_key key)
{
}

/*
 * vbfs_leaf: tree leaf
 * [key0|off0|len0|key1|off1|len1|....free space...|---val1---|--val0--]
 *                                                            <--off0--|
 *                                                 <--------off1-------|
 *                                                 <--len0---><--len1-->
 */
struct vbfs_leaf {
	struct vbfs_hdr hdr;
	struct {
		union vbfs_key key;
		uint32_t       off;
		uint16_t       len;
	} index[];
} __attribute__ ((__packed__));

struct vbfs {
	struct vbfs_node *alloc_tree;
};

addr_t
vbfs_alloc(struct vbfs *vbfs, uint64_t len)
{
	vbfs->alloc_tree;
}

/* hard-coded addresses in the space */
const addr_t vbfs_alloc_tree_addr = 0;

int
vbfs_init(unsigned char *begin, uint64_t len)
{
}

int
vbfs_mkfs(void)
{
}



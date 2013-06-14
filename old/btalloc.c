/* Assuming a linear space, is it possible to manage this space using a b+tree
 * structure ? */

enum {
	BTA_TYPE_NODE,
	BTA_TYPE_LEAF
}

struct bta_hdr {
	uint8_t type;
	uint16_t nr_items;
} __attribute__((__packed__));

struct bta_node_entry {
	uint64_t addr;
	uint64_t child;
} __attribute__((__packed__));

struct bta_node {
	struct bta_hdr        hdr;
	struct bta_node_entry entries[];
};

enum {
	BTA_LEAF_TNODE,
	BTA_LEAF_USER
};

struct bta_leaf_entry {
	uint64_t addr;
	uint8_t  type;
} __attribute__ ((packed));

struct bta_leaf {
	struct bta_hdr        hdr;
	struct bta_leaf_entry entries[];
};


static struct {
	unsigned char *start;
	size_t size;
} Space;

void
talloc_init(unsigned char *mem, uint64_t size)
{
	static const int root_nr_items = 64;
	Space.start = mem;
	Space.size = size;

	struct *root = Space.start;
	memset(root, 0, );
	root->nr_items = root_nr_items;
}

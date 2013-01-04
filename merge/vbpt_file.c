#include <string.h>
#include <assert.h>

#include "vbpt.h"
#include "vbpt_log.h"

#define MIN(x,y) ((x) < (y) ? (x) : (y))

/**
 * Simple file interface over vbpt
 */

// There are two cases of non-existent data:
//  - keys that are not there
//  - leafs with small ->d_len
// In both of these cases, we just fill the buffer with zeroes
void
vbpt_file_pread(vbpt_tree_t *tree, off_t offset, void *buff, size_t len)
{
	uint64_t key     = offset / VBPT_LEAF_SIZE;
	off_t    src_off = offset % VBPT_LEAF_SIZE;
	char     *dst    = buff;
	VBPT_START_TIMER(file_pread);
	while (len > 0) {
		vbpt_leaf_t *leaf = vbpt_logtree_get(tree, key);
		size_t cp_len; // copy length
		size_t ze_len; // zero length
		size_t to_len = MIN(VBPT_LEAF_SIZE - src_off, len); // total len
		if (leaf == NULL || leaf->d_len <= src_off) {
			cp_len = 0;
			ze_len = to_len;
		} else if (leaf->d_len - src_off >= to_len) {
			cp_len = to_len;
			ze_len = 0;
		} else {
			assert(leaf->d_len > src_off);
			cp_len = leaf->d_len - src_off;
			ze_len = to_len - cp_len;
		}
		assert(to_len = cp_len + ze_len);

		if (cp_len)
			memcpy(dst, leaf->data + src_off, cp_len);
		if (ze_len)
			bzero(dst + cp_len, ze_len);

		src_off = 0;
		dst += to_len;
		len -= to_len;
		key++;
	}
	VBPT_STOP_TIMER(file_pread);
}


// @new is COW from @old where @src, @src_len data are copied starting at
// @dst_off. Set new->data accordingly.
static void
cow_leaf_write(vbpt_leaf_t *new, const vbpt_leaf_t *old,
               size_t dst_off, const void *src, size_t src_len)
{
	//VBPT_START_TIMER(cow_leaf_write);
	// COW or zero part before dst_off
	if (dst_off) {
		size_t cp0_len = MIN(dst_off, old->d_len);
		size_t ze0_len = dst_off - cp0_len;
		if (cp0_len)
			memcpy(new->data, old->data, cp0_len);
		if (ze0_len)
			bzero(new->data + cp0_len, ze0_len);
	}
	// copy data from buffer
	memcpy(new->data + dst_off, src, src_len);
	// COW remaining data if needed
	size_t end = dst_off + src_len;
	if (end < old->d_len) {
		memcpy(new->data + end, old->data + end, old->d_len - end);
		end = old->d_len;
	}
	new->d_len = end;
	//VBPT_STOP_TIMER(cow_leaf_write);
}

void
vbpt_file_pwrite(vbpt_tree_t *tree, off_t offset, const void *buff, size_t len)
{
	uint64_t key    = offset / VBPT_LEAF_SIZE;
	off_t dst_off   = offset % VBPT_LEAF_SIZE;
	const char *src = buff;
	ver_t *ver    = tree->ver;

	VBPT_START_TIMER(file_pwrite);
	while (len > 0) {
		size_t src_len = MIN(VBPT_LEAF_SIZE - dst_off, len);
		const vbpt_leaf_t *old = vbpt_logtree_get(tree, key);
		if (old == NULL) {
			// allocate new leaf
			vbpt_leaf_t *new = vbpt_leaf_alloc(VBPT_LEAF_SIZE, ver);
			if (dst_off)
				bzero(new->data, dst_off);
			// copy data
			memcpy(new->data + dst_off, src, src_len);
			new->d_len = dst_off + src_len;
			assert(new->d_total_len >= new->d_len);
			// insert leaf
			vbpt_leaf_t *l;
			vbpt_logtree_insert(tree, key, new, &l);
			assert(l == NULL);
		} else if (ver_eq(ver, old->l_hdr.ver)) {
			// modify in-place
			vbpt_leaf_t *leaf = (vbpt_leaf_t *)old;
			memcpy(leaf->data + dst_off, src, src_len);
			if (dst_off + src_len > leaf->d_len)
				leaf->d_len = dst_off + src_len;
		} else {
			vbpt_leaf_t *new = vbpt_leaf_alloc(VBPT_LEAF_SIZE, ver);
			cow_leaf_write(new, old, dst_off, src, src_len);
			vbpt_leaf_t *l;
			vbpt_logtree_insert(tree, key, new, &l);
			assert(l == old);
			vbpt_leaf_putref(l);
		}

		dst_off = 0;
		len -= src_len;
		src += src_len;
		key++;
	}
	VBPT_STOP_TIMER(file_pwrite);
}

#if defined(VBPT_FILE_TEST)

#include "vbpt_log.h" // vbpt_logtree_log_init

int main(int argc, const char *argv[])
{
	ver_t *ver = ver_create();
	vbpt_tree_t *tree = vbpt_tree_alloc(ver);
	//vbpt_logtree_log_init(tree);
	#define BUFFSIZE 1024
	char buff_1[BUFFSIZE];
	memset(buff_1, 42, BUFFSIZE);
	vbpt_file_pwrite(tree, 0, buff_1, BUFFSIZE);

	char buff_2[BUFFSIZE];
	memset(buff_2, 0, BUFFSIZE);
	vbpt_file_pread(tree, 0, buff_2, BUFFSIZE);

	for (size_t i=0; i<BUFFSIZE; i++) {
		assert(buff_1[i] == buff_2[i]);
	}

	return 0;
}
#endif

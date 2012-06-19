/**
 * draw vbpt using graphviz
 */

#include <stdio.h>
#include <graphviz/gvc.h>

#include "darray.h"
#include "hash.h"

#include "vbpt.h"
#include "vbpt_gv.h"

static Agraph_t *gvGraph = NULL;

static char *fillcolors[] = {"red", "blue", "orange", "yellow", "green"};
static char *
get_fillcolor(ver_t *ver)
{
	static int cnt=0;
	static hash_t *h = NULL;
	if (!h) {
		h = hash_init(128);
	}

	unsigned long color = hash_lookup(h, (unsigned long)ver);
	if (color == HASH_ENTRY_NOTFOUND) {
		hash_ins(h, (unsigned long)ver, color=cnt++);
	}

	assert(color < sizeof(fillcolors));
	return fillcolors[color];
}

static void
do_agset(void *obj, char *attr, char *value)
{
	if (agset(obj, attr, value) == -1) {
		fprintf(stderr, "Couldn't set [%s,%s] to %p\n", attr,value,obj);
	}

}

static void
do_agattr(void *obj, char *attr, char *value)
{
	if (agattr(obj, attr, value) == NULL) // is this check correct?
		fprintf(stderr, "Couldn't set attribute [%s,%s] to %p\n", attr,value,obj);
}

static Agraph_t *
get_graph(void)
{
	if (gvGraph == NULL) {
		aginit();
		/* we need to set default values for attributes we want to
		 * customize */
		agnodeattr(NULL, "shape", "oval");
		agraphattr(NULL, "style", "invis");
		agraphattr(NULL, "color", "black");
		agnodeattr(NULL, "fillcolor", "pink");
		agnodeattr(NULL, "label", "");
		agnodeattr(NULL, "rankdir", "");
		gvGraph = agopen("VBPT", AGDIGRAPHSTRICT);
	}

	return gvGraph;
}

void
vbpt_gv_reset(void)
{
	if (gvGraph == NULL)
		return;
	agclose(gvGraph);
	gvGraph = NULL;
}

void
vbpt_gv_write(char *fname)
{
	Agraph_t *g = get_graph();

	if (fname == NULL) {
		fname = "vbpt.dot";
	}

	FILE *fp = fopen(fname, "w");
	if (fp == NULL) {
		perror(fname);
		exit(1);
	}

	agwrite(g, fp);
	fclose(fp);
	vbpt_gv_reset();
}

static void *
vbpt_add_leaf(vbpt_leaf_t *leaf)
{
	Agraph_t *g = get_graph();
	Agnode_t *n;

	char node_name[32];
	snprintf(node_name, sizeof(node_name), "%p", leaf);

	if ( (n=agfindnode(g, node_name)) != NULL ) {
		return n;
	}

	n = agnode(g, node_name);
	do_agset(n, "fillcolor", get_fillcolor(leaf->l_hdr.ver));
	return n;
}

/* recursively add a node to the graph */
void *
vbpt_gv_add_node(vbpt_node_t *node)
{
	Agraph_t *g = get_graph();
	Agnode_t *n;

	char node_name[32];
	snprintf(node_name, sizeof(node_name), "%p", node);

	if ((n = agfindnode(g, node_name)) != NULL) // node exists, do nothing
		return n;

	n = agnode(g, node_name);
	do_agset(n, "shape", "record");
	do_agattr(n, "style", "filled");
	do_agset(n, "fillcolor", get_fillcolor(node->n_hdr.ver));

	/* create label */
	darray_char da_label;
	darray_init(da_label);
	darray_append_lit(da_label, "");
	for (uint16_t i=0; i<node->items_nr; i++) {
		uint64_t child_key = node->kvp[i].key;
		if (i != 0)
			darray_append_lit(da_label, "|");
		char lbl[128];
		snprintf(lbl, sizeof(lbl), "<k%d> %"PRIu64, i, child_key);
		darray_append_string(da_label, lbl);
	}
	do_agset(n, "label", da_label.item);
	darray_free(da_label);

	for (uint16_t i=0; i<node->items_nr; i++) {
		vbpt_hdr_t *child_hdr = node->kvp[i].val;
		// find parent
		Agnode_t *parent = agfindnode(g, node_name);
		assert(parent != NULL);
		// find/create child
		Agnode_t *child;
		switch (child_hdr->type) {
			case  VBPT_NODE:
			child = vbpt_gv_add_node(hdr2node(child_hdr));
			break;

			case  VBPT_LEAF:
			child = vbpt_add_leaf(hdr2leaf(child_hdr));
			break;

			default:
			assert(false);
		}
		Agedge_t *e = agedge(g, parent, child);
		char port_name[16];
		snprintf(port_name, sizeof(port_name), "k%d", i);
		do_agset(e, "tailport", port_name);
		do_agset(e, "headport", "n");
	}

	return n;
}

#if 0
if (parent == NULL) {
	printf("could not find: --\%s--\n", parent_name);
	for (Agnode_t *n = agfstnode(g); ; n = agnxtnode(g, n) ) {
		printf("n=%s\n", n->name);
		if (n == aglstnode(g))
			break;
	}
	exit(10);
}
#endif

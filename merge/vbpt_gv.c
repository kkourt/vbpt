/**
 * draw vbpt using graphviz
 */

#include <stdio.h>
#include <graphviz/gvc.h>

#include "vbpt.h"
#include "darray.h"

static Agraph_t *gvGraph = NULL;

static void
do_agset(void *obj, char *attr, char *value)
{
	if (agset(obj, attr, value) == -1)
		fprintf(stderr, "Couldn't set [%s,%s] to %p\n", attr,value,obj);
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
		agnodeattr(NULL, "label", "");
		agnodeattr(NULL, "rankdir", "");
		gvGraph = agopen("VBPT", AGDIGRAPHSTRICT);
	}

	return gvGraph;
}

static void
vbpt_reset(void)
{
	if (gvGraph == NULL)
		return;
	agclose(gvGraph);
	gvGraph = NULL;
}

static void
vbpt_gv_end(char *fname)
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
	vbpt_reset();
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
	return n;
}

/* recursively add a node to the graph */
static void *
vbpt_add_node(vbpt_node_t *node)
{
	Agraph_t *g = get_graph();
	Agnode_t *n;

	char node_name[32];
	snprintf(node_name, sizeof(node_name), "%p", node);

	if ((n = agfindnode(g, node_name)) != NULL) // node exists, do nothing
		return n;

	n = agnode(g, node_name);
	do_agset(n, "shape", "record");

	/* create label */
	darray_char da_label;
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
		char parent_name[64];
		snprintf("%s:k%d", sizeof(parent_name), node_name, i);
		Agnode_t *parent = agfindnode(g, parent_name);
		assert(parent != NULL);
		// find/create child
		Agnode_t *child;
		switch (child_hdr->type) {
			case  VBPT_NODE:
			child = vbpt_add_node(hdr2node(child_hdr));
			break;

			case  VBPT_LEAF:
			child = vbpt_add_leaf(hdr2leaf(child_hdr));
			break;

			default:
			assert(false);
		}
		agedge(g, parent, child);
	}

	return n;
}

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>

#include "vbpt.h"
#include "vbpt_mtree.h"
#include "vbpt_merge.h"
#include "vbpt_tx.h"

#include "mt_lib.h"
#include "tsc.h"
#include "misc.h"

static void __attribute__((unused))
init_vbpt(vbpt_tree_t *tree, size_t size)
{
	const size_t bsize = 1024;
	const size_t nb = size / bsize;
	char buff[bsize];
	memset(buff, 'a', bsize);

	vbpt_logtree_log_init(tree);

	for (unsigned int i=0; i<nb; i++)
		vbpt_file_pwrite(tree, i*bsize, buff, bsize);

	size_t rem = size % bsize;
	if (rem)
		vbpt_file_pwrite(tree, nb*bsize, buff, rem);
}

/* initialize file by writting @size 'a' characters */
static void __attribute__((unused))
initf(const char *fname, size_t size)
{
	const size_t bsize = 1024;
	const size_t nb = size / bsize;
	char buff[bsize];
	memset(buff, 'a', bsize);

	int fd = open(fname, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
	if (fd == -1) {
		fprintf(stderr, "%s: failed to open file\n", __FUNCTION__);
		perror(fname);
		exit(1);
	}

	for (unsigned int i=0; i<nb; i++) {
		int ret = write(fd, buff, bsize);
		if (ret != bsize) {
			perror("write");
			exit(1);
		}
	}

	size_t rem = size % bsize;
	if (rem) {
		int ret = write(fd, buff, rem);
		if (ret != rem) {
			perror("write");
			exit(1);
		}
	}

	close(fd);
}

static int __attribute__((unused))
openf(const char *fname)
{
	const int flags = O_CREAT | O_RDWR;
	mode_t mode     = S_IRUSR | S_IWUSR;
	int fd = open(fname, flags, mode);
	if (fd == -1) {
		perror(fname);
		exit(1);
	}

	return fd;
}


const char dropf[] = "/proc/sys/vm/drop_caches";

#define DROP_DATA     1  /* free pagecache */
#define DROP_METADATA 2  /* free dentries and inodes */
#define DROP_ALL      3 /* free  pagecache,  dentries  and  inodes */
void
vm_drop_caches(int i)
{
	FILE *f = fopen(dropf, "w");
	if (f == NULL) {
		perror("fopen");
		exit(1);
	}

	switch (i) {
		case DROP_DATA:
		case DROP_METADATA:
		case DROP_ALL:
		fprintf(f, "%d\n", i);
		break;

		default:
		fprintf(stderr, "%s: invalid argument: %d\n", __FUNCTION__, i);
		exit(1);
	}
}

struct targ {
	int tid;                  // thread id
	int nthreads;             // total number of threads
	int nb;                   // total number of blocks
	int bsize;                // block size
	char *fname;              // filename for t_fs
	char *buff;               // this is to be used by t_mem()
	int  fd;                  // t_fs()
	vbpt_mtree_t *mtree;      // t_vbpt()
	unsigned int core;        // core this thread is affined to
	pthread_barrier_t *tbar;  // barrier
	uint64_t ticks;           // ticks
	vbpt_merge_stats_t merge_stats;
	vbpt_stats_t      vbpt_stats;
};

__attribute__((unused))
static void
vbpt_thr_print_stats(struct targ *arg)
{
	#define pr_ticks(x__) do { \
		double p__ = (double)s->x__ / (double)arg->ticks; \
		if (p__ < 0.1) \
			break; \
		printf("\t" # x__ ": %lu (%.1lf%%)\n", s->x__, p__*100); \
	} while (0)

	printf(" ticks=%.1lf M\n", arg->ticks/(1000*1000.0));
	printf("  VBPT Stats:\n");
	vbpt_stats_do_report("\t", &arg->vbpt_stats, arg->ticks);
	printf("  Merge Stats:\n");
	uint64_t merge_ticks = arg->merge_stats.merge_ticks;
	printf("\tmerge ticks: %.1lf M [merge/total:%lf]\n",
	          merge_ticks/(1000*1000.0),
	          (double)merge_ticks/(double)arg->ticks);
	vbpt_merge_stats_do_report("\t", &arg->merge_stats);
}

__attribute__((unused))
static void *
t_vbpt(void *arg)
{
	struct targ *targ   = arg;
	size_t t_bsize      = targ->bsize;
	vbpt_mtree_t *mtree = targ->mtree;
	char buff[t_bsize];
	INIT_VBPT_STATS();
	pthread_barrier_wait(targ->tbar);
	TSC_SET_TICKS(targ->ticks, {
	for (unsigned int b=targ->tid; b<targ->nb; b+=targ->nthreads) {
		while (1) {
			vbpt_txtree_t *txt;
			txt = vbpt_txtree_alloc(mtree);

			off_t off = b*t_bsize;
			vbpt_file_pread(txt->tree, off, buff, t_bsize);
			for (unsigned int i=0; i<t_bsize; i++)
				buff[i] += targ->tid;
			vbpt_file_pwrite(txt->tree, off, buff, t_bsize);

			vbpt_logtree_finalize(txt->tree);
			vbpt_txt_res_t ret = vbpt_txt_try_commit(txt, mtree, 2);
			//update_stats(ret);
			if (ret == VBPT_COMMIT_OK || ret == VBPT_COMMIT_MERGED)
				break;
		}
	}
	})
	pthread_barrier_wait(targ->tbar);
	vbpt_merge_stats_get(&targ->merge_stats);
	vbpt_stats_get(&targ->vbpt_stats);
	pthread_barrier_wait(targ->tbar);
	return NULL;
}

__attribute__((unused))
static void *
t_fs(void *arg)
{
	struct targ *targ = arg;
	size_t t_bsize = targ->bsize;
	char buff[t_bsize];

	pthread_barrier_wait(targ->tbar);
	//TSC_MEASURE_TICKS(ticks, {
		for (unsigned int b=targ->tid; b<targ->nb; b+=targ->nthreads) {
			int ret;
			ret = pread(targ->fd, buff, t_bsize, b*t_bsize);
			assert(ret == t_bsize);
			for (unsigned int i=0; i<t_bsize; i++)
				buff[i] += targ->tid;
			ret = pwrite(targ->fd, buff, t_bsize, b*t_bsize);
			assert(ret == t_bsize);
		}
	//})
	pthread_barrier_wait(targ->tbar);
	//targ->ticks = ticks;
	pthread_barrier_wait(targ->tbar);
	return NULL;
}


__attribute__((unused))
static void *
t_mem(void *arg)
{
	struct targ *t = arg;
	unsigned char b_local[t->bsize];
	char *b_file = t->buff;

	pthread_barrier_wait(t->tbar);
	TSC_MEASURE_TICKS(ticks, {
		for (unsigned int b=t->tid; b<t->nb; b+=t->nthreads) {
			/* copy from buffer */
			memcpy(b_local, b_file + b*t->bsize, t->bsize);
			/* modify */
			for (unsigned int i=0; i<t->bsize; i++)
				b_local[i] += t->tid;
			/* copy back */
			memcpy(b_file + b*t->bsize, b_local, t->bsize);
		}
	})
	pthread_barrier_wait(t->tbar);
	t->ticks = ticks;
	pthread_barrier_wait(t->tbar);
	return NULL;
}


int main(int argc, const char *argv[])
{
	if (argc < 4) {
		fprintf(stderr, "Usage: %s <fname> <nblocks> <bsize>\n", argv[0]);
		exit(1);
	}

	#if !defined(NO_FILES)
	const char *fname = argv[1];
	#endif
	size_t nb = atoi(argv[2]);              /* number of blocks */
	size_t bsize = atoi(argv[3]);           /* block size */
	size_t fsize = nb*bsize;

	unsigned int ncpus;
	unsigned int *cpus;
	mt_get_options(&ncpus, &cpus);

	#if defined(SAME_FILE)
	initf(fname, fsize);
	#elif defined(SEP_FILES)
	size_t s = strlen(fname) + 16;
	char fnames[ncpus][s];
	for (int i=0; i<ncpus; i++) {
		snprintf(fnames[i], s, "%s.%d", fname, i);
		initf(fnames[i], fsize);
	}
	#elif defined(NO_FILES)
	char *b = xmalloc(fsize);
	memset(b, 'a', fsize);
	#elif defined(VBPT_FILE)
	vbpt_tree_t  *tree0 = vbpt_tree_create();
	vbpt_mtree_t *mtree = vbpt_mtree_alloc(tree0);
	init_vbpt(tree0, fsize);
	#endif

	pthread_t   tids[ncpus];
	struct targ targs[ncpus];
	pthread_barrier_t tbar;

	pthread_barrier_init(&tbar, NULL, ncpus+1);
	bzero(targs, sizeof(targs));
	for (int i=0; i<ncpus; i++) {
		targs[i].tid = i;
		targs[i].nthreads = ncpus;
		targs[i].bsize = bsize;
		targs[i].nb = nb;

		#if defined(SAME_FILE)
		targs[i].fd = openf(fname);
		#elif defined(SEP_FILES)
		targs[i].fd = openf(fnames[i]);
		#elif defined(NO_FILES)
		targs[i].buff = b;
		#elif defined(VBPT_FILE)
		targs[i].mtree = mtree;
		#endif

		targs[i].core = cpus[i];
		targs[i].tbar = &tbar;

		#if !defined(NO_FILES)
		if (targs[i].fd == -1) {
			fprintf(stderr, "%s: failed to open file\n", __FUNCTION__);
			perror(fname);
			exit(1);
		}
		#endif
	}


	/* drop file-system caches */
	//vm_drop_caches(DROP_ALL);

	#if defined(SEP_FILES) || defined(SAME_FILE)
	#define t_fn t_fs
	#elif defined(NO_FILES)
	#define t_fn t_mem
	#elif defined(VBPT_FILE)
	#define t_fn t_vbpt
	#endif

	for (int i=0; i<ncpus; i++) {
		pthread_create(tids + i, NULL, t_fn, targs + i);
	}

	pthread_barrier_wait(&tbar);         // START
	TSC_MEASURE_TICKS(ticks, {
		pthread_barrier_wait(&tbar); // END
	})
	pthread_barrier_wait(&tbar);         // GOT STATS

	for (int i=0; i<ncpus; i++) {
		pthread_join(tids[i], NULL);
	}
	printf("%-20s: nb=%zu bsize=%zu nthreads=%d ticks=%.1lfM\n",
	       argv[0], nb, bsize, ncpus, ticks/(1000*1000.0));

	#if defined(VBPT_FILE)
	for (unsigned i=0; i<ncpus; i++) {
		printf("T: %2u [tid:%d] ", i, targs[i].tid);
		vbpt_thr_print_stats(targs+i);
	}
	#endif

	if (getenv("KEEP_FILES") != NULL)
		return 0;

	#if defined(SAME_FILE)
	unlink(fname);
	#elif defined(SEP_FILES)
	for (int i=0; i<ncpus; i++) {
		unlink(fnames[i]);
	}
	#endif
	free(cpus);
	return 0;
}

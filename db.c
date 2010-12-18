#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

struct indata {
	uint32_t	follower;
	uint32_t	leader;
} __attribute__((__packed__));

struct idmap {
	uint32_t	following;
	uint32_t	followed;
	uint32_t	nfollowing;
	uint32_t	nfollowed;
};

struct perthread {
	pthread_t	id;
	int		have_nleader;
	int		nleader;
	pthread_mutex_t	mtx;
	pthread_cond_t	cond;
} __attribute__((__aligned__ (64)));

struct perthread *threads;
pthread_barrier_t bar;

long ncpu;
int nentries;
int nentries2;
int maxfollower;

struct indata *indata;

uint32_t *following;
uint32_t *followed;

struct idmap *idmap;

static int
cmpid(const void *a, const void *b)
{
	return (*(uint32_t *)a - *(uint32_t *)b);
}

static void *
worker(void *arg)
{
	long mycpu = (long)arg;
	struct perthread *me = &threads[mycpu];
	int start, end;
	int start2, end2;
	int start3, end3;
	int startid, endid;
	int nleader;
	int i, dest, from;
	uint32_t id;

	start = nentries / ncpu * mycpu;
	end = nentries / ncpu * (mycpu + 1);

	if (mycpu == ncpu - 1)
		end = nentries;

	/*
	 * First convert the input data format to our compact representation.
	 */

	/* search for the first complete record */
	if (end < nentries)
		id = indata[end].follower;

	for (i = end; i < nentries; ++i) {
		if (indata[i].follower != id)
			break;
	}
	end = i;

	id = indata[start].follower;
	for (i = start; i < end; ++i) {
		if (indata[i].follower != id)
			break;
	}
	start = i;

	if (mycpu == 0)
		start = 0;

	start2 = dest = start;
	startid = indata[start].follower;
	id = 0;
	for (i = start; i < end; ++i) {
		if (indata[i].follower != id) {
			id = indata[i].follower;

			idmap[id].following = dest;
		}
		following[dest++] = indata[i].leader;
		idmap[id].nfollowing++;
	}
	end2 = dest;
	endid = id;

	switch (pthread_barrier_wait(&bar)) {
	case 0:
	case PTHREAD_BARRIER_SERIAL_THREAD:
		break;
	default:
		errx(1, "pthread_barrier_wait");
	}

	for (i = start2; i < end2; ++i) {
		id = following[i];

		__sync_add_and_fetch(&idmap[id].nfollowed, 1);
	}

	switch (pthread_barrier_wait(&bar)) {
	case 0:
	case PTHREAD_BARRIER_SERIAL_THREAD:
		break;
	default:
		errx(1, "pthread_barrier_wait");
	}

	nleader = 0;
	for (i = startid; i < endid; ++i)
		nleader += idmap[i].nfollowed;

	/* propagate counts */
	if (mycpu != 0) {
		if ((errno = pthread_mutex_lock(&me->mtx)) != 0)
			err(1, "pthread_mutex_lock");
		while (!me->have_nleader)
			pthread_cond_wait(&me->cond, &me->mtx);
		pthread_mutex_unlock(&me->mtx);
	}

	start3 = me->nleader;
	end3 = start3 + nleader;
	if (mycpu != ncpu - 1) {
		struct perthread *next = &threads[mycpu + 1];

		pthread_mutex_lock(&next->mtx);
		next->nleader = end3;
		next->have_nleader = 1;
		pthread_mutex_unlock(&next->mtx);
		pthread_cond_signal(&next->cond);
	}

	dest = start3;
	for (i = startid; i < endid; ++i) {
		idmap[i].followed = dest;
		dest += idmap[i].nfollowed;
	}

	switch (pthread_barrier_wait(&bar)) {
	case 0:
	case PTHREAD_BARRIER_SERIAL_THREAD:
		break;
	default:
		errx(1, "pthread_barrier_wait");
	}

	for (from = startid; from < endid; ++from) {
		int fromstart = idmap[from].following;

		for (i = 0; i < idmap[from].nfollowing; ++i) {
			int pos;

			id = following[i + fromstart];
			dest = idmap[id].followed;
			pos = __sync_fetch_and_add(&followed[dest + idmap[id].nfollowed - 1], 1);
			followed[dest + pos] = from;
		}
	}

	switch (pthread_barrier_wait(&bar)) {
	case 0:
	case PTHREAD_BARRIER_SERIAL_THREAD:
		break;
	default:
		errx(1, "pthread_barrier_wait");
	}

	for (i = startid; i < endid; ++i) {
		qsort(&followed[idmap[i].followed], idmap[i].nfollowed,
		      sizeof(*followed), cmpid);
	}

	switch (pthread_barrier_wait(&bar)) {
	case 0:
	case PTHREAD_BARRIER_SERIAL_THREAD:
		break;
	default:
		errx(1, "pthread_barrier_wait");
	}

	/*
	 * Now find pairs.
	 *
	 * Each of our id entries is a candidate x.
	 * Each x might follow some leader a.
	 * x must be in the list of followers of a, L(a).
	 * We select a pair (x,y) for each y in L(a) where
	 * y > x.  Then calculate the similarity of (x,y).
	 *
	 * We need to avoid to output the same pair multiple times.
	 * To do so, we collect all y first for all L(a) and then sort
	 * this list.  Skip duplicated items.
	 */

	size_t lbs = 1000, lbp;
	uint32_t *lb = calloc(sizeof(*lb), lbs);

	if (!lb)
		err(1, "alloc");

	for (uint32_t x = startid; x < endid; ++x) {
		int nf = idmap[x].nfollowing;
		int y;

		lbp = 0;

		dest = idmap[x].following;
		for (i = 0; i < nf; ++i) {
			uint32_t *pme;
			int a;

			a = following[dest + i];

			/* Binary search for us */
			pme = (uint32_t *)bsearch(&x, &followed[idmap[a].followed],
				    idmap[a].nfollowed, sizeof(*followed),
				    cmpid);

			if (pme - &followed[idmap[a].followed] + 1 >= idmap[a].nfollowed)
				continue;

			if (lbp == lbs) {
				lbs *= 2;
				lb = realloc(lb, sizeof(*lb) * lbs);
				if (!lb)
					err(2, "alloc");
			}

			lb[lbp++] = pme[1];
		}

		qsort(lb, lbp, sizeof(*lb), cmpid);

		y = 0;
		for (i = 0; i < lbp; ++i) {
			if (lb[i] == y)
				continue;

			y = lb[i];

			/* calculate similarity between x and y */
		}
	}

	return (NULL);
}

int main (int argc, char const* argv[])
{
	int fd;
	struct stat st;
	void *buf;

	fd = open(argv[1], O_RDONLY);
	if (fd < 0)
		err(1, "open: %s", argv[1]);

	if (fstat(fd, &st) < 0)
		err(1, "stat");

	buf = mmap(NULL, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
	if (buf == MAP_FAILED)
		err(1, "mmap");

	indata = buf;

	nentries = st.st_size / sizeof(struct indata);
	maxfollower = indata[nentries - 1].follower;
	nentries2 = nentries + maxfollower;

	following = calloc(sizeof(*following), nentries2);
	followed = calloc(sizeof(*followed), nentries2);
	idmap = calloc(sizeof(*idmap), maxfollower + 2);

	if (!following || !followed || !idmap)
		err(1, "alloc");

	ncpu = sysconf(_SC_NPROCESSORS_ONLN);
	if (ncpu == -1)
		ncpu = 1;

	threads = calloc(sizeof(*threads), ncpu);
	if (!threads)
		err(1, "alloc");

	if ((errno = pthread_barrier_init(&bar, NULL, ncpu) != 0))
		err(1, "pthread_barrier_init");

	for (int i = 0; i < ncpu; ++i) {
		errno = pthread_create(&threads[i].id, NULL, worker, (void *)(uintptr_t)i);
		if (errno)
			err(1, "pthread_create");
	}

	for (int i = 0; i < ncpu; ++i) {
		void *ex;

		pthread_join(threads[i].id, &ex);
	}

	return 0;
}

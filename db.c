#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <err.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

struct indata {
	uint32_t	follower;
	uint32_t	leader;
} __attribute__((__packed__));

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

uint32_t *idmap;

static int
is_crossptr(uint32_t id)
{
	return (id & 0x80000000);
}

static int
make_crossptr(uint32_t id)
{
	return (id | 0x80000000);
}

static int
get_crossptr(uint32_t id)
{
	return (id & ~0x80000000);
}

static void *
worker(void *arg)
{
	long mycpu = (long)arg;
	struct perthread *me = &threads[mycpu];
	int start, end;
	int start2, end2;
	int start3, end3;
	int nleader;
	int i, dest, from;
	uint32_t id, lastid;

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

	start2 = dest = start + indata[i].follower - 1;	/* index is 1-based */
	id = lastid = indata[i].follower - 1;
	for (i = start; i < end; ++i) {
		if (indata[i].follower != id) {
			id = indata[i].follower;

			/* put in temp counter for followed */
			while (lastid++ != id) {
				idmap[lastid] = dest;
				following[dest++] = 0;
			}
		}
		following[dest++] = indata[i].leader;
	}
	end2 = dest;

	switch (pthread_barrier_wait(&bar)) {
	case 0:
	case PTHREAD_BARRIER_SERIAL_THREAD:
		break;
	default:
		errx(1, "pthread_barrier_wait");
	}

	for (i = start2; i < end2; ++i) {
		if (following[i] == 0) {
			following[i] = make_crossptr(0);
		} else {
			following[i] = idmap[following[i]];
		}
	}

	switch (pthread_barrier_wait(&bar)) {
	case 0:
		break;
	case PTHREAD_BARRIER_SERIAL_THREAD:
		free(idmap);
		break;
	default:
		errx(1, "pthread_barrier_wait");
	}

#ifdef FULLRANGE
	nleader = 0;
	for (i = 0; i < nentries2; ++i) {
		id = following[i];
		if (is_crossptr(id) || id < start2 || id >= end2)
			continue;

		/* hack: first count the number of entries */
		following[id]++;
		nleader++;
	}

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
#else
	for (i = start2; i < end2; ++i) {
		id = following[i];
		if (is_crossptr(id))
			continue;

		/* hack: first count the number of entries */
		__sync_add_and_fetch(&following[id], 1);
	}

	/* propagate counts */
	if (mycpu != 0) {
		if ((errno = pthread_mutex_lock(&me->mtx)) != 0)
			err(1, "pthread_mutex_lock");
		while (!me->have_nleader)
			pthread_cond_wait(&me->cond, &me->mtx);
		pthread_mutex_unlock(&me->mtx);
	}

	for (i = start2; i < end2; ++i) {
		if (!is_crossptr(following[i]))
			continue;

		nleader += get_crossptr(following[i]);
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
#endif

	dest = start3;
	for (i = start2; i < end2; ++i) {
		int count;

		if (!is_crossptr(following[i]))
			continue;
		count = get_crossptr(following[i]);
		following[i] = make_crossptr(dest);
		dest += count + 1;
	}

	switch (pthread_barrier_wait(&bar)) {
	case 0:
	case PTHREAD_BARRIER_SERIAL_THREAD:
		break;
	default:
		errx(1, "pthread_barrier_wait");
	}

#ifdef FULLRANGE
	for (i = 0; i < nentries2; ++i) {
		id = following[i];
		if (is_crossptr(id)) {
			from = i;
			continue;
		}
		if (id < start2 || id >= end2)
			continue;

		dest = get_crossptr(following[id]);
		followed[dest]++;
		followed[dest + followed[dest]] = from;
	}
#else
	for (i = start2; i < end2; ++i) {
		id = following[i];
		if (is_crossptr(id)) {
			from = i;
			continue;
		}

		dest = get_crossptr(following[id]);
		dest += __sync_add_and_fetch(&followed[dest], 1);
		followed[dest] = from;
	}
#endif

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
	idmap = calloc(sizeof(*idmap), maxfollower + 1);

	if (!following || !followed || !idmap)
		err(1, "alloc");

	ncpu = sysconf(_SC_NPROCESSORS_ONLN);
	if (ncpu == -1)
		ncpu = 1;

	ncpu *= 10;

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

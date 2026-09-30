/*
 * chunkfs: read-only FUSE lowlevel filesystem that serves files stored as
 * fixed-size chunk files, with a switchable read reply path.
 *
 * Store layout: <store>/<name>.<idx> (every chunk exactly --chunk-size bytes
 * except the last), optional <store>/<name>.whole (whole-file copy, used by
 * the passthrough mode). The mount exposes /<name>.
 *
 * Built inside the libfuse source tree (example/) because the vmsplice mode
 * needs the request's unique id and channel fd, which only fuse_i.h exposes.
 */
#define _GNU_SOURCE
#define FUSE_USE_VERSION FUSE_MAKE_VERSION(3, 18)

#include <fuse_lowlevel.h>
#include <fuse_kernel.h>
#include "fuse_i.h"
#if __has_include("fuse_cap_names_i.h")
#include "fuse_cap_names_i.h"
#define HAVE_CAP_NAMES 1
#endif

#include <dirent.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

#define MAXSEG 64
#define NBUCKETS 14
#define TIMEOUT 86400.0

enum mode {
	M_COPY, M_MEMCACHE, M_MMAP, M_SPLICE, M_SPLICE_NOMOVE, M_VMSPLICE,
	M_URING, M_URING_BUFPOOL, M_URING_ZC, M_PASSTHROUGH,
};

static const char *mode_names[] = {
	"copy", "memcache", "mmap", "splice", "splice-nomove", "vmsplice",
	"uring", "uring-bufpool", "uring-zc", "passthrough",
};

struct chunk {
	_Atomic int fd;
	void *_Atomic map;
	char *_Atomic mem;
	uint32_t len;
};

struct cfile {
	char *name;
	uint64_t size;
	uint32_t nchunks;
	struct chunk *chunks;
	int whole_fd;
	int backing_id;
	int open_count;
};

struct counters {
	uint64_t reads, bytes, uring_reads, fallbacks, errors, mc_over_limit;
	uint64_t zc_reads, zc_whole;
	uint64_t writev_calls, writev_bytes;
	uint64_t splice_calls, splice_bytes, splice_move_calls;
	uint64_t vmsplice_calls, vmsplice_bytes, vmsplice_gift_calls;
	uint64_t hist[NBUCKETS];
	uint64_t max_req, min_req;
};
#define NCOUNTERS (sizeof(struct counters) / sizeof(uint64_t))

struct tstate {
	struct tstate *next;
	struct counters c;
	char *buf;
	size_t bufsz;
	int pipe[2];
	size_t pipe_slots;
};

static struct {
	enum mode mode;
	const char *store;
	const char *mountpoint;
	int store_fd;
	uint32_t chunk_size;
	unsigned max_write, max_readahead, max_read;
	unsigned threads, uring_q_depth;
	bool clone_fd, direct_io, keep_cache, gift, debug;
	uint64_t memcache_limit;
	char *preload;
	const char *stats_json;
	char *fuse_opts;

	struct cfile *files;
	size_t nfiles;

	_Atomic uint64_t mc_bytes, mc_chunks;
	_Atomic int passthrough_failed;

	pthread_mutex_t lock;
	struct tstate *threads_list;
	struct fuse_session *se;

	bool snap_taken;
	struct counters base;
	struct rusage ru_base;
	struct timespec t_base;

	int pipe_default, pipe_grow;
	char init_error[512];
	char init_log[8192];
	size_t init_log_len;
	struct {
		bool seen;
		uint64_t capable_ext, want_ext;
		unsigned max_write, max_read, max_readahead, max_pages;
		bool uring, bufpool;
	} neg;
} g = {
	.mode = M_COPY,
	.chunk_size = 4u << 20,
	.uring_q_depth = 8,
	.memcache_limit = 8ull << 30,
	.lock = PTHREAD_MUTEX_INITIALIZER,
};

static __thread struct tstate *tls;

#define CNT(t, f) atomic_load_explicit((_Atomic uint64_t *)&(t)->c.f, memory_order_relaxed)
#define SET(t, f, v) atomic_store_explicit((_Atomic uint64_t *)&(t)->c.f, (v), memory_order_relaxed)
#define ADD(t, f, v) SET(t, f, CNT(t, f) + (v))

static struct tstate *ts(void)
{
	if (tls)
		return tls;
	struct tstate *t = calloc(1, sizeof(*t));
	if (!t)
		abort();
	t->pipe[0] = t->pipe[1] = -1;
	t->c.min_req = UINT64_MAX;
	pthread_mutex_lock(&g.lock);
	t->next = g.threads_list;
	g.threads_list = t;
	pthread_mutex_unlock(&g.lock);
	tls = t;
	return t;
}

/*
 * Interpose the reply-path syscalls libfuse makes (it calls them through the
 * PLT, so the executable's definitions win) to count which path each reply
 * really took; libfuse falls back from splice to writev silently.
 */
static ssize_t (*real_writev)(int, const struct iovec *, int);
static ssize_t (*real_splice)(int, loff_t *, int, loff_t *, size_t, unsigned);
static ssize_t (*real_vmsplice)(int, const struct iovec *, size_t, unsigned);

__attribute__((constructor)) static void resolve_real(void)
{
	real_writev = dlsym(RTLD_NEXT, "writev");
	real_splice = dlsym(RTLD_NEXT, "splice");
	real_vmsplice = dlsym(RTLD_NEXT, "vmsplice");
}

ssize_t writev(int fd, const struct iovec *iov, int cnt)
{
	ssize_t r = real_writev(fd, iov, cnt);
	struct tstate *t = ts();
	ADD(t, writev_calls, 1);
	if (r > 0)
		ADD(t, writev_bytes, r);
	return r;
}

ssize_t splice(int fdi, loff_t *offi, int fdo, loff_t *offo, size_t len, unsigned fl)
{
	ssize_t r = real_splice(fdi, offi, fdo, offo, len, fl);
	struct tstate *t = ts();
	ADD(t, splice_calls, 1);
	if (fl & SPLICE_F_MOVE)
		ADD(t, splice_move_calls, 1);
	if (r > 0)
		ADD(t, splice_bytes, r);
	return r;
}

ssize_t vmsplice(int fd, const struct iovec *iov, size_t n, unsigned fl)
{
	ssize_t r = real_vmsplice(fd, iov, n, fl);
	struct tstate *t = ts();
	ADD(t, vmsplice_calls, 1);
	if (fl & SPLICE_F_GIFT)
		ADD(t, vmsplice_gift_calls, 1);
	if (r > 0)
		ADD(t, vmsplice_bytes, r);
	return r;
}

static void sum_counters(struct counters *out)
{
	memset(out, 0, sizeof(*out));
	out->min_req = UINT64_MAX;
	pthread_mutex_lock(&g.lock);
	for (struct tstate *t = g.threads_list; t; t = t->next) {
		uint64_t *dst = (uint64_t *)out;
		_Atomic uint64_t *src = (_Atomic uint64_t *)&t->c;
		for (size_t i = 0; i < NCOUNTERS; i++) {
			uint64_t v = atomic_load_explicit(&src[i], memory_order_relaxed);
			if (&dst[i] == &out->max_req)
				dst[i] = v > dst[i] ? v : dst[i];
			else if (&dst[i] == &out->min_req)
				dst[i] = v < dst[i] ? v : dst[i];
			else
				dst[i] += v;
		}
	}
	pthread_mutex_unlock(&g.lock);
}

/* ---------- store ---------- */

static int cmp_file(const void *a, const void *b)
{
	return strcmp(((const struct cfile *)a)->name, ((const struct cfile *)b)->name);
}

static struct cfile *find_file(const char *name)
{
	struct cfile key = { .name = (char *)name };
	return bsearch(&key, g.files, g.nfiles, sizeof(key), cmp_file);
}

static void die(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	fprintf(stderr, "chunkfs: ");
	vfprintf(stderr, fmt, ap);
	fprintf(stderr, "\n");
	va_end(ap);
	exit(2);
}

static void scan_store(void)
{
	DIR *d = fdopendir(dup(g.store_fd));
	if (!d)
		die("opendir %s: %s", g.store, strerror(errno));
	size_t cap = 0;
	struct dirent *de;
	while ((de = readdir(d))) {
		const char *dot = strrchr(de->d_name, '.');
		if (!dot || dot == de->d_name || strcmp(dot, ".0"))
			continue;
		if (g.nfiles == cap) {
			cap = cap ? cap * 2 : 1024;
			g.files = realloc(g.files, cap * sizeof(*g.files));
		}
		struct cfile *f = &g.files[g.nfiles++];
		memset(f, 0, sizeof(*f));
		f->name = strndup(de->d_name, dot - de->d_name);
		f->whole_fd = -1;
	}
	closedir(d);
	if (!g.nfiles)
		die("no chunk files (<name>.0) in %s", g.store);
	qsort(g.files, g.nfiles, sizeof(*g.files), cmp_file);

	char p[PATH_MAX];
	for (size_t i = 0; i < g.nfiles; i++) {
		struct cfile *f = &g.files[i];
		size_t ccap = 16;
		f->chunks = calloc(ccap, sizeof(*f->chunks));
		struct stat st;
		for (;;) {
			snprintf(p, sizeof(p), "%s.%u", f->name, f->nchunks);
			if (fstatat(g.store_fd, p, &st, 0) < 0)
				break;
			if (f->nchunks && f->chunks[f->nchunks - 1].len != g.chunk_size)
				die("%s: chunk %u is short but not last", f->name, f->nchunks - 1);
			if ((uint64_t)st.st_size > g.chunk_size || st.st_size == 0)
				die("%s: size %lld does not fit chunk size %u",
				    p, (long long)st.st_size, g.chunk_size);
			if (f->nchunks == ccap) {
				ccap *= 2;
				f->chunks = realloc(f->chunks, ccap * sizeof(*f->chunks));
			}
			struct chunk *c = &f->chunks[f->nchunks++];
			c->fd = -1;
			c->map = NULL;
			c->mem = NULL;
			c->len = st.st_size;
			f->size += st.st_size;
		}
		if (g.mode == M_PASSTHROUGH || g.mode == M_URING_ZC) {
			snprintf(p, sizeof(p), "%s.whole", f->name);
			f->whole_fd = openat(g.store_fd, p, O_RDONLY | O_CLOEXEC);
			if (f->whole_fd < 0)
				die("%s mode needs %s/%s: %s", mode_names[g.mode],
				    g.store, p, strerror(errno));
			if (fstat(f->whole_fd, &st) < 0 || (uint64_t)st.st_size != f->size)
				die("%s: size differs from its chunks", p);
		}
	}
}

static int chunk_fd(struct cfile *f, uint32_t i)
{
	struct chunk *c = &f->chunks[i];
	int fd = atomic_load_explicit(&c->fd, memory_order_acquire);
	if (fd >= 0)
		return fd;
	char p[PATH_MAX];
	snprintf(p, sizeof(p), "%s.%u", f->name, i);
	int nfd = openat(g.store_fd, p, O_RDONLY | O_CLOEXEC);
	if (nfd < 0)
		return -errno;
	int exp = -1;
	if (!atomic_compare_exchange_strong(&c->fd, &exp, nfd)) {
		close(nfd);
		return exp;
	}
	return nfd;
}

static void *chunk_map(struct cfile *f, uint32_t i)
{
	struct chunk *c = &f->chunks[i];
	void *m = atomic_load_explicit(&c->map, memory_order_acquire);
	if (m)
		return m;
	int fd = chunk_fd(f, i);
	if (fd < 0)
		return NULL;
	m = mmap(NULL, c->len, PROT_READ, MAP_SHARED, fd, 0);
	if (m == MAP_FAILED)
		return NULL;
	void *exp = NULL;
	if (!atomic_compare_exchange_strong(&c->map, &exp, m)) {
		munmap(m, c->len);
		return exp;
	}
	return m;
}

static int pread_full(int fd, char *dst, size_t len, off_t off)
{
	while (len) {
		ssize_t r = pread(fd, dst, len, off);
		if (r < 0) {
			if (errno == EINTR)
				continue;
			return -errno;
		}
		if (r == 0)
			return -EIO;
		dst += r;
		len -= r;
		off += r;
	}
	return 0;
}

static char *chunk_mem(struct cfile *f, uint32_t i)
{
	struct chunk *c = &f->chunks[i];
	char *m = atomic_load_explicit(&c->mem, memory_order_acquire);
	if (m)
		return m;
	if (atomic_fetch_add(&g.mc_bytes, c->len) + c->len > g.memcache_limit) {
		atomic_fetch_sub(&g.mc_bytes, c->len);
		return NULL;
	}
	int fd = chunk_fd(f, i);
	if (fd < 0 || posix_memalign((void **)&m, 4096, c->len)) {
		atomic_fetch_sub(&g.mc_bytes, c->len);
		return NULL;
	}
	if (pread_full(fd, m, c->len, 0) < 0) {
		free(m);
		atomic_fetch_sub(&g.mc_bytes, c->len);
		return NULL;
	}
	char *exp = NULL;
	if (!atomic_compare_exchange_strong(&c->mem, &exp, m)) {
		free(m);
		atomic_fetch_sub(&g.mc_bytes, c->len);
		return exp;
	}
	atomic_fetch_add(&g.mc_chunks, 1);
	return m;
}

static bool prefix_match(const char *name)
{
	if (!g.preload)
		return false;
	if (!strcmp(g.preload, "all"))
		return true;
	char *list = strdup(g.preload), *save = NULL;
	bool hit = false;
	for (char *p = strtok_r(list, ",", &save); p && !hit; p = strtok_r(NULL, ",", &save))
		hit = !strncmp(name, p, strlen(p));
	free(list);
	return hit;
}

static void preload(void)
{
	uint64_t n = 0, miss = 0;
	for (size_t i = 0; i < g.nfiles; i++) {
		struct cfile *f = &g.files[i];
		if (!prefix_match(f->name))
			continue;
		for (uint32_t k = 0; k < f->nchunks; k++) {
			if (chunk_mem(f, k))
				n++;
			else
				miss++;
		}
	}
	fprintf(stderr, "chunkfs: preloaded %" PRIu64 " chunks (%" PRIu64 " MiB), %" PRIu64
		" over the --memcache-bytes limit\n", n, atomic_load(&g.mc_bytes) >> 20, miss);
}

/* ---------- reply paths ---------- */

struct seg {
	struct cfile *f;
	uint32_t idx;
	uint32_t coff;
	uint32_t len;
};

static int build_segs(struct cfile *f, uint64_t off, size_t size, struct seg *s)
{
	int n = 0;
	while (size) {
		uint32_t idx = off / g.chunk_size;
		uint32_t coff = off % g.chunk_size;
		uint32_t len = f->chunks[idx].len - coff;
		if (len > size)
			len = size;
		if (n == MAXSEG)
			return -1;
		s[n++] = (struct seg){ f, idx, coff, len };
		off += len;
		size -= len;
	}
	return n;
}

static int copy_segs(char *dst, const struct seg *s, int n)
{
	for (int i = 0; i < n; i++) {
		int fd = chunk_fd(s[i].f, s[i].idx);
		if (fd < 0)
			return fd;
		int r = pread_full(fd, dst, s[i].len, s[i].coff);
		if (r < 0)
			return r;
		dst += s[i].len;
	}
	return 0;
}

static void reply_copy(fuse_req_t req, struct tstate *t, const struct seg *s, int n, size_t size)
{
	if (t->bufsz < size) {
		free(t->buf);
		t->bufsz = 0;
		if (posix_memalign((void **)&t->buf, 4096, size)) {
			t->buf = NULL;
			fuse_reply_err(req, ENOMEM);
			return;
		}
		t->bufsz = size;
	}
	int r = copy_segs(t->buf, s, n);
	if (r < 0) {
		ADD(t, errors, 1);
		fuse_reply_err(req, -r);
		return;
	}
	fuse_reply_buf(req, t->buf, size);
}

static void reply_memcache(fuse_req_t req, struct tstate *t, const struct seg *s, int n, size_t size)
{
	struct iovec iov[MAXSEG];
	for (int i = 0; i < n; i++) {
		char *m = chunk_mem(s[i].f, s[i].idx);
		if (!m) {
			ADD(t, mc_over_limit, 1);
			ADD(t, fallbacks, 1);
			reply_copy(req, t, s, n, size);
			return;
		}
		iov[i] = (struct iovec){ m + s[i].coff, s[i].len };
	}
	fuse_reply_iov(req, iov, n);
}

static int map_iov(struct iovec *iov, const struct seg *s, int n)
{
	for (int i = 0; i < n; i++) {
		char *m = chunk_map(s[i].f, s[i].idx);
		if (!m)
			return -EIO;
		iov[i] = (struct iovec){ m + s[i].coff, s[i].len };
	}
	return 0;
}

static void reply_mmap(fuse_req_t req, struct tstate *t, const struct seg *s, int n)
{
	struct iovec iov[MAXSEG];
	if (map_iov(iov, s, n) < 0) {
		ADD(t, errors, 1);
		fuse_reply_err(req, EIO);
		return;
	}
	fuse_reply_iov(req, iov, n);
}

static void reply_splice(fuse_req_t req, struct tstate *t, const struct seg *s, int n, bool move)
{
	struct {
		struct fuse_bufvec v;
		struct fuse_buf more[MAXSEG];
	} bv;
	bv.v.count = n;
	bv.v.idx = 0;
	bv.v.off = 0;
	for (int i = 0; i < n; i++) {
		int fd = chunk_fd(s[i].f, s[i].idx);
		if (fd < 0) {
			ADD(t, errors, 1);
			fuse_reply_err(req, -fd);
			return;
		}
		bv.v.buf[i] = (struct fuse_buf){
			.size = s[i].len,
			.flags = FUSE_BUF_IS_FD | FUSE_BUF_FD_SEEK,
			.fd = fd,
			.pos = s[i].coff,
		};
	}
	uint64_t before = CNT(t, writev_calls);
	fuse_reply_data(req, &bv.v, move ? FUSE_BUF_SPLICE_MOVE : FUSE_BUF_SPLICE_NONBLOCK);
	if (CNT(t, writev_calls) != before)
		ADD(t, fallbacks, 1);
}

static bool ensure_pipe(struct tstate *t)
{
	if (t->pipe[0] >= 0)
		return true;
	if (pipe2(t->pipe, O_CLOEXEC) < 0)
		return false;
	size_t want = (size_t)(g.neg.max_write ? g.neg.max_write : g.max_write) + (MAXSEG + 2) * 4096ul;
	int sz = fcntl(t->pipe[0], F_SETPIPE_SZ, want);
	if (sz < 0) {
		long max = 0;
		FILE *fp = fopen("/proc/sys/fs/pipe-max-size", "r");
		if (fp) {
			if (fscanf(fp, "%ld", &max) != 1)
				max = 0;
			fclose(fp);
		}
		sz = max > 0 ? fcntl(t->pipe[0], F_SETPIPE_SZ, max) : -1;
		if (sz < 0)
			sz = fcntl(t->pipe[0], F_GETPIPE_SZ);
	}
	t->pipe_slots = sz > 0 ? sz / 4096 : 16;
	return true;
}

static void drop_pipe(struct tstate *t)
{
	close(t->pipe[0]);
	close(t->pipe[1]);
	t->pipe[0] = t->pipe[1] = -1;
}

static int vmsplice_all(int fd, struct iovec *iov, int n, unsigned flags)
{
	while (n) {
		ssize_t r = vmsplice(fd, iov, n, flags);
		if (r < 0) {
			if (errno == EINTR)
				continue;
			return -errno;
		}
		while (n && (size_t)r >= iov->iov_len) {
			r -= iov->iov_len;
			iov++;
			n--;
		}
		if (n) {
			iov->iov_base = (char *)iov->iov_base + r;
			iov->iov_len -= r;
		}
	}
	return 0;
}

static void reply_vmsplice(fuse_req_t req, struct tstate *t, const struct seg *s, int n, size_t size)
{
	struct iovec iov[MAXSEG];
	if (map_iov(iov, s, n) < 0) {
		ADD(t, errors, 1);
		fuse_reply_err(req, EIO);
		return;
	}
	size_t slots = 1;
	for (int i = 0; i < n; i++) {
		uintptr_t a = (uintptr_t)iov[i].iov_base;
		slots += (a + iov[i].iov_len + 4095) / 4096 - a / 4096;
	}
	if (!ensure_pipe(t) || slots > t->pipe_slots) {
		/* The whole reply must sit in the pipe: /dev/fuse takes one message per splice. */
		ADD(t, fallbacks, 1);
		fuse_reply_iov(req, iov, n);
		return;
	}
	struct fuse_out_header out = {
		.len = sizeof(out) + size,
		.error = 0,
		.unique = req->unique,
	};
	struct iovec hdr = { &out, sizeof(out) };
	int chfd = req->ch ? req->ch->fd : req->se->fd;
	/*
	 * SPLICE_F_GIFT only for the file pages: the header lives on the stack.
	 * The kernel accepts GIFT on MAP_SHARED file pages, but fuse can only
	 * steal a page whose refcount is 1, which a page-cache page never has.
	 */
	int r = vmsplice_all(t->pipe[1], &hdr, 1, 0);
	if (!r)
		r = vmsplice_all(t->pipe[1], iov, n, g.gift ? SPLICE_F_GIFT : 0);
	if (!r) {
		ssize_t w = splice(t->pipe[0], NULL, chfd, NULL, out.len, g.gift ? SPLICE_F_MOVE : 0);
		if (w == (ssize_t)out.len) {
			fuse_reply_none(req);
			return;
		}
		r = w < 0 ? -errno : -EIO;
	}
	drop_pipe(t);
	ADD(t, errors, 1);
	if (r == -ENOENT)
		fuse_reply_none(req);
	else
		fuse_reply_err(req, EIO);
}

static void reply_uring(fuse_req_t req, struct tstate *t, const struct seg *s, int n, size_t size)
{
	char *p;
	size_t psz;
	if (fuse_req_get_payload(req, &p, &psz, NULL) != 0 || psz < size) {
		ADD(t, fallbacks, 1);
		reply_copy(req, t, s, n, size);
		return;
	}
	int r = copy_segs(p, s, n);
	if (r < 0) {
		ADD(t, errors, 1);
		fuse_reply_err(req, -r);
		return;
	}
	fuse_reply_buf(req, p, size);
}

#ifdef CHUNKFS_ZC
static void reply_uring_zc(fuse_req_t req, struct tstate *t, const struct seg *s, int n,
			   size_t size, uint64_t off, struct fuse_file_info *fi)
{
	if (fi->fh != 1) {
		ADD(t, fallbacks, 1);
		reply_uring(req, t, s, n, size);
		return;
	}
	/* One READ_FIXED per request: a request that spans chunks reads the .whole copy. */
	int fd;
	off_t pos;
	if (n == 1) {
		fd = chunk_fd(s[0].f, s[0].idx);
		pos = s[0].coff;
	} else {
		fd = s[0].f->whole_fd;
		pos = off;
		ADD(t, zc_whole, 1);
	}
	if (fd < 0 || fuse_do_zero_copy(req, fd, NULL, pos, size, true) < 0) {
		ADD(t, errors, 1);
		fuse_reply_err(req, EIO);
		return;
	}
	ADD(t, zc_reads, 1);
}
#endif

/* ---------- operations ---------- */

static struct cfile *ino_file(fuse_ino_t ino)
{
	if (ino < 2 || ino - 2 >= g.nfiles)
		return NULL;
	return &g.files[ino - 2];
}

static void fill_attr(fuse_ino_t ino, struct stat *st)
{
	memset(st, 0, sizeof(*st));
	st->st_ino = ino;
	st->st_uid = getuid();
	st->st_gid = getgid();
	if (ino == FUSE_ROOT_ID) {
		st->st_mode = S_IFDIR | 0555;
		st->st_nlink = 2;
	} else {
		struct cfile *f = ino_file(ino);
		st->st_mode = S_IFREG | 0444;
		st->st_nlink = 1;
		st->st_size = f->size;
		st->st_blocks = (f->size + 511) / 512;
	}
	st->st_blksize = g.chunk_size;
}

static void record_negotiated(struct fuse_session *se)
{
	if (g.neg.seen)
		return;
	pthread_mutex_lock(&g.lock);
	if (!g.neg.seen) {
		g.neg.capable_ext = se->conn.capable_ext;
		g.neg.want_ext = se->conn.want_ext;
		g.neg.max_write = se->conn.max_write;
		g.neg.max_read = se->conn.max_read;
		g.neg.max_readahead = se->conn.max_readahead;
		g.neg.max_pages = (se->conn.max_write - 1) / 4096 + 1;
		g.neg.uring = !!(se->conn.want_ext & FUSE_CAP_OVER_IO_URING);
#ifdef FUSE_CAP_IO_URING_BUFPOOL
		g.neg.bufpool = !!(se->conn.want_ext & FUSE_CAP_IO_URING_BUFPOOL);
#endif
		g.neg.seen = true;
	}
	pthread_mutex_unlock(&g.lock);
}

static void init_fail(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(g.init_error, sizeof(g.init_error), fmt, ap);
	va_end(ap);
	fprintf(stderr, "chunkfs: %s\n", g.init_error);
	fuse_session_exit(g.se);
}
#define INIT_FAIL(...) do { init_fail(__VA_ARGS__); return; } while (0)

static void cfs_init(void *userdata, struct fuse_conn_info *conn)
{
	(void)userdata;
	if (g.max_write)
		conn->max_write = g.max_write;
	if (g.max_readahead)
		conn->max_readahead = g.max_readahead;
	fuse_unset_feature_flag(conn, FUSE_CAP_SPLICE_READ);
	/* libfuse does not enable SPLICE_WRITE/MOVE by default; without them fuse_reply_data() silently writev()s. */
	if (g.mode == M_SPLICE || g.mode == M_SPLICE_NOMOVE) {
		if (!fuse_set_feature_flag(conn, FUSE_CAP_SPLICE_WRITE))
			INIT_FAIL("splice: kernel/libfuse did not offer FUSE_CAP_SPLICE_WRITE");
		if (g.mode == M_SPLICE && !fuse_set_feature_flag(conn, FUSE_CAP_SPLICE_MOVE))
			INIT_FAIL("splice: kernel/libfuse did not offer FUSE_CAP_SPLICE_MOVE");
	} else {
		fuse_unset_feature_flag(conn, FUSE_CAP_SPLICE_WRITE);
		fuse_unset_feature_flag(conn, FUSE_CAP_SPLICE_MOVE);
	}
	bool uring = g.mode == M_URING || g.mode == M_URING_BUFPOOL || g.mode == M_URING_ZC;
	if (uring) {
		if (!(conn->capable_ext & FUSE_CAP_OVER_IO_URING))
			INIT_FAIL("%s: kernel did not offer FUSE_OVER_IO_URING; is /sys/module/fuse/parameters/enable_uring Y?",
			    mode_names[g.mode]);
#ifdef FUSE_CONN_FLAG_SINGLE_ISSUER
		fuse_set_conn_flag(conn, FUSE_CONN_FLAG_SINGLE_ISSUER);
#endif
	} else {
		fuse_unset_feature_flag(conn, FUSE_CAP_OVER_IO_URING);
	}
#ifdef FUSE_CAP_IO_URING_BUFPOOL
	if (g.mode == M_URING_BUFPOOL) {
		if (!(conn->capable_ext & FUSE_CAP_IO_URING_BUFPOOL))
			INIT_FAIL("uring-bufpool: kernel did not offer FUSE_HAS_IO_URING_BUFPOOL (needs 7.3+)");
		fuse_set_feature_flag(conn, FUSE_CAP_IO_URING_BUFPOOL);
	} else {
		fuse_unset_feature_flag(conn, FUSE_CAP_IO_URING_BUFPOOL);
	}
#endif
	if (g.mode == M_PASSTHROUGH) {
		if (!fuse_set_feature_flag(conn, FUSE_CAP_PASSTHROUGH))
			INIT_FAIL("passthrough: kernel did not offer FUSE_PASSTHROUGH (CONFIG_FUSE_PASSTHROUGH, 6.9+)");
		conn->max_backing_stack_depth = 1;
	}
	if ((conn->max_write ? conn->max_write : g.max_write) / g.chunk_size + 2 > MAXSEG)
		INIT_FAIL("chunk size %u too small for max_write %u (max %d segments per request)",
		    g.chunk_size, conn->max_write, MAXSEG);
}

static void cfs_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
	record_negotiated(req->se);
	struct cfile *f = parent == FUSE_ROOT_ID ? find_file(name) : NULL;
	if (!f) {
		fuse_reply_err(req, ENOENT);
		return;
	}
	struct fuse_entry_param e = { .ino = 2 + (f - g.files), .attr_timeout = TIMEOUT,
				      .entry_timeout = TIMEOUT };
	fill_attr(e.ino, &e.attr);
	fuse_reply_entry(req, &e);
}

static void cfs_getattr(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	(void)fi;
	record_negotiated(req->se);
	if (ino != FUSE_ROOT_ID && !ino_file(ino)) {
		fuse_reply_err(req, ENOENT);
		return;
	}
	struct stat st;
	fill_attr(ino, &st);
	fuse_reply_attr(req, &st, TIMEOUT);
}

static void cfs_readdir(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
			struct fuse_file_info *fi)
{
	(void)fi;
	if (ino != FUSE_ROOT_ID) {
		fuse_reply_err(req, ENOTDIR);
		return;
	}
	char *buf = malloc(size);
	size_t used = 0;
	for (size_t i = off; i < g.nfiles + 2; i++) {
		struct stat st = { 0 };
		const char *name;
		if (i < 2) {
			name = i ? ".." : ".";
			st.st_ino = FUSE_ROOT_ID;
			st.st_mode = S_IFDIR;
		} else {
			name = g.files[i - 2].name;
			st.st_ino = i;
			st.st_mode = S_IFREG;
		}
		size_t l = fuse_add_direntry(req, buf + used, size - used, name, &st, i + 1);
		if (l > size - used)
			break;
		used += l;
	}
	fuse_reply_buf(req, buf, used);
	free(buf);
}

static void cfs_open(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	struct cfile *f = ino_file(ino);
	if (!f) {
		fuse_reply_err(req, ENOENT);
		return;
	}
	if ((fi->flags & O_ACCMODE) != O_RDONLY) {
		fuse_reply_err(req, EROFS);
		return;
	}
	fi->direct_io = g.direct_io;
	fi->keep_cache = g.keep_cache;
	if (g.mode == M_PASSTHROUGH) {
		pthread_mutex_lock(&g.lock);
		if (!f->open_count) {
			f->backing_id = fuse_passthrough_open(req, f->whole_fd);
			if (f->backing_id <= 0) {
				pthread_mutex_unlock(&g.lock);
				if (!atomic_exchange(&g.passthrough_failed, 1))
					fprintf(stderr, "chunkfs: fuse_passthrough_open failed (%s); needs CAP_SYS_ADMIN\n",
						strerror(errno));
				fuse_reply_err(req, EPERM);
				return;
			}
		}
		f->open_count++;
		fi->backing_id = f->backing_id;
		fi->keep_cache = 0;
		pthread_mutex_unlock(&g.lock);
	}
#ifdef CHUNKFS_ZC
	/* fi->zerocopy is not handed back on read, so remember it in fh. */
	if (g.mode == M_URING_ZC && fuse_should_do_zero_copy(req)) {
		fi->zerocopy = 1;
		fi->fh = 1;
	}
#endif
	fuse_reply_open(req, fi);
}

static void cfs_release(fuse_req_t req, fuse_ino_t ino, struct fuse_file_info *fi)
{
	(void)fi;
	struct cfile *f = ino_file(ino);
	if (g.mode == M_PASSTHROUGH && f) {
		pthread_mutex_lock(&g.lock);
		if (f->open_count && !--f->open_count) {
			fuse_passthrough_close(req, f->backing_id);
			f->backing_id = 0;
		}
		pthread_mutex_unlock(&g.lock);
	}
	fuse_reply_err(req, 0);
}

static int bucket(size_t sz)
{
	int b = 0;
	for (size_t s = 4096; s < sz && b < NBUCKETS - 1; s <<= 1)
		b++;
	return b;
}

static void cfs_read(fuse_req_t req, fuse_ino_t ino, size_t size, off_t off,
		     struct fuse_file_info *fi)
{
	struct tstate *t = ts();
	struct cfile *f = ino_file(ino);
	ADD(t, reads, 1);
	ADD(t, hist[bucket(size)], 1);
	if (size > CNT(t, max_req))
		SET(t, max_req, size);
	if (size < CNT(t, min_req))
		SET(t, min_req, size);
	if (fuse_req_is_uring(req))
		ADD(t, uring_reads, 1);
	if (!f) {
		fuse_reply_err(req, EBADF);
		return;
	}
	if ((uint64_t)off >= f->size) {
		fuse_reply_buf(req, NULL, 0);
		return;
	}
	if (off + size > f->size)
		size = f->size - off;
	struct seg s[MAXSEG];
	int n = build_segs(f, off, size, s);
	if (n < 0) {
		ADD(t, errors, 1);
		fuse_reply_err(req, EINVAL);
		return;
	}
	ADD(t, bytes, size);
	switch (g.mode) {
	case M_COPY:
	case M_PASSTHROUGH:
		if (g.mode == M_PASSTHROUGH)
			ADD(t, fallbacks, 1);
		reply_copy(req, t, s, n, size);
		break;
	case M_MEMCACHE:
		reply_memcache(req, t, s, n, size);
		break;
	case M_MMAP:
		reply_mmap(req, t, s, n);
		break;
	case M_SPLICE:
	case M_SPLICE_NOMOVE:
		reply_splice(req, t, s, n, g.mode == M_SPLICE);
		break;
	case M_VMSPLICE:
		reply_vmsplice(req, t, s, n, size);
		break;
	case M_URING:
	case M_URING_BUFPOOL:
		reply_uring(req, t, s, n, size);
		break;
	case M_URING_ZC:
#ifdef CHUNKFS_ZC
		reply_uring_zc(req, t, s, n, size, off, fi);
#endif
		break;
	}
}

static const struct fuse_lowlevel_ops ops = {
	.init = cfs_init,
	.lookup = cfs_lookup,
	.getattr = cfs_getattr,
	.readdir = cfs_readdir,
	.open = cfs_open,
	.release = cfs_release,
	.read = cfs_read,
};

/* ---------- stats ---------- */

static void log_func(enum fuse_log_level level, const char *fmt, va_list ap)
{
	char line[1024];
	vsnprintf(line, sizeof(line), fmt, ap);
	if (strstr(line, "FUSE_INIT:") || strstr(line, "uring") || strstr(line, "splice")) {
		pthread_mutex_lock(&g.lock);
		size_t l = strlen(line);
		if (g.init_log_len + l < sizeof(g.init_log)) {
			memcpy(g.init_log + g.init_log_len, line, l);
			g.init_log_len += l;
		}
		pthread_mutex_unlock(&g.lock);
	}
	if (level <= FUSE_LOG_NOTICE || g.debug || strstr(line, "uring"))
		fputs(line, stderr);
}

static long status_kib(const char *key)
{
	FILE *f = fopen("/proc/self/status", "r");
	char line[256];
	long v = -1;
	size_t kl = strlen(key);
	while (f && fgets(line, sizeof(line), f))
		if (!strncmp(line, key, kl) && line[kl] == ':') {
			v = strtol(line + kl + 1, NULL, 10);
			break;
		}
	if (f)
		fclose(f);
	return v;
}

static void take_snapshot(void)
{
	int fd = open("/proc/self/clear_refs", O_WRONLY);
	if (fd >= 0) {
		if (write(fd, "5", 1) < 0)
			perror("clear_refs");
		close(fd);
	}
	sum_counters(&g.base);
	pthread_mutex_lock(&g.lock);
	for (struct tstate *t = g.threads_list; t; t = t->next) {
		SET(t, max_req, 0);
		SET(t, min_req, UINT64_MAX);
	}
	pthread_mutex_unlock(&g.lock);
	getrusage(RUSAGE_SELF, &g.ru_base);
	clock_gettime(CLOCK_MONOTONIC, &g.t_base);
	g.snap_taken = true;
}

static void write_stats(void);
static bool stats_written;

static void *signal_thread(void *arg)
{
	sigset_t *set = arg;
	for (;;) {
		int sig;
		if (sigwait(set, &sig))
			continue;
		if (sig == SIGUSR2) {
			write_stats();
			stats_written = true;
			fprintf(stderr, "chunkfs: stats written\n");
			continue;
		}
		take_snapshot();
		if (g.stats_json) {
			char p[PATH_MAX];
			snprintf(p, sizeof(p), "%s.mark", g.stats_json);
			FILE *f = fopen(p, "w");
			if (f) {
				fprintf(f, "ok\n");
				fclose(f);
			}
		}
		fprintf(stderr, "chunkfs: stats window started\n");
	}
	return NULL;
}

static double tv_s(struct timeval tv)
{
	return tv.tv_sec + tv.tv_usec / 1e6;
}

static void json_str(FILE *f, const char *s)
{
	fputc('"', f);
	for (; *s; s++) {
		if (*s == '"' || *s == '\\')
			fprintf(f, "\\%c", *s);
		else if (*s == '\n')
			fputs("\\n", f);
		else if ((unsigned char)*s < 0x20)
			fprintf(f, "\\u%04x", *s);
		else
			fputc(*s, f);
	}
	fputc('"', f);
}

static void caps_json(FILE *f, uint64_t caps)
{
	fputc('[', f);
#ifdef HAVE_CAP_NAMES
	bool first = true;
	for (const struct fuse_cap_name *c = fuse_cap_names; c->name; c++)
		if (caps & c->flag) {
			fprintf(f, "%s\"%s\"", first ? "" : ",", c->name);
			first = false;
		}
#else
	(void)caps;
#endif
	fputc(']', f);
}

static const char *bucket_label(int b)
{
	static const char *l[NBUCKETS] = { "4K", "8K", "16K", "32K", "64K", "128K", "256K",
					   "512K", "1M", "2M", "4M", "8M", "16M", ">16M" };
	return l[b];
}

static void window_json(FILE *f, const struct counters *c, double ut, double st,
			double wall)
{
	double gib = c->bytes / (double)(1ull << 30);
	fprintf(f, "{\"wall_s\":%.3f,\"utime_s\":%.3f,\"stime_s\":%.3f,\"cpu_s\":%.3f,"
		"\"cpu_s_per_gib\":%.4f,", wall, ut, st, ut + st, gib > 0 ? (ut + st) / gib : 0.0);
	fprintf(f, "\"reads\":%" PRIu64 ",\"bytes\":%" PRIu64 ",\"uring_reads\":%" PRIu64
		",\"fallbacks\":%" PRIu64 ",\"errors\":%" PRIu64 ",\"memcache_over_limit\":%" PRIu64
		",\"zc_reads\":%" PRIu64 ",\"zc_spanning_whole\":%" PRIu64 ",",
		c->reads, c->bytes, c->uring_reads, c->fallbacks, c->errors, c->mc_over_limit,
		c->zc_reads, c->zc_whole);
	fprintf(f, "\"writev_calls\":%" PRIu64 ",\"writev_bytes\":%" PRIu64
		",\"splice_calls\":%" PRIu64 ",\"splice_bytes\":%" PRIu64 ",\"splice_move_calls\":%" PRIu64
		",\"vmsplice_calls\":%" PRIu64 ",\"vmsplice_bytes\":%" PRIu64
		",\"vmsplice_gift_calls\":%" PRIu64 ",",
		c->writev_calls, c->writev_bytes, c->splice_calls, c->splice_bytes,
		c->splice_move_calls, c->vmsplice_calls, c->vmsplice_bytes, c->vmsplice_gift_calls);
	fprintf(f, "\"req_size_min\":%" PRIu64 ",\"req_size_max\":%" PRIu64 ",\"req_size_hist\":{",
		c->reads ? c->min_req : 0, c->max_req);
	bool first = true;
	for (int b = 0; b < NBUCKETS; b++)
		if (c->hist[b]) {
			fprintf(f, "%s\"%s\":%" PRIu64, first ? "" : ",", bucket_label(b), c->hist[b]);
			first = false;
		}
	fputs("}}", f);
}

static void write_stats(void)
{
	struct counters tot, win;
	struct rusage ru;
	struct timespec now;
	sum_counters(&tot);
	getrusage(RUSAGE_SELF, &ru);
	clock_gettime(CLOCK_MONOTONIC, &now);
	win = tot;
	double ut = tv_s(ru.ru_utime), st = tv_s(ru.ru_stime), wall = 0;
	if (g.snap_taken) {
		uint64_t *w = (uint64_t *)&win;
		const uint64_t *b = (const uint64_t *)&g.base;
		for (size_t i = 0; i < NCOUNTERS; i++)
			if (&w[i] != &win.max_req && &w[i] != &win.min_req)
				w[i] -= b[i];
		ut -= tv_s(g.ru_base.ru_utime);
		st -= tv_s(g.ru_base.ru_stime);
		wall = (now.tv_sec - g.t_base.tv_sec) + (now.tv_nsec - g.t_base.tv_nsec) / 1e9;
	}

	FILE *outs[2] = { stderr, NULL };
	char tmp[PATH_MAX];
	snprintf(tmp, sizeof(tmp), "%s.tmp", g.stats_json ? g.stats_json : "");
	if (g.stats_json && !stats_written)
		outs[1] = fopen(tmp, "w");
	for (int k = 0; k < 2; k++) {
		FILE *f = outs[k];
		if (!f)
			continue;
		fprintf(f, "{\"mode\":\"%s\",\"libfuse\":\"%s\",\"init_error\":", mode_names[g.mode],
			fuse_pkgversion());
		json_str(f, g.init_error);
		fputc(',', f);
		fprintf(f, "\"options\":{\"chunk_size\":%u,\"max_write\":%u,\"max_readahead\":%u,"
			"\"max_read\":%u,\"threads\":%u,\"clone_fd\":%d,\"direct_io\":%d,\"keep_cache\":%d,"
			"\"gift\":%d,\"memcache_bytes\":%" PRIu64 ",\"uring_q_depth\":%u,\"preload\":",
			g.chunk_size, g.max_write, g.max_readahead, g.max_read, g.threads, g.clone_fd,
			g.direct_io, g.keep_cache, g.gift, g.memcache_limit, g.uring_q_depth);
		json_str(f, g.preload ? g.preload : "");
		fprintf(f, "},\"negotiated\":{\"seen\":%d,\"capable_ext\":\"0x%" PRIx64 "\",\"want_ext\":\"0x%"
			PRIx64 "\",\"want\":", g.neg.seen, g.neg.capable_ext, g.neg.want_ext);
		caps_json(f, g.neg.want_ext);
		fprintf(f, ",\"max_write\":%u,\"max_read\":%u,\"max_readahead\":%u,\"max_pages\":%u,"
			"\"io_uring\":%d,\"io_uring_bufpool\":%d,\"passthrough\":%d,\"splice_write\":%d,"
			"\"splice_move\":%d,\"libfuse_log\":",
			g.neg.max_write, g.neg.max_read, g.neg.max_readahead, g.neg.max_pages,
			g.neg.uring, g.neg.bufpool,
			!!(g.neg.want_ext & FUSE_CAP_PASSTHROUGH) && !g.passthrough_failed,
			!!(g.neg.want_ext & FUSE_CAP_SPLICE_WRITE), !!(g.neg.want_ext & FUSE_CAP_SPLICE_MOVE));
		g.init_log[g.init_log_len] = 0;
		json_str(f, g.init_log);
		fprintf(f, "},\"memcache\":{\"bytes\":%" PRIu64 ",\"chunks\":%" PRIu64 "},",
			atomic_load(&g.mc_bytes), atomic_load(&g.mc_chunks));
		fprintf(f, "\"rss\":{\"maxrss_kib\":%ld,\"hwm_kib\":%ld,\"rss_kib\":%ld,\"rss_anon_kib\":%ld,"
			"\"rss_file_kib\":%ld},", ru.ru_maxrss, status_kib("VmHWM"), status_kib("VmRSS"),
			status_kib("RssAnon"), status_kib("RssFile"));
		fprintf(f, "\"pipe_probe\":{\"default_bytes\":%d,\"grow_to_2m\":%d},", g.pipe_default,
			g.pipe_grow);
		fprintf(f, "\"window_valid\":%d,\"window\":", g.snap_taken);
		window_json(f, &win, ut, st, wall);
		fputs(",\"total\":", f);
		window_json(f, &tot, tv_s(ru.ru_utime), tv_s(ru.ru_stime), 0);
		fputs("}\n", f);
		if (k) {
			fclose(f);
			rename(tmp, g.stats_json);
		}
	}
}

/* ---------- main ---------- */

/*
 * An unprivileged user over /proc/sys/fs/pipe-user-pages-soft gets 2-page
 * pipes that cannot grow; every splice/vmsplice reply then falls back.
 */
static void probe_pipe(void)
{
	int p[2];
	if (pipe2(p, O_CLOEXEC) < 0) {
		g.pipe_default = g.pipe_grow = -errno;
		return;
	}
	g.pipe_default = fcntl(p[0], F_GETPIPE_SZ);
	g.pipe_grow = fcntl(p[0], F_SETPIPE_SZ, 2 << 20);
	if (g.pipe_grow < 0)
		g.pipe_grow = -errno;
	close(p[0]);
	close(p[1]);
	if ((g.mode == M_SPLICE || g.mode == M_SPLICE_NOMOVE || g.mode == M_VMSPLICE) &&
	    g.pipe_grow < 0)
		fprintf(stderr, "chunkfs: WARNING: pipes are %d bytes and cannot grow to 2 MiB (%s); "
			"most %s replies will fall back to writev\n", g.pipe_default,
			strerror(-g.pipe_grow), mode_names[g.mode]);
}

static void usage(const char *argv0)
{
	fprintf(stderr,
		"usage: %s --store DIR [options] MOUNTPOINT\n"
		"  --mode M              copy|memcache|mmap|splice|splice-nomove|vmsplice|uring|\n"
		"                        uring-bufpool|uring-zc|passthrough (default copy)\n"
		"  --chunk-size N        chunk size in bytes (default 4194304)\n"
		"  --max-write N         conn->max_write (also max_pages and io-uring payload size)\n"
		"  --max-readahead N     conn->max_readahead\n"
		"  --max-read N          -o max_read=N mount option\n"
		"  --threads N           max worker threads (/dev/fuse transport)\n"
		"  --clone-fd            one /dev/fuse fd per worker thread\n"
		"  --direct-io           FOPEN_DIRECT_IO on open (bypass the FUSE page cache)\n"
		"  --keep-cache          FOPEN_KEEP_CACHE on open\n"
		"  --gift                vmsplice: SPLICE_F_GIFT + SPLICE_F_MOVE\n"
		"  --memcache-bytes N    memcache bound (default 8 GiB); beyond it reads fall back to copy\n"
		"  --preload LIST        memcache: load chunks of files whose name starts with any of\n"
		"                        the comma-separated prefixes (or 'all') before mounting\n"
		"  --uring-q-depth N     io-uring queue depth per CPU (default 8)\n"
		"  --stats-json FILE     write stats here at exit or on SIGUSR2 (then not again at\n"
		"                        exit); SIGUSR1 starts the measured window and touches FILE.mark\n"
		"  -o OPTS               extra libfuse/mount options\n"
		"  --debug               libfuse debug output\n", argv0);
	exit(2);
}

static uint64_t parse_size(const char *s)
{
	char *end;
	uint64_t v = strtoull(s, &end, 0);
	switch (*end) {
	case 'k': case 'K': return v << 10;
	case 'm': case 'M': return v << 20;
	case 'g': case 'G': return v << 30;
	case 0: return v;
	}
	die("bad size '%s'", s);
	return 0;
}

static void add_opt(char **opts, const char *o)
{
	char *n;
	if (asprintf(&n, "%s%s%s", *opts ? *opts : "", *opts ? "," : "", o) < 0)
		abort();
	free(*opts);
	*opts = n;
}

static bool read_param_y(const char *path)
{
	char c = 0;
	FILE *f = fopen(path, "r");
	if (f) {
		if (fread(&c, 1, 1, f) != 1)
			c = 0;
		fclose(f);
	}
	return c == 'Y' || c == 'y' || c == '1';
}

int main(int argc, char *argv[])
{
	static const struct option lo[] = {
		{ "mode", required_argument, 0, 'm' },
		{ "store", required_argument, 0, 's' },
		{ "chunk-size", required_argument, 0, 'c' },
		{ "max-write", required_argument, 0, 'W' },
		{ "max-readahead", required_argument, 0, 'R' },
		{ "max-read", required_argument, 0, 'r' },
		{ "threads", required_argument, 0, 't' },
		{ "clone-fd", no_argument, 0, 'C' },
		{ "direct-io", no_argument, 0, 'D' },
		{ "keep-cache", no_argument, 0, 'K' },
		{ "gift", no_argument, 0, 'G' },
		{ "memcache-bytes", required_argument, 0, 'M' },
		{ "preload", required_argument, 0, 'P' },
		{ "uring-q-depth", required_argument, 0, 'q' },
		{ "stats-json", required_argument, 0, 'j' },
		{ "debug", no_argument, 0, 'd' },
		{ "help", no_argument, 0, 'h' },
		{ 0 }
	};
	int o;
	while ((o = getopt_long(argc, argv, "o:h", lo, NULL)) != -1) {
		switch (o) {
		case 'm': {
			size_t i;
			for (i = 0; i < sizeof(mode_names) / sizeof(*mode_names); i++)
				if (!strcmp(optarg, mode_names[i]))
					break;
			if (i == sizeof(mode_names) / sizeof(*mode_names))
				die("unknown mode '%s'", optarg);
			g.mode = i;
			break;
		}
		case 's': g.store = optarg; break;
		case 'c': g.chunk_size = parse_size(optarg); break;
		case 'W': g.max_write = parse_size(optarg); break;
		case 'R': g.max_readahead = parse_size(optarg); break;
		case 'r': g.max_read = parse_size(optarg); break;
		case 't': g.threads = atoi(optarg); break;
		case 'C': g.clone_fd = true; break;
		case 'D': g.direct_io = true; break;
		case 'K': g.keep_cache = true; break;
		case 'G': g.gift = true; break;
		case 'M': g.memcache_limit = parse_size(optarg); break;
		case 'P': g.preload = optarg; break;
		case 'q': g.uring_q_depth = atoi(optarg); break;
		case 'j': g.stats_json = optarg; break;
		case 'd': g.debug = true; break;
		case 'o': add_opt(&g.fuse_opts, optarg); break;
		default: usage(argv[0]);
		}
	}
	if (optind != argc - 1 || !g.store)
		usage(argv[0]);
	g.mountpoint = argv[optind];
	if (g.chunk_size < 4096 || g.chunk_size % 4096)
		die("--chunk-size must be a multiple of 4096");
	if (g.gift && g.mode != M_VMSPLICE)
		die("--gift only applies to --mode vmsplice");
	if (g.preload && g.mode != M_MEMCACHE)
		die("--preload only applies to --mode memcache");

	bool uring = g.mode == M_URING || g.mode == M_URING_BUFPOOL || g.mode == M_URING_ZC;
#ifndef CHUNKFS_ZC
	if (g.mode == M_URING_ZC)
		die("uring-zc: this binary is built against libfuse master, which has no io-uring "
		    "zero-copy API; use chunkfs-zc (built from joannekoong/libfuse zero_copy_v7)");
#endif
#ifndef FUSE_CAP_IO_URING_BUFPOOL
	if (g.mode == M_URING_BUFPOOL)
		die("uring-bufpool: this libfuse has no FUSE_CAP_IO_URING_BUFPOOL");
#endif
	if (uring && !read_param_y("/sys/module/fuse/parameters/enable_uring"))
		die("%s: /sys/module/fuse/parameters/enable_uring is not Y; as root: "
		    "echo Y > /sys/module/fuse/parameters/enable_uring", mode_names[g.mode]);
	if ((g.mode == M_PASSTHROUGH || g.mode == M_URING_ZC) && geteuid() != 0)
		die("%s needs CAP_SYS_ADMIN; run as root", mode_names[g.mode]);
	if (g.mode == M_PASSTHROUGH && g.direct_io)
		die("passthrough: --direct-io (FOPEN_DIRECT_IO) disables passthrough");

	struct rlimit rl;
	if (!getrlimit(RLIMIT_NOFILE, &rl)) {
		rl.rlim_cur = rl.rlim_max;
		setrlimit(RLIMIT_NOFILE, &rl);
	}

	g.store_fd = open(g.store, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (g.store_fd < 0)
		die("open %s: %s", g.store, strerror(errno));
	scan_store();
	probe_pipe();
	if (g.preload)
		preload();

	setenv("FUSE_INIT_STATUS", "1", 1);
	fuse_set_log_func(log_func);

	char *fopts = NULL;
	add_opt(&fopts, "ro,fsname=chunkfs,subtype=chunkfs,default_permissions");
	if (g.max_read) {
		char b[64];
		snprintf(b, sizeof(b), "max_read=%u", g.max_read);
		add_opt(&fopts, b);
	}
	if (uring)
		add_opt(&fopts, "io_uring");
	if (g.mode == M_URING_BUFPOOL)
		add_opt(&fopts, "io_uring_bufpool");
	if (g.mode == M_URING_ZC)
		add_opt(&fopts, "io_uring_zero_copy");
	if (uring) {
		/*
		 * Last: the zero_copy_v7 draft stores the int-sized fuse_opt value of
		 * io_uring_zero_copy into a bool that precedes q_depth, zeroing it.
		 */
		char b[64];
		snprintf(b, sizeof(b), "io_uring_q_depth=%u", g.uring_q_depth);
		add_opt(&fopts, b);
	}
	if (g.fuse_opts)
		add_opt(&fopts, g.fuse_opts);

	char *fargv[] = { argv[0], (char *)"-o", fopts, g.debug ? (char *)"-d" : NULL, NULL };
	struct fuse_args args = FUSE_ARGS_INIT(g.debug ? 4 : 3, fargv);
	struct fuse_session *se = fuse_session_new(&args, &ops, sizeof(ops), NULL);
	if (!se)
		die("fuse_session_new failed (options: %s)", fopts);
	g.se = se;
	if (fuse_set_signal_handlers(se))
		die("fuse_set_signal_handlers failed");

	static sigset_t set;
	sigemptyset(&set);
	sigaddset(&set, SIGUSR1);
	sigaddset(&set, SIGUSR2);
	pthread_sigmask(SIG_BLOCK, &set, NULL);
	pthread_t st;
	pthread_create(&st, NULL, signal_thread, &set);

	if (fuse_session_mount(se, g.mountpoint))
		die("mount on %s failed", g.mountpoint);
	fprintf(stderr, "chunkfs: mode=%s store=%s files=%zu chunk=%u mounted on %s\n",
		mode_names[g.mode], g.store, g.nfiles, g.chunk_size, g.mountpoint);

	struct fuse_loop_config *cfg = fuse_loop_cfg_create();
	fuse_loop_cfg_set_clone_fd(cfg, g.clone_fd);
	if (g.threads)
		fuse_loop_cfg_set_max_threads(cfg, g.threads);
	int ret = fuse_session_loop_mt(se, cfg);
	fuse_loop_cfg_destroy(cfg);

	fuse_session_unmount(se);
	write_stats();
	if (g.init_error[0]) {
		fprintf(stderr, "chunkfs: refused at FUSE_INIT: %s\n", g.init_error);
		ret = 3;
	}
	fuse_remove_signal_handlers(se);
	fuse_session_destroy(se);
	return ret ? 1 : 0;
}

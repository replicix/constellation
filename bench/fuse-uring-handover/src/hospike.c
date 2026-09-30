// SPDX-License-Identifier: GPL-2.0
/*
 * hospike: does a FUSE-over-io_uring connection survive a session handover?
 *
 * Plan 38 milestone Z0a. A raw-uapi FUSE server (no libfuse) that serves a
 * one-file read-only filesystem over io_uring, then hands its /dev/fuse fd to
 * a freshly exec'd copy of itself and goes away, the way Constellation's
 * FuseSession::detach -> Session::from_fd_resumed handover does. What the new
 * process can do with the connection afterwards is the question.
 *
 *   hospike serve-a --mnt DIR --variant N --out DIR [--gap S] [--depth D]
 *       Process A: mount(2), FUSE_INIT with FUSE_OVER_IO_URING, register
 *       nr_queues x depth ring entries, serve. SIGUSR1 starts the handover:
 *       exec process B with the /dev/fuse fd inherited, then per variant
 *         1  A tears its ring down (io_uring_queue_exit) and exits;
 *            B only reads /dev/fuse                         -> (a), (b)
 *         2  as 1, then B registers fresh ring entries      -> (c)
 *         3  B registers first, then A tears down and exits -> overlap
 *         4  A dies by SIGKILL with its ring live; B then registers
 *   hospike serve-b ...           Process B (exec'd by A, not by hand).
 *   hospike client --file F --log L   Continuous verified reader.
 *   hospike probe --file F            One open+pread+verify, prints result.
 *
 * Every process writes key=value stats to <out>/<role>.stats (atomic rename,
 * every 250 ms and at exit) and a log to stderr.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <liburing.h>

#include "fuse_kernel.h"

#define FILE_SIZE (256ull << 20)
#define ROOT_INO 1
#define PROBE_INO 2
#define PROBE_NAME "probe"
#define MAX_WRITE (1u << 20)
#define MAX_PAGES 256
#define DEV_BUFSZ (MAX_WRITE + 4096)

/* ---------------------------------------------------------------- logging */

static const char *role = "?";

static double now_s(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return ts.tv_sec + ts.tv_nsec / 1e9;
}

static void logf_(const char *fmt, ...)
{
	char buf[1024];
	va_list ap;
	int n = snprintf(buf, sizeof(buf), "[%.3f %s] ", now_s(), role);
	va_start(ap, fmt);
	vsnprintf(buf + n, sizeof(buf) - n, fmt, ap);
	va_end(ap);
	fprintf(stderr, "%s\n", buf);
	fflush(stderr);
}
#define LOG(...) logf_(__VA_ARGS__)
#define DIE(...) do { LOG(__VA_ARGS__); exit(2); } while (0)

/* ---------------------------------------------------------------- content */

static inline uint64_t word_at(uint64_t off8)
{
	return (off8 * 0x9E3779B97F4A7C15ull) ^ 0x00C0FFEE00C0FFEEull;
}

static inline uint8_t byte_at(uint64_t off)
{
	return (uint8_t)(word_at(off & ~7ull) >> ((off & 7) * 8));
}

static void fill(uint8_t *dst, uint64_t off, size_t len)
{
	size_t i = 0;
	while (i < len && ((off + i) & 7))
		dst[i] = byte_at(off + i), i++;
	for (; i + 8 <= len; i += 8) {
		uint64_t w = word_at(off + i);
		memcpy(dst + i, &w, 8);
	}
	for (; i < len; i++)
		dst[i] = byte_at(off + i);
}

/* returns the first bad offset, or UINT64_MAX */
static uint64_t verify(const uint8_t *p, uint64_t off, size_t len)
{
	for (size_t i = 0; i < len; i += 509)
		if (p[i] != byte_at(off + i))
			return off + i;
	if (len && p[len - 1] != byte_at(off + len - 1))
		return off + len - 1;
	return UINT64_MAX;
}

/* ---------------------------------------------------------------- stats */

enum { T_DEV, T_RING, T_NR };
#define OPS 64
static atomic_ulong served[T_NR][OPS];      /* by transport and opcode */
static atomic_ulong served_other[T_NR];     /* opcode >= OPS */
static atomic_ulong reg_submitted, reg_immediate_ok_pending, reg_err_count;
static atomic_int reg_first_err;            /* first negative res seen */
static atomic_ulong cqe_err_count;
static atomic_int cqe_first_err;
static atomic_int ring_ready;
static atomic_int phase;                    /* free-form, see phase_names */
static const char *phase_names[] = {
	"start", "serving", "handover", "b-started", "a-gone", "registering",
	"registered", "tearing-down", "exiting", "dev-only",
};
static char stats_path[PATH_MAX];
static double t_handover, t_a_gone, t_registered;
static int nr_queues, depth = 2;
static size_t payload_sz;

static void write_stats(void)
{
	char tmp[PATH_MAX + 16];
	FILE *f;

	if (!stats_path[0])
		return;
	snprintf(tmp, sizeof(tmp), "%s.%d", stats_path, gettid());
	f = fopen(tmp, "w");
	if (!f)
		return;
	fprintf(f, "role=%s\npid=%d\nphase=%s\nring_ready=%d\n", role, getpid(),
		phase_names[atomic_load(&phase)], atomic_load(&ring_ready));
	fprintf(f, "nr_queues=%d\ndepth=%d\npayload_sz=%zu\n", nr_queues, depth,
		payload_sz);
	for (int t = 0; t < T_NR; t++) {
		const char *tn = t == T_DEV ? "dev" : "ring";
		unsigned long tot = 0, reqs = 0;
		for (int o = 0; o < OPS; o++) {
			unsigned long v = atomic_load(&served[t][o]);
			tot += v;
			if (o != FUSE_FORGET && o != FUSE_BATCH_FORGET &&
			    o != FUSE_INTERRUPT && o != FUSE_INIT)
				reqs += v;
			if (v)
				fprintf(f, "%s_op_%d=%lu\n", tn, o, v);
		}
		tot += atomic_load(&served_other[t]);
		fprintf(f, "%s_total=%lu\n%s_reqs=%lu\n", tn, tot, tn, reqs);
		fprintf(f, "%s_read=%lu\n", tn, atomic_load(&served[t][FUSE_READ]));
	}
	fprintf(f, "reg_submitted=%lu\nreg_pending=%lu\nreg_err_count=%lu\nreg_first_err=%d\n",
		atomic_load(&reg_submitted), atomic_load(&reg_immediate_ok_pending),
		atomic_load(&reg_err_count), atomic_load(&reg_first_err));
	fprintf(f, "cqe_err_count=%lu\ncqe_first_err=%d\n",
		atomic_load(&cqe_err_count), atomic_load(&cqe_first_err));
	fprintf(f, "t_handover=%.3f\nt_a_gone=%.3f\nt_registered=%.3f\n",
		t_handover, t_a_gone, t_registered);
	fclose(f);
	rename(tmp, stats_path);
}

static void *stats_thread(void *arg)
{
	(void)arg;
	for (;;) {
		write_stats();
		usleep(250000);
	}
	return NULL;
}

static void set_phase(int p)
{
	atomic_store(&phase, p);
	LOG("phase -> %s", phase_names[p]);
	write_stats();
}

/* ---------------------------------------------------------------- fs ops */

static void fill_attr(struct fuse_attr *a, uint64_t ino)
{
	memset(a, 0, sizeof(*a));
	a->ino = ino;
	a->nlink = ino == ROOT_INO ? 2 : 1;
	a->mode = ino == ROOT_INO ? (S_IFDIR | 0755) : (S_IFREG | 0444);
	a->size = ino == ROOT_INO ? 0 : FILE_SIZE;
	a->blocks = a->size / 512;
	a->blksize = 4096;
}

static size_t add_dirent(uint8_t *buf, size_t cap, uint64_t ino, uint64_t off,
			 const char *name, uint32_t type)
{
	size_t nl = strlen(name);
	size_t sz = FUSE_DIRENT_ALIGN(FUSE_NAME_OFFSET + nl);
	struct fuse_dirent *d = (struct fuse_dirent *)buf;
	if (sz > cap)
		return 0;
	memset(buf, 0, sz);
	d->ino = ino;
	d->off = off;
	d->namelen = nl;
	d->type = type;
	memcpy(d->name, name, nl);
	return sz;
}

/* op-header size per opcode, for the /dev/fuse path where the op header and
 * the remaining in-args are contiguous */
static size_t op_hdr_size(uint32_t opcode)
{
	switch (opcode) {
	case FUSE_GETATTR: return sizeof(struct fuse_getattr_in);
	case FUSE_OPEN: case FUSE_OPENDIR: return sizeof(struct fuse_open_in);
	case FUSE_READ: case FUSE_READDIR: return sizeof(struct fuse_read_in);
	case FUSE_RELEASE: case FUSE_RELEASEDIR: return sizeof(struct fuse_release_in);
	case FUSE_FLUSH: return sizeof(struct fuse_flush_in);
	case FUSE_ACCESS: return sizeof(struct fuse_access_in);
	case FUSE_GETXATTR: return sizeof(struct fuse_getxattr_in);
	case FUSE_FORGET: return sizeof(struct fuse_forget_in);
	case FUSE_BATCH_FORGET: return sizeof(struct fuse_batch_forget_in);
	case FUSE_INTERRUPT: return sizeof(struct fuse_interrupt_in);
	default: return 0; /* LOOKUP, STATFS, DESTROY: args start in "rest" */
	}
}

/*
 * Serve one request. `op` is the per-op header, `rest` the remaining in-args.
 * The reply body goes to `out` (capacity MAX_WRITE); returns the negative
 * errno or the body length. *noreply for FORGET/INTERRUPT.
 */
static int handle(int t, const struct fuse_in_header *in, const void *op,
		  const void *rest, size_t rest_len, uint8_t *out, bool *noreply)
{
	uint32_t opc = in->opcode;
	*noreply = false;
	if (opc < OPS)
		atomic_fetch_add(&served[t][opc], 1);
	else
		atomic_fetch_add(&served_other[t], 1);

	switch (opc) {
	case FUSE_LOOKUP: {
		const char *name = rest;
		struct fuse_entry_out *e = (void *)out;
		if (in->nodeid != ROOT_INO || rest_len == 0 ||
		    strncmp(name, PROBE_NAME, rest_len) != 0)
			return -ENOENT;
		memset(e, 0, sizeof(*e));
		e->nodeid = PROBE_INO;
		e->generation = 1;
		e->entry_valid = 1;
		e->attr_valid = 1;
		fill_attr(&e->attr, PROBE_INO);
		return sizeof(*e);
	}
	case FUSE_GETATTR: {
		struct fuse_attr_out *a = (void *)out;
		if (in->nodeid != ROOT_INO && in->nodeid != PROBE_INO)
			return -ENOENT;
		memset(a, 0, sizeof(*a));
		a->attr_valid = 1;
		fill_attr(&a->attr, in->nodeid);
		return sizeof(*a);
	}
	case FUSE_OPEN: case FUSE_OPENDIR: {
		struct fuse_open_out *o = (void *)out;
		if ((opc == FUSE_OPEN) != (in->nodeid == PROBE_INO))
			return opc == FUSE_OPEN ? -EISDIR : -ENOTDIR;
		memset(o, 0, sizeof(*o));
		o->fh = in->nodeid;
		/* every read reaches the daemon, no page cache in between */
		o->open_flags = opc == FUSE_OPEN ? FOPEN_DIRECT_IO : 0;
		return sizeof(*o);
	}
	case FUSE_READ: {
		const struct fuse_read_in *r = op;
		uint64_t len;
		if (r->offset >= FILE_SIZE)
			return 0;
		len = r->size;
		if (len > MAX_WRITE)
			len = MAX_WRITE;
		if (r->offset + len > FILE_SIZE)
			len = FILE_SIZE - r->offset;
		fill(out, r->offset, len);
		return (int)len;
	}
	case FUSE_READDIR: {
		const struct fuse_read_in *r = op;
		size_t n = 0, cap = r->size < MAX_WRITE ? r->size : MAX_WRITE, s;
		static const struct { uint64_t ino; const char *name; uint32_t type; } ents[] = {
			{ ROOT_INO, ".", S_IFDIR >> 12 },
			{ ROOT_INO, "..", S_IFDIR >> 12 },
			{ PROBE_INO, PROBE_NAME, S_IFREG >> 12 },
		};
		for (uint64_t i = r->offset; i < 3; i++) {
			s = add_dirent(out + n, cap - n, ents[i].ino, i + 1,
				       ents[i].name, ents[i].type);
			if (!s)
				break;
			n += s;
		}
		return (int)n;
	}
	case FUSE_STATFS: {
		struct fuse_statfs_out *s = (void *)out;
		memset(s, 0, sizeof(*s));
		s->st.bsize = s->st.frsize = 4096;
		s->st.namelen = 255;
		s->st.blocks = FILE_SIZE / 4096;
		return sizeof(*s);
	}
	case FUSE_RELEASE: case FUSE_RELEASEDIR: case FUSE_FLUSH:
	case FUSE_ACCESS: case FUSE_DESTROY:
		return 0;
	case FUSE_FORGET: case FUSE_BATCH_FORGET: case FUSE_INTERRUPT:
		*noreply = true;
		return 0;
	default:
		return -ENOSYS;
	}
}

/* ---------------------------------------------------------------- /dev/fuse */

static int devfd = -1;
static atomic_int dev_stop;

static void dev_reply(uint64_t unique, int res, const uint8_t *body)
{
	struct fuse_out_header oh = { .unique = unique };
	struct iovec iov[2] = { { &oh, sizeof(oh) }, { (void *)body, 0 } };
	int cnt = 1;
	if (res < 0) {
		oh.error = res;
	} else if (res > 0) {
		iov[1].iov_len = res;
		cnt = 2;
	}
	oh.len = sizeof(oh) + (res > 0 ? res : 0);
	if (writev(devfd, iov, cnt) < 0 && errno != ENOENT)
		LOG("dev writev unique=%" PRIu64 ": %s", unique, strerror(errno));
}

/* poll()+read() so the loop can be stopped without closing the shared fd */
static void *dev_thread(void *arg)
{
	uint8_t *buf = aligned_alloc(4096, DEV_BUFSZ);
	uint8_t *out = aligned_alloc(4096, MAX_WRITE);
	(void)arg;
	while (!atomic_load(&dev_stop)) {
		struct pollfd p = { .fd = devfd, .events = POLLIN };
		int r = poll(&p, 1, 100);
		ssize_t n;
		if (r <= 0)
			continue;
		if (p.revents & (POLLERR | POLLHUP)) {
			LOG("dev poll revents=0x%x, connection gone", p.revents);
			break;
		}
		if (atomic_load(&dev_stop))
			break;
		n = read(devfd, buf, DEV_BUFSZ);
		if (n < 0) {
			if (errno == EINTR || errno == EAGAIN || errno == ENOENT)
				continue;
			LOG("dev read: %s", strerror(errno));
			break;
		}
		struct fuse_in_header *in = (void *)buf;
		size_t oh = op_hdr_size(in->opcode);
		uint8_t *body = buf + sizeof(*in);
		size_t body_len = n - sizeof(*in);
		bool noreply;
		int res = handle(T_DEV, in, body, body + oh,
				 body_len > oh ? body_len - oh : 0, out, &noreply);
		if (in->opcode != FUSE_FORGET && in->opcode != FUSE_BATCH_FORGET &&
		    in->opcode != FUSE_INTERRUPT)
			LOG("served over /dev/fuse: op=%u unique=%" PRIu64 " nodeid=%" PRIu64,
			    in->opcode, in->unique, in->nodeid);
		if (!noreply)
			dev_reply(in->unique, res, out);
	}
	free(buf);
	free(out);
	return NULL;
}

/* ---------------------------------------------------------------- ring */

struct ent {
	int qid;
	bool dead;
	struct fuse_uring_req_header *hdr;
	uint8_t *payload;
	struct iovec iov[2];
};

static struct io_uring ring;
static struct ent *ents;
static int nr_ents, ents_live;

static struct io_uring_sqe *get_sqe(void)
{
	struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
	if (!sqe) {
		io_uring_submit(&ring);
		sqe = io_uring_get_sqe(&ring);
	}
	if (!sqe)
		DIE("no sqe");
	return sqe;
}

static void prep_cmd(struct io_uring_sqe *sqe, int idx, uint32_t cmd_op,
		     uint64_t commit_id)
{
	struct ent *e = &ents[idx];
	struct fuse_uring_cmd_req *req;

	memset(sqe, 0, 128); /* SQE128: 64 B sqe + 80 B cmd area */
	sqe->opcode = IORING_OP_URING_CMD;
	sqe->fd = devfd;
	sqe->cmd_op = cmd_op;
	sqe->addr = (uint64_t)(uintptr_t)e->iov;
	sqe->len = 2;
	sqe->user_data = idx + 1;
	req = (struct fuse_uring_cmd_req *)sqe->cmd;
	req->qid = e->qid;
	req->commit_id = commit_id;
}

static void ring_setup(void)
{
	struct io_uring_params p = { .flags = IORING_SETUP_SQE128 | IORING_SETUP_CQSIZE };
	int r;

	nr_ents = nr_queues * depth;
	p.cq_entries = nr_ents * 4;
	r = io_uring_queue_init_params(nr_ents + 8, &ring, &p);
	if (r)
		DIE("io_uring_queue_init: %s", strerror(-r));
	ents = calloc(nr_ents, sizeof(*ents));
	for (int i = 0; i < nr_ents; i++) {
		struct ent *e = &ents[i];
		e->qid = i / depth;
		e->hdr = aligned_alloc(4096, 4096);
		e->payload = aligned_alloc(4096, payload_sz);
		memset(e->hdr, 0, 4096);
		e->iov[0] = (struct iovec){ e->hdr, sizeof(*e->hdr) };
		e->iov[1] = (struct iovec){ e->payload, payload_sz };
	}
}

/*
 * Submit REGISTER for every entry. A REGISTER that the kernel accepts returns
 * -EIOCBQUEUED internally: no CQE until a request is delivered. A rejected one
 * completes at once with res < 0. So "accepted" = no CQE within the window.
 */
static void ring_register_all(void)
{
	struct io_uring_cqe *cqe;
	struct __kernel_timespec ts = { .tv_sec = 0, .tv_nsec = 300000000 };
	int r;

	for (int i = 0; i < nr_ents; i++)
		prep_cmd(get_sqe(), i, FUSE_IO_URING_CMD_REGISTER, 0);
	r = io_uring_submit(&ring);
	LOG("REGISTER: submitted %d sqes (io_uring_submit=%d) for %d queues x %d",
	    nr_ents, r, nr_queues, depth);
	atomic_fetch_add(&reg_submitted, nr_ents);
	ents_live = nr_ents;
	/* collect immediate failures; a success-CQE here is a delivered request,
	 * leave it for the serve loop */
	while (io_uring_wait_cqe_timeout(&ring, &cqe, &ts) == 0) {
		if (cqe->res >= 0)
			break;
		int idx = cqe->user_data - 1;
		if (atomic_fetch_add(&reg_err_count, 1) == 0)
			atomic_store(&reg_first_err, cqe->res);
		LOG("REGISTER qid=%d rejected: res=%d (%s)", ents[idx].qid,
		    cqe->res, strerror(-cqe->res));
		ents[idx].dead = true;
		ents_live--;
		io_uring_cqe_seen(&ring, cqe);
	}
	atomic_store(&reg_immediate_ok_pending, ents_live);
	LOG("REGISTER: %d accepted (pending), %lu rejected", ents_live,
	    atomic_load(&reg_err_count));
}

static void ring_serve_cqe(struct io_uring_cqe *cqe)
{
	int idx = cqe->user_data - 1;
	struct ent *e = &ents[idx];
	struct fuse_in_header in;
	struct fuse_out_header *oh = (void *)e->hdr->in_out;
	struct fuse_uring_ent_in_out *eio = &e->hdr->ring_ent_in_out;
	uint64_t commit_id;
	bool noreply;
	int res;

	if (cqe->res < 0) {
		if (atomic_fetch_add(&cqe_err_count, 1) == 0)
			atomic_store(&cqe_first_err, cqe->res);
		LOG("ring cqe qid=%d res=%d (%s)", e->qid, cqe->res, strerror(-cqe->res));
		e->dead = true;
		ents_live--;
		return;
	}
	memcpy(&in, e->hdr->in_out, sizeof(in));
	commit_id = eio->commit_id;
	res = handle(T_RING, &in, e->hdr->op_in, e->payload, eio->payload_sz,
		     e->payload, &noreply);
	if (noreply)
		LOG("unexpected no-reply op %u over the ring", in.opcode);
	memset(oh, 0, sizeof(*oh));
	oh->unique = in.unique;
	oh->error = res < 0 ? res : 0;
	oh->len = sizeof(*oh) + (res > 0 ? res : 0);
	eio->payload_sz = res > 0 ? res : 0;
	prep_cmd(get_sqe(), idx, FUSE_IO_URING_CMD_COMMIT_AND_FETCH, commit_id);
}

static volatile sig_atomic_t handover_req, term_req;

/* Serve until `stop()` is true (checked every `tick_ms`) or all entries die. */
static void ring_loop(bool (*stop)(void), int tick_ms)
{
	struct io_uring_cqe *cqe;
	while (ents_live > 0 && !term_req) {
		struct __kernel_timespec ts = { 0, (long long)tick_ms * 1000000 };
		int r = io_uring_submit_and_wait_timeout(&ring, &cqe, 1, &ts, NULL);
		unsigned head, n = 0;
		if (r < 0 && r != -ETIME && r != -EINTR)
			DIE("io_uring_submit_and_wait_timeout: %s", strerror(-r));
		io_uring_for_each_cqe(&ring, head, cqe) {
			ring_serve_cqe(cqe);
			n++;
		}
		io_uring_cq_advance(&ring, n);
		if (n)
			io_uring_submit(&ring);
		if (stop && stop())
			return;
	}
	LOG("ring loop ends: live entries %d, term %d", ents_live, (int)term_req);
}

/* ---------------------------------------------------------------- process A */

static int variant, gap_s = 3;
static char outdir[PATH_MAX], self_exe[PATH_MAX];

static void on_usr1(int s) { (void)s; handover_req = 1; }
static void on_term(int s) { (void)s; term_req = 1; }

static bool stop_on_handover(void) { return handover_req; }

static int b_sync_rd = -1;  /* A reads B's progress bytes */
static char b_last = 0;
static bool b_poll(char want)
{
	char c;
	while (read(b_sync_rd, &c, 1) == 1)
		b_last = c;
	return b_last == want || (want == 'R' && b_last == 'X');
}
static bool stop_on_b_ready(void) { return b_poll('S'); }
static bool stop_on_b_registered(void) { return b_poll('R'); }

static void do_init(void)
{
	uint8_t *buf = aligned_alloc(4096, DEV_BUFSZ);
	ssize_t n;
	struct fuse_in_header *in = (void *)buf;
	struct fuse_init_in *ii = (void *)(buf + sizeof(*in));
	struct {
		struct fuse_out_header h;
		struct fuse_init_out o;
	} rep = { 0 };
	uint64_t kflags;

	do
		n = read(devfd, buf, DEV_BUFSZ);
	while (n < 0 && errno == EINTR);
	if (n < 0 || in->opcode != FUSE_INIT)
		DIE("expected FUSE_INIT, got n=%zd op=%u (%s)", n, in->opcode, strerror(errno));
	atomic_fetch_add(&served[T_DEV][FUSE_INIT], 1);
	kflags = ii->flags;
	if (ii->flags & FUSE_INIT_EXT)
		kflags |= (uint64_t)ii->flags2 << 32;
	LOG("FUSE_INIT: kernel %u.%u flags=0x%" PRIx64 " over_io_uring=%d",
	    ii->major, ii->minor, kflags, !!(kflags & FUSE_OVER_IO_URING));
	if (!(kflags & FUSE_OVER_IO_URING))
		DIE("kernel does not offer FUSE_OVER_IO_URING (fuse.enable_uring=Y?)");

	uint64_t want = kflags & (FUSE_ASYNC_READ | FUSE_BIG_WRITES | FUSE_MAX_PAGES |
				  FUSE_INIT_EXT | FUSE_OVER_IO_URING | FUSE_PARALLEL_DIROPS);
	rep.o.major = FUSE_KERNEL_VERSION;
	rep.o.minor = FUSE_KERNEL_MINOR_VERSION;
	rep.o.max_readahead = ii->max_readahead;
	rep.o.flags = (uint32_t)want;
	rep.o.flags2 = (uint32_t)(want >> 32);
	rep.o.max_background = 16;
	rep.o.congestion_threshold = 12;
	rep.o.max_write = MAX_WRITE;
	rep.o.time_gran = 1;
	rep.o.max_pages = MAX_PAGES;
	rep.h.unique = in->unique;
	rep.h.len = sizeof(rep);
	if (write(devfd, &rep, sizeof(rep)) != sizeof(rep))
		DIE("INIT reply: %s", strerror(errno));
	free(buf);
}

static void spawn_b(void)
{
	int a_alive[2], b_sync[2];
	char fdbuf[16], vbuf[16], gbuf[16], abuf[16], sbuf[16], dbuf[16], pbuf[32];
	pid_t pid;

	/* a_alive: B sees EOF when A is gone (A's write end is CLOEXEC in B) */
	if (pipe2(a_alive, O_CLOEXEC) || pipe2(b_sync, O_CLOEXEC))
		DIE("pipe: %s", strerror(errno));
	fcntl(b_sync[0], F_SETFL, O_NONBLOCK);
	b_sync_rd = b_sync[0];
	pid = fork();
	if (pid < 0)
		DIE("fork: %s", strerror(errno));
	if (pid == 0) {
		char logp[PATH_MAX + 16];
		int lfd;
		/* the three fds B keeps across exec */
		fcntl(devfd, F_SETFD, 0);
		fcntl(a_alive[0], F_SETFD, 0);
		fcntl(b_sync[1], F_SETFD, 0);
		snprintf(logp, sizeof(logp), "%s/b.log", outdir);
		lfd = open(logp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
		if (lfd >= 0)
			dup2(lfd, 2);
		setsid(); /* survive A and its process group */
		snprintf(fdbuf, sizeof(fdbuf), "%d", devfd);
		snprintf(vbuf, sizeof(vbuf), "%d", variant);
		snprintf(gbuf, sizeof(gbuf), "%d", gap_s);
		snprintf(abuf, sizeof(abuf), "%d", a_alive[0]);
		snprintf(sbuf, sizeof(sbuf), "%d", b_sync[1]);
		snprintf(dbuf, sizeof(dbuf), "%d", depth);
		snprintf(pbuf, sizeof(pbuf), "%zu", payload_sz);
		execl(self_exe, "hospike", "serve-b", "--fd", fdbuf, "--variant", vbuf,
		      "--gap", gbuf, "--a-alive-fd", abuf, "--sync-fd", sbuf,
		      "--depth", dbuf, "--payload", pbuf, "--out", outdir, (char *)NULL);
		_exit(127);
	}
	close(a_alive[0]);
	close(b_sync[1]);
	/* a_alive[1] stays open (and CLOEXEC) until A dies */
	LOG("spawned B pid=%d", pid);
}

static int serve_a(const char *mnt)
{
	char opts[256];
	pthread_t dt, st;

	role = "A";
	snprintf(stats_path, sizeof(stats_path), "%s/a.stats", outdir);
	nr_queues = get_nprocs_conf();
	payload_sz = MAX_WRITE; /* = max(8 KiB, max_write, max_pages * 4 KiB) */
	signal(SIGUSR1, on_usr1);
	signal(SIGTERM, on_term);
	signal(SIGPIPE, SIG_IGN);

	devfd = open("/dev/fuse", O_RDWR | O_CLOEXEC);
	if (devfd < 0)
		DIE("open /dev/fuse: %s", strerror(errno));
	snprintf(opts, sizeof(opts),
		 "fd=%d,rootmode=40000,user_id=0,group_id=0,allow_other", devfd);
	if (mount("hospike", mnt, "fuse.hospike", MS_NOSUID | MS_NODEV, opts))
		DIE("mount %s: %s", mnt, strerror(errno));
	LOG("mounted %s (variant %d, %d queues x depth %d)", mnt, variant, nr_queues, depth);
	pthread_create(&st, NULL, stats_thread, NULL);

	do_init();
	ring_setup();
	ring_register_all();
	if (ents_live != nr_ents)
		DIE("A could not register its ring");
	atomic_store(&ring_ready, 1);
	pthread_create(&dt, NULL, dev_thread, NULL);
	set_phase(1);

	ring_loop(stop_on_handover, 100);
	if (term_req)
		goto out;

	/* ---- handover ---- */
	t_handover = now_s();
	set_phase(2);
	atomic_store(&dev_stop, 1); /* A stops reading /dev/fuse; B takes over */
	pthread_join(dt, NULL);
	spawn_b();
	/* keep serving the ring while B starts (and, for 3, registers) */
	ring_loop(variant == 3 ? stop_on_b_registered : stop_on_b_ready, 5);
	LOG("B reports '%c'", b_last);
	write_stats();
	if (variant == 4) {
		set_phase(8);
		LOG("variant 4: SIGKILL self with the ring live");
		raise(SIGKILL);
	}
	set_phase(7);
	io_uring_queue_exit(&ring);
	LOG("io_uring_queue_exit done");
out:
	set_phase(8);
	return 0;
}

/* ---------------------------------------------------------------- process B */

static bool stop_never(void) { return false; }

static int serve_b(int a_alive_fd, int sync_fd)
{
	pthread_t dt, st;
	char c;

	role = "B";
	snprintf(stats_path, sizeof(stats_path), "%s/b.stats", outdir);
	nr_queues = get_nprocs_conf();
	signal(SIGTERM, on_term);
	signal(SIGPIPE, SIG_IGN);
	prctl(PR_SET_NAME, "hospike-b");
	LOG("B up: fd=%d variant=%d gap=%ds queues=%d depth=%d payload=%zu",
	    devfd, variant, gap_s, nr_queues, depth, payload_sz);
	pthread_create(&st, NULL, stats_thread, NULL);
	pthread_create(&dt, NULL, dev_thread, NULL);
	set_phase(3);
	if (write(sync_fd, "S", 1) != 1)
		LOG("sync write: %s", strerror(errno));

	if (variant == 3) {
		ring_setup();
		set_phase(5);
		ring_register_all();
		t_registered = now_s();
		set_phase(6);
		if (write(sync_fd, ents_live ? "R" : "X", 1) != 1)
			LOG("sync write: %s", strerror(errno));
	}
	/* wait for A to be gone */
	while (read(a_alive_fd, &c, 1) > 0)
		;
	t_a_gone = now_s();
	set_phase(4);

	if (variant == 1) {
		set_phase(9);
		while (!term_req)
			pause();
		goto out;
	}
	if (variant == 2 || variant == 4) {
		LOG("A gone; registering in %d s", gap_s);
		sleep(gap_s);
		ring_setup();
		set_phase(5);
		ring_register_all();
		t_registered = now_s();
		set_phase(6);
	}
	ring_loop(stop_never, 100);
	while (!term_req)
		pause();
out:
	set_phase(8);
	return 0;
}

/* ---------------------------------------------------------------- clients */

static atomic_ulong c_ok, c_err, c_bad, c_opens;
static atomic_int c_last_err;
static _Atomic double c_inflight_since; /* 0 = idle */

static void *client_report(void *arg)
{
	FILE *f = arg;
	for (;;) {
		double t = now_s(), s = atomic_load(&c_inflight_since);
		fprintf(f, "t=%.3f ok=%lu err=%lu bad=%lu opens=%lu last_err=%d inflight_ms=%.0f\n",
			t, atomic_load(&c_ok), atomic_load(&c_err), atomic_load(&c_bad),
			atomic_load(&c_opens), atomic_load(&c_last_err),
			s > 0 ? (t - s) * 1000 : 0.0);
		fflush(f);
		usleep(100000);
	}
	return NULL;
}

static int run_client(const char *file, const char *logp, size_t bs, int reopen)
{
	uint8_t *buf = aligned_alloc(4096, bs);
	FILE *lf = logp ? fopen(logp, "w") : stdout;
	pthread_t rt;
	uint64_t off = 0;
	int fd = -1, n_since_open = 0;

	if (!lf)
		DIE("open %s: %s", logp, strerror(errno));
	pthread_create(&rt, NULL, client_report, lf);
	for (;;) {
		if (fd < 0 || n_since_open >= reopen) {
			if (fd >= 0) {
				atomic_store(&c_inflight_since, now_s());
				close(fd);
				atomic_store(&c_inflight_since, 0);
			}
			atomic_store(&c_inflight_since, now_s());
			fd = open(file, O_RDONLY);
			atomic_store(&c_inflight_since, 0);
			n_since_open = 0;
			if (fd < 0) {
				atomic_fetch_add(&c_err, 1);
				atomic_store(&c_last_err, errno);
				usleep(50000);
				continue;
			}
			atomic_fetch_add(&c_opens, 1);
		}
		atomic_store(&c_inflight_since, now_s());
		ssize_t n = pread(fd, buf, bs, off);
		atomic_store(&c_inflight_since, 0);
		n_since_open++;
		if (n < 0) {
			atomic_fetch_add(&c_err, 1);
			atomic_store(&c_last_err, errno);
			close(fd);
			fd = -1;
			usleep(50000);
			continue;
		}
		if ((size_t)n != bs || verify(buf, off, n) != UINT64_MAX)
			atomic_fetch_add(&c_bad, 1);
		else
			atomic_fetch_add(&c_ok, 1);
		off = (off + bs) % FILE_SIZE;
	}
	return 0;
}

static int run_probe(const char *file)
{
	uint8_t buf[4096];
	double t0 = now_s();
	int fd = open(file, O_RDONLY);
	ssize_t n;
	if (fd < 0) {
		printf("open_err=%d(%s) ms=%.0f\n", errno, strerror(errno), (now_s() - t0) * 1000);
		return 1;
	}
	n = pread(fd, buf, sizeof(buf), 4096 * 7);
	if (n < 0) {
		printf("read_err=%d(%s) ms=%.0f\n", errno, strerror(errno), (now_s() - t0) * 1000);
		return 1;
	}
	close(fd);
	printf("%s n=%zd ms=%.0f\n",
	       verify(buf, 4096 * 7, n) == UINT64_MAX && n == 4096 ? "ok" : "BAD", n,
	       (now_s() - t0) * 1000);
	return 0;
}

/* ---------------------------------------------------------------- main */

int main(int argc, char **argv)
{
	static const struct option lo[] = {
		{ "mnt", 1, 0, 'm' }, { "variant", 1, 0, 'v' }, { "out", 1, 0, 'o' },
		{ "gap", 1, 0, 'g' }, { "depth", 1, 0, 'd' }, { "fd", 1, 0, 'f' },
		{ "a-alive-fd", 1, 0, 'a' }, { "sync-fd", 1, 0, 's' },
		{ "payload", 1, 0, 'p' }, { "file", 1, 0, 'F' }, { "log", 1, 0, 'l' },
		{ "bs", 1, 0, 'b' }, { "reopen", 1, 0, 'r' }, { 0 },
	};
	const char *cmd, *mnt = NULL, *file = NULL, *logp = NULL;
	int a_alive_fd = -1, sync_fd = -1, reopen = 64, c;
	size_t bs = 128 << 10;

	if (argc < 2) {
		fprintf(stderr, "usage: hospike serve-a|serve-b|client|probe [opts]\n");
		return 2;
	}
	cmd = argv[1];
	optind = 2;
	while ((c = getopt_long(argc, argv, "", lo, NULL)) != -1) {
		switch (c) {
		case 'm': mnt = optarg; break;
		case 'v': variant = atoi(optarg); break;
		case 'o': snprintf(outdir, sizeof(outdir), "%s", optarg); break;
		case 'g': gap_s = atoi(optarg); break;
		case 'd': depth = atoi(optarg); break;
		case 'f': devfd = atoi(optarg); break;
		case 'a': a_alive_fd = atoi(optarg); break;
		case 's': sync_fd = atoi(optarg); break;
		case 'p': payload_sz = strtoull(optarg, NULL, 0); break;
		case 'F': file = optarg; break;
		case 'l': logp = optarg; break;
		case 'b': bs = strtoull(optarg, NULL, 0); break;
		case 'r': reopen = atoi(optarg); break;
		default: return 2;
		}
	}
	if (readlink("/proc/self/exe", self_exe, sizeof(self_exe) - 1) < 0)
		DIE("readlink /proc/self/exe");
	if (!strcmp(cmd, "serve-a")) {
		if (!mnt || !outdir[0] || variant < 1 || variant > 4)
			DIE("serve-a needs --mnt, --out and --variant 1..4");
		return serve_a(mnt);
	}
	if (!strcmp(cmd, "serve-b")) {
		if (devfd < 0 || a_alive_fd < 0 || sync_fd < 0 || !payload_sz)
			DIE("serve-b is exec'd by serve-a");
		return serve_b(a_alive_fd, sync_fd);
	}
	role = "C";
	if (!strcmp(cmd, "client") && file)
		return run_client(file, logp, bs, reopen);
	if (!strcmp(cmd, "probe") && file)
		return run_probe(file);
	fprintf(stderr, "unknown command or missing --file\n");
	return 2;
}

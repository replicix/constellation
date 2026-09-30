/*
 * mkdata: generate and verify the chunkfs store.
 *
 *   mkdata gen STORE CHUNK_SIZE [--no-whole] NAME:SIZE[:COUNT]...
 *       writes STORE/<name>_<i>.<idx> chunks (+ STORE/<name>_<i>.whole) and
 *       STORE/.meta; files that already exist with the right size are kept.
 *   mkdata verify PATH NAME [--direct] [--random N] [--seed S]
 *       reads PATH sequentially (1 MiB) and at N random offsets/lengths
 *       (default 2000, lengths up to 2 MiB, crossing chunk boundaries) and
 *       compares against the generator; exits 1 on the first mismatch.
 *
 * Content is a pure function of (name, offset), so verify needs no store.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

static uint64_t splitmix(uint64_t x)
{
	x += 0x9e3779b97f4a7c15ull;
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
	x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
	return x ^ (x >> 31);
}

static uint64_t name_seed(const char *name)
{
	uint64_t h = 1469598103934665603ull;
	for (; *name; name++)
		h = (h ^ (unsigned char)*name) * 1099511628211ull;
	return h;
}

static void fill(uint64_t seed, uint64_t off, char *buf, size_t len)
{
	while (len) {
		uint64_t w = splitmix(seed ^ (off / 8));
		size_t in = off % 8, n = 8 - in;
		if (n > len)
			n = len;
		memcpy(buf, (char *)&w + in, n);
		buf += n;
		off += n;
		len -= n;
	}
}

static uint64_t parse_size(const char *s, char **end)
{
	uint64_t v = strtoull(s, end, 0);
	switch (**end) {
	case 'k': case 'K': (*end)++; return v << 10;
	case 'm': case 'M': (*end)++; return v << 20;
	case 'g': case 'G': (*end)++; return v << 30;
	}
	return v;
}

static int write_full(int fd, const char *buf, size_t len)
{
	while (len) {
		ssize_t r = write(fd, buf, len);
		if (r < 0) {
			if (errno == EINTR)
				continue;
			return -1;
		}
		buf += r;
		len -= r;
	}
	return 0;
}

static bool has_size(int dfd, const char *name, uint64_t size)
{
	struct stat st;
	return !fstatat(dfd, name, &st, 0) && (uint64_t)st.st_size == size;
}

static int gen_file(int dfd, const char *name, uint64_t size, uint64_t cs, bool whole)
{
	uint64_t seed = name_seed(name), nchunks = (size + cs - 1) / cs;
	char p[PATH_MAX];
	bool ok = true;
	for (uint64_t i = 0; i < nchunks && ok; i++) {
		snprintf(p, sizeof(p), "%s.%" PRIu64, name, i);
		ok = has_size(dfd, p, i + 1 < nchunks ? cs : size - i * cs);
	}
	snprintf(p, sizeof(p), "%s.%" PRIu64, name, nchunks);
	if (ok && faccessat(dfd, p, F_OK, 0) == 0)
		ok = false;
	snprintf(p, sizeof(p), "%s.whole", name);
	if (ok && (!whole || has_size(dfd, p, size)))
		return 0;

	size_t bsz = 1 << 20;
	char *buf = malloc(bsz);
	int wfd = -1;
	if (whole) {
		wfd = openat(dfd, p, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
		if (wfd < 0) {
			perror(p);
			return -1;
		}
	}
	for (uint64_t i = 0; ; i++) {
		snprintf(p, sizeof(p), "%s.%" PRIu64, name, i);
		if (i >= nchunks) {
			if (unlinkat(dfd, p, 0) < 0)
				break;
			continue;
		}
		int fd = openat(dfd, p, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
		if (fd < 0) {
			perror(p);
			return -1;
		}
		uint64_t start = i * cs, end = start + cs < size ? start + cs : size;
		for (uint64_t o = start; o < end; o += bsz) {
			size_t n = end - o < bsz ? end - o : bsz;
			fill(seed, o, buf, n);
			if (write_full(fd, buf, n) || (wfd >= 0 && write_full(wfd, buf, n))) {
				perror(p);
				return -1;
			}
		}
		close(fd);
	}
	if (wfd >= 0)
		close(wfd);
	free(buf);
	return 1;
}

static int cmd_gen(int argc, char **argv)
{
	if (argc < 4) {
		fprintf(stderr, "usage: mkdata gen STORE CHUNK_SIZE [--no-whole] NAME:SIZE[:COUNT]...\n");
		return 2;
	}
	const char *store = argv[2];
	char *end;
	uint64_t cs = parse_size(argv[3], &end);
	if (*end || cs < 4096 || cs % 4096) {
		fprintf(stderr, "bad chunk size %s\n", argv[3]);
		return 2;
	}
	mkdir(store, 0755);
	if (access(store, F_OK)) {
		perror(store);
		return 1;
	}
	int dfd = open(store, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if (dfd < 0) {
		perror(store);
		return 1;
	}
	char meta[64];
	snprintf(meta, sizeof(meta), "chunk_size=%" PRIu64 "\n", cs);
	char old[64] = "";
	int mfd = openat(dfd, ".meta", O_RDONLY);
	if (mfd >= 0) {
		ssize_t r = read(mfd, old, sizeof(old) - 1);
		old[r > 0 ? r : 0] = 0;
		close(mfd);
		if (strcmp(old, meta)) {
			fprintf(stderr, "%s was generated with a different chunk size (%s); remove it first\n",
				store, old);
			return 1;
		}
	}
	bool whole = true;
	uint64_t made = 0, kept = 0;
	for (int i = 4; i < argc; i++) {
		if (!strcmp(argv[i], "--no-whole")) {
			whole = false;
			continue;
		}
		char *spec = strdup(argv[i]), *c1 = strchr(spec, ':');
		if (!c1) {
			fprintf(stderr, "bad spec %s\n", argv[i]);
			return 2;
		}
		*c1 = 0;
		uint64_t size = parse_size(c1 + 1, &end), count = 1;
		if (*end == ':')
			count = strtoull(end + 1, &end, 0);
		if (*end || !size) {
			fprintf(stderr, "bad spec %s\n", argv[i]);
			return 2;
		}
		for (uint64_t k = 0; k < count; k++) {
			char name[256];
			snprintf(name, sizeof(name), "%s_%" PRIu64, spec, k);
			int r = gen_file(dfd, name, size, cs, whole);
			if (r < 0)
				return 1;
			r ? made++ : kept++;
		}
		free(spec);
	}
	mfd = openat(dfd, ".meta", O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (mfd < 0 || write_full(mfd, meta, strlen(meta))) {
		perror(".meta");
		return 1;
	}
	close(mfd);
	syncfs(dfd);
	fprintf(stderr, "mkdata: %s: %" PRIu64 " files written, %" PRIu64 " already present\n",
		store, made, kept);
	return 0;
}

static int check(int fd, uint64_t seed, uint64_t off, size_t len, char *got, char *want,
		 const char *path)
{
	size_t done = 0;
	while (done < len) {
		ssize_t r = pread(fd, got + done, len - done, off + done);
		if (r < 0) {
			fprintf(stderr, "verify %s: pread off=%" PRIu64 " len=%zu: %s\n", path, off, len,
				strerror(errno));
			return -1;
		}
		if (r == 0)
			break;
		done += r;
	}
	if (done != len) {
		fprintf(stderr, "verify %s: short read at off=%" PRIu64 ": %zu of %zu\n", path, off, done, len);
		return -1;
	}
	fill(seed, off, want, len);
	if (memcmp(got, want, len)) {
		size_t i = 0;
		while (got[i] == want[i])
			i++;
		fprintf(stderr, "verify %s: MISMATCH at offset %" PRIu64 " (read off=%" PRIu64 " len=%zu)\n",
			path, off + i, off, len);
		return -1;
	}
	return 0;
}

static int cmd_verify(int argc, char **argv)
{
	if (argc < 4) {
		fprintf(stderr, "usage: mkdata verify PATH NAME [--direct] [--random N] [--seed S]\n");
		return 2;
	}
	const char *path = argv[2];
	uint64_t seed = name_seed(argv[3]), rseed = 1;
	bool direct = false;
	long nrand = 2000;
	for (int i = 4; i < argc; i++) {
		if (!strcmp(argv[i], "--direct"))
			direct = true;
		else if (!strcmp(argv[i], "--random") && i + 1 < argc)
			nrand = atol(argv[++i]);
		else if (!strcmp(argv[i], "--seed") && i + 1 < argc)
			rseed = strtoull(argv[++i], NULL, 0);
		else {
			fprintf(stderr, "unknown argument %s\n", argv[i]);
			return 2;
		}
	}
	int fd = open(path, O_RDONLY | O_CLOEXEC | (direct ? O_DIRECT : 0));
	struct stat st;
	if (fd < 0 || fstat(fd, &st) < 0) {
		fprintf(stderr, "verify %s: %s\n", path, strerror(errno));
		return 1;
	}
	uint64_t size = st.st_size;
	size_t maxlen = 2 << 20;
	char *got, *want;
	if (posix_memalign((void **)&got, 4096, maxlen) || posix_memalign((void **)&want, 4096, maxlen))
		return 1;
	for (uint64_t o = 0; o < size; o += 1 << 20) {
		size_t n = size - o < (1 << 20) ? size - o : (1 << 20);
		if (check(fd, seed, o, n, got, want, path))
			return 1;
	}
	uint64_t x = rseed;
	for (long i = 0; i < nrand && size; i++) {
		x = splitmix(x);
		uint64_t off = x % size;
		x = splitmix(x);
		size_t len = 1 + x % maxlen;
		if (direct) {
			off &= ~4095ull;
			len = (len + 4095) & ~4095ull;
		}
		if (off + len > size)
			len = size - off;
		if (check(fd, seed, off, len, got, want, path))
			return 1;
	}
	char c;
	if (pread(fd, &c, 1, size) != 0) {
		fprintf(stderr, "verify %s: read past EOF returned data\n", path);
		return 1;
	}
	close(fd);
	printf("verify %s: ok (%" PRIu64 " bytes, %ld random reads%s)\n", path, size, nrand,
	       direct ? ", O_DIRECT" : "");
	return 0;
}

int main(int argc, char **argv)
{
	if (argc > 1 && !strcmp(argv[1], "gen"))
		return cmd_gen(argc, argv);
	if (argc > 1 && !strcmp(argv[1], "verify"))
		return cmd_verify(argc, argv);
	fprintf(stderr, "usage: mkdata gen|verify ...\n");
	return 2;
}

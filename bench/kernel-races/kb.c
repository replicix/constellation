// Plan 42 section 8 workloads that need threads and per-operation latencies.
// Not part of the daemon. Build: gcc -O2 -pthread -o kb kb.c
//
//   kb reopen-prep DIR N SIZE       create N files of SIZE bytes, fsync, close
//   kb reopen-pass DIR N SIZE       one pass: open, read SIZE bytes, close, per file
//   kb openlat FILE N               N open/close of one file; mean, p50, p99 (ns)
//   kb pcrace FILE SECS OPENER      race 1's workload (writers, readers, optional opener)
//   kb dbprep FILE BYTES            write BYTES of data in 1 MiB writes, fsync, close
//   kb db FILE SECS THREADS         4 KiB random read/write 50/50 on one open handle
//   kb commit WDIR RDIR N           N small files written and closed in WDIR; RDIR is
//                                   another view of the same file system; times both sides
// Every command prints one RESULT line of key=value pairs.
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static uint64_t now_ns(void) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    return (uint64_t)t.tv_sec * 1000000000ull + t.tv_nsec;
}
static void die(const char *m) { perror(m); exit(2); }

typedef struct { uint64_t *v; size_t n, cap; } Lat;
static void lat_add(Lat *l, uint64_t x) {
    if (l->n == l->cap) {
        l->cap = l->cap ? l->cap * 2 : 1 << 16;
        l->v = realloc(l->v, l->cap * sizeof(uint64_t));
        if (!l->v) die("realloc");
    }
    l->v[l->n++] = x;
}
static int cmp64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return x < y ? -1 : x > y;
}
static void lat_merge(Lat *dst, Lat *src) {
    for (size_t i = 0; i < src->n; i++) lat_add(dst, src->v[i]);
}
static void lat_print(const char *name, Lat *l) {
    if (!l->n) { printf(" %s_n=0", name); return; }
    qsort(l->v, l->n, sizeof(uint64_t), cmp64);
    double sum = 0;
    for (size_t i = 0; i < l->n; i++) sum += l->v[i];
    printf(" %s_n=%zu %s_mean_ns=%.0f %s_p50_ns=%llu %s_p99_ns=%llu %s_max_ns=%llu", name, l->n,
           name, sum / l->n, name, (unsigned long long)l->v[l->n / 2], name,
           (unsigned long long)l->v[(size_t)(l->n * 0.99)], name,
           (unsigned long long)l->v[l->n - 1]);
}
static uint64_t xs(uint64_t *s) {
    uint64_t x = *s;
    x ^= x << 13; x ^= x >> 7; x ^= x << 17;
    return *s = x;
}
static volatile int stop_flag;

// ---- reopen-read
static void reopen_prep(const char *dir, int n, size_t size) {
    mkdir(dir, 0755);
    char *buf = malloc(size);
    for (size_t i = 0; i < size; i++) buf[i] = (char)(i * 131 + 7);
    char p[4096];
    for (int i = 0; i < n; i++) {
        snprintf(p, sizeof p, "%s/f%05d", dir, i);
        int fd = open(p, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd < 0) die(p);
        if (write(fd, buf, size) != (ssize_t)size) die("write");
        close(fd);
    }
    int d = open(dir, O_RDONLY);
    if (d >= 0) { fsync(d); close(d); }
    printf("RESULT reopen_prep files=%d\n", n);
}
static void reopen_pass(const char *dir, int n, size_t size) {
    char *buf = malloc(size);
    char p[4096];
    uint64_t t0 = now_ns();
    long bad = 0;
    for (int i = 0; i < n; i++) {
        snprintf(p, sizeof p, "%s/f%05d", dir, i);
        int fd = open(p, O_RDONLY);
        if (fd < 0) die(p);
        ssize_t r = pread(fd, buf, size, 0);
        if (r != (ssize_t)size) bad++;
        close(fd);
    }
    printf("RESULT reopen_pass secs=%.4f short_reads=%ld\n", (now_ns() - t0) / 1e9, bad);
}

// ---- open latency
static void openlat(const char *file, long n) {
    Lat l = {0};
    int fd = open(file, O_RDONLY);
    if (fd < 0) die(file);
    char b[16];
    pread(fd, b, sizeof b, 0);
    close(fd);
    uint64_t t0 = now_ns();
    for (long i = 0; i < n; i++) {
        uint64_t a = now_ns();
        fd = open(file, O_RDONLY);
        if (fd < 0) die(file);
        close(fd);
        lat_add(&l, now_ns() - a);
    }
    printf("RESULT openlat total_secs=%.4f", (now_ns() - t0) / 1e9);
    lat_print("open_close", &l);
    printf("\n");
}

// ---- pcrace
#define PAGES 64
#define WRITERS 4
#define READERS 2
#define PG 4096
static const char *g_file;
static int g_opener;
typedef struct { int id; Lat wlat; uint64_t writes, stale, opens; } PcT;
static void *pc_writer(void *a) {
    PcT *t = a;
    int fd = open(g_file, O_RDWR);
    if (fd < 0) die(g_file);
    uint64_t s = 0x9e3779b97f4a7c15ull * (t->id + 1);
    unsigned char byte = t->id * 61, data[PG / 2], back[PG / 2];
    while (!stop_flag) {
        int page = t->id + WRITERS * (xs(&s) % (PAGES / WRITERS));
        byte++; if (!byte) byte = 1;
        memset(data, byte, sizeof data);
        uint64_t a0 = now_ns();
        if (pwrite(fd, data, sizeof data, (off_t)page * PG) != sizeof data) die("pwrite");
        lat_add(&t->wlat, now_ns() - a0);
        t->writes++;
        ssize_t r = pread(fd, back, sizeof back, (off_t)page * PG);
        if (r != sizeof back || memcmp(back, data, sizeof back)) t->stale++;
    }
    close(fd);
    return NULL;
}
static void *pc_reader(void *a) {
    PcT *t = a;
    int fd = open(g_file, O_RDONLY);
    if (fd < 0) die(g_file);
    uint64_t s = 0x1234567ull * (t->id + 11);
    char b[PG / 2];
    while (!stop_flag) pread(fd, b, sizeof b, (off_t)(xs(&s) % PAGES) * PG + PG / 2);
    close(fd);
    return NULL;
}
static void *pc_opener(void *a) {
    PcT *t = a;
    while (!stop_flag) {
        int fd = open(g_file, O_RDONLY);
        if (fd < 0) die(g_file);
        close(fd);
        t->opens++;
    }
    return NULL;
}
static void pcrace(const char *file, int secs, int opener) {
    g_file = file; g_opener = opener;
    unsigned char *d = calloc(PAGES, PG);
    for (int p = 0; p < PAGES; p++) memset(d + p * PG + PG / 2, p + 1, PG / 2);
    int fd = open(file, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0 || write(fd, d, PAGES * PG) != PAGES * PG) die("prep");
    fsync(fd); close(fd);
    pthread_t th[WRITERS + READERS + 1];
    PcT t[WRITERS + READERS + 1];
    memset(t, 0, sizeof t);
    int nt = 0;
    for (int i = 0; i < WRITERS; i++, nt++) { t[nt].id = i; pthread_create(&th[nt], 0, pc_writer, &t[nt]); }
    for (int i = 0; i < READERS; i++, nt++) { t[nt].id = i; pthread_create(&th[nt], 0, pc_reader, &t[nt]); }
    if (opener) { pthread_create(&th[nt], 0, pc_opener, &t[nt]); nt++; }
    uint64_t t0 = now_ns();
    sleep(secs);
    stop_flag = 1;
    for (int i = 0; i < nt; i++) pthread_join(th[i], 0);
    Lat all = {0};
    uint64_t w = 0, st = 0, op = 0;
    for (int i = 0; i < nt; i++) { lat_merge(&all, &t[i].wlat); w += t[i].writes; st += t[i].stale; op += t[i].opens; }
    printf("RESULT pcrace secs=%.2f opener=%d writes=%llu stale=%llu opens=%llu", (now_ns() - t0) / 1e9,
           opener, (unsigned long long)w, (unsigned long long)st, (unsigned long long)op);
    lat_print("write", &all);
    printf("\n");
}

// ---- database style
static void dbprep(const char *file, uint64_t bytes) {
    int fd = open(file, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) die(file);
    size_t mb = 1 << 20;
    uint64_t *buf = malloc(mb), s = 88172645463325252ull;
    for (uint64_t off = 0; off < bytes; off += mb) {
        for (size_t i = 0; i < mb / 8; i++) buf[i] = xs(&s);
        if (pwrite(fd, buf, mb, off) != (ssize_t)mb) die("pwrite");
    }
    fsync(fd); close(fd);
    printf("RESULT dbprep bytes=%llu\n", (unsigned long long)bytes);
}
static uint64_t g_dbsize;
static int g_dbfd;
typedef struct { int id; Lat r, w; } DbT;
static void *db_thread(void *a) {
    DbT *t = a;
    uint64_t s = 0xdeadbeefcafeull * (t->id + 3);
    char *buf = aligned_alloc(4096, 4096);
    memset(buf, 0x5a + t->id, 4096);
    uint64_t blocks = g_dbsize / 4096;
    while (!stop_flag) {
        uint64_t r = xs(&s);
        off_t off = (off_t)(r % blocks) * 4096;
        uint64_t a0 = now_ns();
        if ((r >> 40) & 1) {
            if (pwrite(g_dbfd, buf, 4096, off) != 4096) die("db pwrite");
            lat_add(&t->w, now_ns() - a0);
        } else {
            if (pread(g_dbfd, buf, 4096, off) != 4096) die("db pread");
            lat_add(&t->r, now_ns() - a0);
        }
    }
    return NULL;
}
static void db(const char *file, int secs, int threads) {
    g_dbfd = open(file, O_RDWR);
    if (g_dbfd < 0) die(file);
    struct stat st;
    fstat(g_dbfd, &st);
    g_dbsize = st.st_size;
    pthread_t th[64]; DbT t[64];
    memset(t, 0, sizeof t);
    for (int i = 0; i < threads; i++) { t[i].id = i; pthread_create(&th[i], 0, db_thread, &t[i]); }
    uint64_t t0 = now_ns();
    sleep(secs);
    stop_flag = 1;
    for (int i = 0; i < threads; i++) pthread_join(th[i], 0);
    double el = (now_ns() - t0) / 1e9;
    Lat r = {0}, w = {0};
    for (int i = 0; i < threads; i++) { lat_merge(&r, &t[i].r); lat_merge(&w, &t[i].w); }
    printf("RESULT db secs=%.2f threads=%d ops_per_sec=%.0f", el, threads, (r.n + w.n) / el);
    lat_print("read", &r); lat_print("write", &w);
    printf("\n");
}

// ---- commit-heavy, two views
static const char *g_wdir, *g_rdir;
static int g_n;
static volatile int g_written;
static double g_seen_secs;
static uint64_t g_t0;
static void *c_observer(void *a) {
    (void)a;
    char p[4096];
    struct stat st;
    for (int i = 0; i < g_n;) {
        snprintf(p, sizeof p, "%s/c%06d", g_rdir, i);
        if (stat(p, &st) == 0 && st.st_size == 4096) { i++; continue; }
        if (stop_flag && g_written >= g_n) { usleep(1000); }
        else usleep(200);
        if (now_ns() - g_t0 > 600ull * 1000000000ull) { g_seen_secs = -1; return NULL; }
    }
    g_seen_secs = (now_ns() - g_t0) / 1e9;
    return NULL;
}
static void commit(const char *wdir, const char *rdir, int n) {
    g_wdir = wdir; g_rdir = rdir; g_n = n;
    char buf[4096];
    memset(buf, 'c', sizeof buf);
    pthread_t th;
    g_t0 = now_ns();
    int observe = rdir && strcmp(rdir, "-");
    if (observe) pthread_create(&th, 0, c_observer, NULL);
    Lat l = {0};
    char p[4096];
    for (int i = 0; i < n; i++) {
        snprintf(p, sizeof p, "%s/c%06d", wdir, i);
        uint64_t a = now_ns();
        int fd = open(p, O_CREAT | O_WRONLY | O_TRUNC, 0644);
        if (fd < 0) die(p);
        if (write(fd, buf, sizeof buf) != sizeof buf) die("write");
        close(fd);
        lat_add(&l, now_ns() - a);
        g_written = i + 1;
    }
    double wsecs = (now_ns() - g_t0) / 1e9;
    stop_flag = 1;
    if (observe) pthread_join(th, 0);
    printf("RESULT commit files=%d write_secs=%.4f seen_all_secs=%.4f", n, wsecs, observe ? g_seen_secs : 0.0);
    lat_print("commit", &l);
    printf("\n");
}

int main(int argc, char **argv) {
    if (argc < 3) { fprintf(stderr, "usage: see source\n"); return 2; }
    const char *c = argv[1];
    if (!strcmp(c, "reopen-prep") && argc == 5) reopen_prep(argv[2], atoi(argv[3]), atol(argv[4]));
    else if (!strcmp(c, "reopen-pass") && argc == 5) reopen_pass(argv[2], atoi(argv[3]), atol(argv[4]));
    else if (!strcmp(c, "openlat") && argc == 4) openlat(argv[2], atol(argv[3]));
    else if (!strcmp(c, "pcrace") && argc == 5) pcrace(argv[2], atoi(argv[3]), atoi(argv[4]));
    else if (!strcmp(c, "dbprep") && argc == 4) dbprep(argv[2], strtoull(argv[3], 0, 10));
    else if (!strcmp(c, "db") && argc == 5) db(argv[2], atoi(argv[3]), atoi(argv[4]));
    else if (!strcmp(c, "commit") && argc == 5) commit(argv[2], argv[3], atoi(argv[4]));
    else { fprintf(stderr, "bad arguments\n"); return 2; }
    return 0;
}

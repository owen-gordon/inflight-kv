/* kv.c: a key-value store with no storage.
 *
 * One of these unikernels runs per metro. Every record carries its own ring
 * (a list of metros), and each node POSTs it to the metro after itself in that
 * ring, forever. A value exists only while it is in a hop; a node forwards each
 * record the moment it arrives and drops it once its TTL is spent. Nodes keep
 * nothing between boots: the ring travels with the record, and the metros'
 * IPs come from the kernel args, so a boot needs no DNS.
 *
 *   PUT    /kv/<key>?ttl=<s>[&ring=a,b,c]  body = value  -> 201
 *   GET    /kv/<key>[?ring=a,b,c]                        -> 200 value | 404
 *   DELETE /kv/<key>[?ring=a,b,c]                        -> 200
 *   GET    /                                             -> node stats
 *   POST   /hop/<boot>/<seq>                             (peer traffic)
 *
 * The ring defaults to every metro in the args, in order; it must include the
 * node it is sent to, and may only name metros from the args (so nobody can
 * point the nodes at arbitrary addresses). GET and DELETE send a marker around
 * the given ring; use the ring the value was PUT on. Hops from each sender are
 * processed in order (seq), so once a node's marker comes back every record on
 * that ring has passed it once: a GET that saw nothing answers 404, and a
 * DELETE's kill window is over.
 *
 * TTLs are whole seconds. A record carries its absolute expiry time (unix
 * seconds, from the platform wall clock); any node that sees it at or past
 * that time drops it.
 *
 * Scale-to-zero: the platform stops a node once no inbound request has been
 * open for the cooldown. Every connection carries one request, so an idle node
 * holds nothing open. While a node has records it has not yet written to the
 * next node, it pins itself: it sends GET /pin to its own public address and
 * leaves that request unanswered, which keeps it running. It answers an
 * incoming hop only once pinned, and unpins as soon as its forward is written.
 * It does not wait for the next node's acknowledgement (that would keep it up
 * for another round trip): the hop is fire-and-forget, so a packet lost after
 * the node has stopped loses the records in it. The written connection is left
 * open rather than closed, because a client close can make the next metro's
 * proxy abort the request; if the node is still up when a refusal comes back,
 * it resends.
 *
 * Usage (kernel args): kv NAME DOMAIN METRO=IP[:PORT] ...
 *   The table must include NAME. METRO's hostname (for Host: and the DNS
 *   fallback) is DOMAIN.METRO.unikraft.app; an empty IP means "use DNS".
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <lwip/netdb.h> /* nolibc's netdb.h shadows lwip's and has no resolver */
#include <ctype.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <uk/plat/time.h>

#define PORT     8080
#define MAXC     128          /* connections, inbound + outbound */
#define MAXOUT   48           /* outbound hops in progress; the rest of MAXC is for inbound */
#define QCAP     (8 * BUFSZ)  /* send queue, used only while every outbound conn is busy */
#define BUFSZ    32768        /* largest request, and largest hop body */
#define TXSZ     (BUFSZ + 512)
#define VALMAX   (BUFSZ - 2048) /* leaves room for proxy headers and the record header */
#define KEYMAX   128
#define MAXOPS   64
#define HOLD     16           /* out-of-order hops held while a gap fills */
#define OP_TIMEOUT_MS  5000
#define GAP_TIMEOUT_MS 2000
#define HOP_TIMEOUT_MS 5000
#define PIN_MAX_MS     45000  /* below the proxy's read timeout */
#define TTL_MAX_S      86400
#define MAXM     8            /* metros in the table */
#define RINGMAX  64           /* "sfo,dal,was,fra,sin" plus room */
#define NSEND    16           /* sender boots tracked for ordering */

typedef long long ll;
typedef unsigned long long ull;

static const char *self;

static struct ep {
	char name[16], host[256];
	int port, ok;
	struct sockaddr_in addr;
	ull seq;                  /* next hop seq we send to this metro */
	char q[QCAP];             /* records waiting for a free outbound conn */
	size_t qlen;
} tab[MAXM], *me;
static int ntab;
static char dring[RINGMAX];   /* default ring: the table, in order */
static ull boot;
static ull st_hops, st_recs, st_ttl, st_full, st_bad;

static ll now_ms(void) { return (ll)(ukplat_monotonic_clock() / 1000000ULL); }
static ll now_s(void) { return (ll)(ukplat_wall_clock() / 1000000000ULL); }
#define EV(what) printf("@%lld %s\n", now_ms(), what) /* trace: ms since boot */

/* ---- connections ---- */

enum { FREE, IN_READ, IN_WAIT, IN_HOLD, IN_PIN, IN_WRITE, OUT_CONN, OUT_SEND, OUT_RESP, NSTATES };
/* IN_WAIT: a GET/DELETE waiting for a record or our marker.
 * IN_HOLD: a hop or PUT, answered once we are pinned (or have nothing to send).
 * IN_PIN:  our own /pin request, answered when we unpin. */

static struct conn {
	int fd, st, retried, pin;  /* pin: outbound conn carrying our /pin */
	struct ep *ep;             /* outbound: where it goes */
	char rx[BUFSZ + 1];
	size_t rlen;
	char tx[TXSZ];
	size_t tlen, toff;
	ll t;                     /* when the current request or hop started */
} C[MAXC];

/* ---- pending GET / DELETE / PUT-overwrite, each lasting one lap ---- */

enum { OP_GET = 1, OP_KILL };

static struct op {
	int type, conn;           /* conn: waiting client, or -1 */
	char key[KEYMAX];
	ull born, marker;
	ll t;
} ops[MAXOPS];

/* ---- out-of-order hops, per sender boot ---- */

static struct sender {
	ull boot, expect;
	ll t;
} S[NSEND];

static struct held {
	int used;
	struct sender *sd;
	ull seq;
	ll t;
	size_t len;
	char body[BUFSZ];
} H[HOLD];

/* ---- records ----
 * R <expires, unix s> <node> <id> <vlen> <ring> <key>\n<value>
 * M <node> <id> <ring>\n
 */

struct rec {
	char type;
	ll exp;
	char node[16];
	ull id;
	unsigned long vlen;
	char ring[RINGMAX];
	char key[KEYMAX];
	const char *val;
};

static int put_rec(char *b, size_t *n, size_t cap, ll exp, const char *node, ull id,
		   const char *ring, const char *key, const char *val, size_t vlen)
{
	char h[KEYMAX + RINGMAX + 96];
	int hl = snprintf(h, sizeof h, "R %lld %s %llu %lu %s %s\n",
			  exp, node, id, (unsigned long)vlen, ring, key);
	if (hl < 0 || *n + hl + vlen > cap)
		return -1;
	memcpy(b + *n, h, hl);
	memcpy(b + *n + hl, val, vlen);
	*n += hl + vlen;
	return 0;
}

static int put_marker(char *b, size_t *n, size_t cap, const char *node, ull id, const char *ring)
{
	int hl = snprintf(b + *n, cap - *n, "M %s %llu %s\n", node, id, ring);
	if (hl < 0 || *n + hl >= cap)
		return -1;
	*n += hl;
	return 0;
}

/* Parse the record at *off. 1 = got one, 0 = end, -1 = malformed. */
static int next_rec(char *b, size_t len, size_t *off, struct rec *r)
{
	if (*off >= len)
		return 0;
	char *line = b + *off;
	char *nl = memchr(line, '\n', len - *off);
	if (!nl)
		return -1;
	r->type = line[0];
	if (r->type == 'R') {
		if (sscanf(line, "R %lld %15s %llu %lu %63s %127s", &r->exp, r->node,
			   &r->id, &r->vlen, r->ring, r->key) != 6)
			return -1;
		r->val = nl + 1;
		if (r->vlen > (size_t)(b + len - r->val))
			return -1;
		*off = (size_t)(nl + 1 - b) + r->vlen;
		return 1;
	}
	if (r->type == 'M' && sscanf(line, "M %15s %llu %63s", r->node, &r->id, r->ring) == 3) {
		*off = (size_t)(nl + 1 - b);
		return 1;
	}
	return -1;
}

/* ---- HTTP helpers ---- */

/* Value of header `name` (lowercase) between the request line and `end`. */
static const char *hdr(const char *h, const char *end, const char *name)
{
	size_t n = strlen(name), i;

	for (const char *l = strstr(h, "\r\n"); l && l < end; l = strstr(l + 2, "\r\n")) {
		const char *s = l + 2;
		for (i = 0; i < n && s + i < end && (s[i] | 32) == name[i]; i++)
			;
		if (i == n && s[n] == ':') {
			for (s += n + 1; *s == ' '; s++)
				;
			return s;
		}
	}
	return NULL;
}

static void cclose(struct conn *c)
{
	int idx = (int)(c - C);

	if (c->fd >= 0)
		close(c->fd);
	c->fd = -1;
	c->st = FREE;
	c->rlen = c->tlen = c->toff = 0;
	c->retried = c->pin = 0;
	for (int i = 0; i < MAXOPS; i++)
		if (ops[i].type && ops[i].conn == idx)
			ops[i].conn = -1; /* client gone; a kill still runs its lap */
}

static void respond(struct conn *c, int code, const char *type, const char *body, size_t blen)
{
	const char *msg = code == 200 ? "OK" : code == 201 ? "Created" :
		code == 400 ? "Bad Request" : code == 404 ? "Not Found" :
		code == 405 ? "Method Not Allowed" : code == 413 ? "Payload Too Large" :
		code == 504 ? "Gateway Timeout" : code == 507 ? "Insufficient Storage" :
		"Service Unavailable";
	int hl = snprintf(c->tx, TXSZ, "HTTP/1.1 %d %s\r\nContent-Type: %s\r\n"
			  "Content-Length: %lu\r\nConnection: close\r\n\r\n",
			  code, msg, type, (unsigned long)blen);
	memcpy(c->tx + hl, body, blen); /* blen <= VALMAX, fits */
	c->tlen = hl + blen;
	c->toff = 0;
	c->st = IN_WRITE;
}

static void text(struct conn *c, int code, const char *s)
{
	respond(c, code, "text/plain", s, strlen(s));
}

static int nonblock(int fd)
{
	return fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
}

static struct conn *slot(void)
{
	for (int i = 0; i < MAXC; i++)
		if (C[i].st == FREE)
			return &C[i];
	return NULL;
}

/* ---- rings ---- */

static struct ep *find(const char *name, size_t n)
{
	for (int i = 0; i < ntab; i++)
		if (strlen(tab[i].name) == n && !strncmp(tab[i].name, name, n))
			return &tab[i];
	return NULL;
}

/* The metro after us in `ring` (comma-separated, wrapping), or NULL if the ring
 * names a metro outside the table, has fewer than two metros, or skips us. */
static struct ep *next_of(const char *ring)
{
	struct ep *first = NULL, *after = NULL;
	int seen = 0, n = 0;

	for (const char *s = ring; *s;) {
		size_t l = strcspn(s, ",");
		struct ep *e = find(s, l);
		if (!e)
			return NULL;
		if (!first)
			first = e;
		if (seen && !after)
			after = e;
		seen |= e == me;
		n++;
		s += l + (s[l] == ',');
	}
	if (n < 2 || !seen)
		return NULL;
	return after ? after : first;
}

/* ---- sending hops ---- */

static int resolve(struct ep *e)
{
	struct hostent *h = lwip_gethostbyname(e->host);

	if (!h || !h->h_addr_list[0]) {
		printf("dns: cannot resolve %s\n", e->host);
		return -1;
	}
	memset(&e->addr, 0, sizeof e->addr);
	e->addr.sin_family = AF_INET;
	e->addr.sin_port = htons(e->port);
	memcpy(&e->addr.sin_addr, h->h_addr_list[0], 4);
	e->ok = 1;
	unsigned char *ip = (unsigned char *)&e->addr.sin_addr;
	printf("%s -> %u.%u.%u.%u:%d\n", e->host, ip[0], ip[1], ip[2], ip[3], e->port);
	return 0;
}

static int dial(struct conn *c, struct ep *e)
{
	c->ep = e;
	if (!e->ok && resolve(e) < 0)
		return -1;
	if ((c->fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
		return -1;
	nonblock(c->fd);
	if (connect(c->fd, (struct sockaddr *)&e->addr, sizeof e->addr) != 0 &&
	    errno != EINPROGRESS) {
		e->ok = 0; /* re-resolve next time */
		return -1;
	}
	c->st = OUT_CONN;
	return 0;
}

/* Send queued records, as many as fit per hop, while outbound conns are free. */
static void flush(void)
{
	for (int d = 0; d < ntab; d++) {
		struct ep *e = &tab[d];
		while (e->qlen) {
			struct conn *c = NULL;
			int nout = 0;

			for (int i = 0; i < MAXC; i++)
				nout += C[i].st >= OUT_CONN;
			if (nout >= MAXOUT || !(c = slot())) {
				return; /* wait for a hop to finish */
			} else if (dial(c, e) < 0) {
				cclose(c);
				break; /* unreachable; try again next round */
			}

			size_t off = 0, cut = 0;
			struct rec r;
			while (next_rec(e->q, e->qlen, &off, &r) == 1 && off <= BUFSZ)
				cut = off; /* largest run of whole records that fits one hop */
			if (!cut) {        /* cannot happen: every record is <= BUFSZ */
				printf("send queue corrupt, dropped\n");
				st_full++;
				e->qlen = 0;
				cclose(c);
				break;
			}
			int hl = snprintf(c->tx, TXSZ, "POST /hop/%llu/%llu HTTP/1.1\r\nHost: %s\r\n"
					  "Content-Type: application/octet-stream\r\nContent-Length: %lu\r\n"
					  "Connection: close\r\n\r\n",
					  boot, e->seq++, e->host, (unsigned long)cut);
			memcpy(c->tx + hl, e->q, cut);
			memmove(e->q, e->q + cut, e->qlen - cut);
			e->qlen -= cut;
			c->tlen = hl + cut;
			c->toff = 0;
			c->rlen = 0;
			c->t = now_ms();
		}
	}
}

static int send_batch(struct ep *e, const char *body, size_t len)
{
	if (e->qlen + len > QCAP) {
		st_full++;
		return -1;
	}
	memcpy(e->q + e->qlen, body, len);
	e->qlen += len;
	flush();
	return 0;
}

/* A hop was refused or lost before an answer: resend it on a fresh connection. */
static void retry(struct conn *c)
{
	if (c->retried < 3) {
		close(c->fd);
		c->fd = -1;
		c->retried++;
		c->toff = 0;
		c->rlen = 0;
		if (dial(c, c->ep) == 0)
			return;
	}
	printf("hop lost after %d retries\n", c->retried);
	st_full++;
	cclose(c);
}

/* ---- the loop ---- */

static void op_free(struct op *o) { o->type = 0; }

/* A record is passing this node. 0 = drop it. */
static int arrive(struct rec *r)
{
	int own = !strcmp(r->node, self);

	if (now_s() >= r->exp) {
		st_ttl++;
		return 0;
	}
	for (int i = 0; i < MAXOPS; i++) /* kills: everything but this node's newer writes */
		if (ops[i].type == OP_KILL && !strcmp(ops[i].key, r->key) &&
		    !(own && r->id >= ops[i].born))
			return 0;
	for (int i = 0; i < MAXOPS; i++)
		if (ops[i].type == OP_GET && !strcmp(ops[i].key, r->key)) {
			if (ops[i].conn >= 0)
				respond(&C[ops[i].conn], 200, "application/octet-stream", r->val, r->vlen);
			op_free(&ops[i]);
		}
	return 1;
}

/* Our marker is back: every record has passed us since it left. */
static void marker_back(ull id)
{
	for (int i = 0; i < MAXOPS; i++) {
		struct op *o = &ops[i];
		if (!o->type || o->marker != id)
			continue;
		if (o->conn >= 0)
			text(&C[o->conn], o->type == OP_GET ? 404 : 200,
			     o->type == OP_GET ? "not found\n" : "deleted\n");
		op_free(o);
	}
}

static void process(char *body, size_t len)
{
	static char out[MAXM][BUFSZ]; /* one batch per next metro; never bigger than the input */
	size_t on[MAXM] = {0}, off = 0;
	struct rec r;
	int k;

	st_hops++;
	while ((k = next_rec(body, len, &off, &r)) == 1) {
		if (r.type == 'M' && !strcmp(r.node, self)) {
			marker_back(r.id);
			continue;
		}
		struct ep *nx = next_of(r.ring);
		if (!nx) {
			st_bad++; /* a ring that does not route through us */
			continue;
		}
		int d = (int)(nx - tab);
		if (r.type == 'M') {
			put_marker(out[d], &on[d], BUFSZ, r.node, r.id, r.ring);
			continue;
		}
		st_recs++;
		if (arrive(&r))
			put_rec(out[d], &on[d], BUFSZ, r.exp, r.node, r.id, r.ring, r.key, r.val, r.vlen);
	}
	if (k < 0)
		printf("malformed hop, dropped the rest\n");
	for (int d = 0; d < ntab; d++)
		if (on[d])
			send_batch(&tab[d], out[d], on[d]);
}

static void drain(struct sender *sd)
{
	for (int again = 1; again;) {
		again = 0;
		for (int i = 0; i < HOLD; i++)
			if (H[i].used && H[i].sd == sd && H[i].seq == sd->expect) {
				H[i].used = 0;
				sd->expect++;
				process(H[i].body, H[i].len);
				again = 1;
			}
	}
}

/* Give up on a gap: continue from this sender's lowest held hop. 0 = none held. */
static int skip_gap(struct sender *sd)
{
	int lo = -1;

	for (int i = 0; i < HOLD; i++)
		if (H[i].used && H[i].sd == sd && (lo < 0 || H[i].seq < H[lo].seq))
			lo = i;
	if (lo < 0)
		return 0;
	sd->expect = H[lo].seq;
	drain(sd);
	return 1;
}

/* Ordering state for sender boot b; a new boot takes the stalest slot. */
static struct sender *sender(ull b, ull s)
{
	struct sender *old = &S[0];

	for (int i = 0; i < NSEND; i++) {
		if (S[i].boot == b)
			return &S[i];
		if (S[i].t < old->t)
			old = &S[i];
	}
	while (skip_gap(old)) /* release whatever the evicted boot still held */
		;
	old->boot = b;
	old->expect = s;
	return old;
}

static void hop_in(ull b, ull s, char *body, size_t len)
{
	struct sender *sd = sender(b, s);

	sd->t = now_ms();
	if (s <= sd->expect) { /* in order (or late after a skipped gap) */
		if (s == sd->expect)
			sd->expect++;
		process(body, len);
		drain(sd);
		return;
	}
	for (int i = 0; i < HOLD; i++)
		if (!H[i].used) {
			H[i].used = 1;
			H[i].sd = sd;
			H[i].seq = s;
			H[i].t = now_ms();
			H[i].len = len;
			memcpy(H[i].body, body, len);
			return;
		}
	skip_gap(sd); /* hold buffer full */
	process(body, len);
}

static struct op *op_new(int type, const char *key, struct conn *c)
{
	for (int i = 0; i < MAXOPS; i++)
		if (!ops[i].type) {
			struct op *o = &ops[i];
			o->type = type;
			o->conn = c ? (int)(c - C) : -1;
			strcpy(o->key, key);
			o->born = o->marker = ukplat_monotonic_clock();
			o->t = now_ms();
			return o;
		}
	return NULL;
}

/* ---- requests ---- */

static void handle_kv(struct conn *c, const char *method, const char *rest,
		      const char *body, size_t blen)
{
	char key[KEYMAX], ring[RINGMAX];
	size_t n = 0;
	static char b[BUFSZ];
	size_t bl = 0;

	while (rest[n] && rest[n] != '?' && n < KEYMAX - 1 &&
	       (isalnum((unsigned char)rest[n]) || strchr("-_.", rest[n])))
		key[n] = rest[n], n++;
	key[n] = 0;
	if (!n || (rest[n] && rest[n] != '?'))
		return text(c, 400, "key: 1-127 chars of [A-Za-z0-9._-]\n");

	const char *rp = strstr(rest + n, "ring=");
	size_t rl = rp ? strcspn(rp + 5, "&") : 0;
	if (!rp)
		strcpy(ring, dring);
	else if (rl && rl < RINGMAX)
		memcpy(ring, rp + 5, rl), ring[rl] = 0;
	else
		ring[0] = 0;
	struct ep *nx = next_of(ring);
	if (!nx)
		return text(c, 400, "ring: 2+ comma-separated metros from the table, including this one\n");

	if (!strcmp(method, "PUT") || !strcmp(method, "POST")) {
		const char *q = strstr(rest + n, "ttl=");
		ll ttl = q ? strtoll(q + 4, NULL, 10) : 0;
		if (ttl < 1 || ttl > TTL_MAX_S)
			return text(c, 400, "need ?ttl=<seconds>, 1..86400\n");
		if (blen > VALMAX)
			return text(c, 413, "value too large\n");
		struct op *o = op_new(OP_KILL, key, NULL); /* drop older copies for a lap */
		if (!o)
			return text(c, 503, "busy\n");
		struct rec r = { .type = 'R', .exp = now_s() + ttl, .id = ukplat_monotonic_clock(),
				 .vlen = blen, .val = body };
		strcpy(r.node, self);
		strcpy(r.key, key);
		arrive(&r); /* a GET waiting here sees it now */
		put_rec(b, &bl, sizeof b, r.exp, self, r.id, ring, key, body, blen);
		put_marker(b, &bl, sizeof b, self, o->marker, ring);
		if (send_batch(nx, b, bl) < 0) {
			op_free(o);
			return text(c, 507, "loop full\n");
		}
		text(c, 201, "stored in flight\n");
		c->st = IN_HOLD; /* sent once we are pinned */
		return;
	}
	if (strcmp(method, "GET") && strcmp(method, "DELETE"))
		return text(c, 405, "PUT, GET or DELETE\n");

	struct op *o = op_new(method[0] == 'G' ? OP_GET : OP_KILL, key, c);
	if (!o)
		return text(c, 503, "busy\n");
	put_marker(b, &bl, sizeof b, self, o->marker, ring);
	if (send_batch(nx, b, bl) < 0) {
		op_free(o);
		return text(c, 503, "cannot reach peer\n");
	}
	c->st = IN_WAIT; /* answered when the record or our marker comes by */
}

static void handle(struct conn *c)
{
	char *e = strstr(c->rx, "\r\n\r\n");

	if (!e) {
		if (c->rlen >= BUFSZ)
			text(c, 413, "too large\n");
		return;
	}
	char *body = e + 4;
	const char *v = hdr(c->rx, e, "content-length");
	long cl = v ? strtol(v, NULL, 10) : 0;
	if (cl < 0 || cl > (long)(c->rx + BUFSZ - body))
		return text(c, 413, "too large\n");
	if (c->rlen - (size_t)(body - c->rx) < (size_t)cl)
		return; /* body not complete yet */

	char method[8], path[KEYMAX + RINGMAX + 64];
	char *sp = strchr(c->rx, ' '), *sp2 = sp ? strchr(sp + 1, ' ') : NULL;
	if (!sp2 || sp - c->rx >= (long)sizeof method || sp2 - sp - 1 >= (long)sizeof path)
		return text(c, 400, "bad request line\n");
	memcpy(method, c->rx, sp - c->rx);
	method[sp - c->rx] = 0;
	memcpy(path, sp + 1, sp2 - sp - 1);
	path[sp2 - sp - 1] = 0;

	ull b, s;
	if (!strcmp(method, "POST") && sscanf(path, "/hop/%llu/%llu", &b, &s) == 2) {
		EV("hop in");
		text(c, 200, "ok\n");
		c->st = IN_HOLD; /* the sender's acknowledgement, sent once we are pinned */
		hop_in(b, s, body, cl);
	} else if (!strcmp(path, "/pin")) {
		EV("pinned");
		text(c, 200, "unpinned\n");
		c->st = IN_PIN;
	} else if (!strncmp(path, "/kv/", 4)) {
		handle_kv(c, method, path + 4, body, cl);
	} else if (!strcmp(path, "/")) {
		char s[512];
		int nops = 0, ns[NSTATES] = {0};
		size_t q = 0;
		for (int i = 0; i < MAXOPS; i++)
			nops += !!ops[i].type;
		for (int i = 0; i < MAXC; i++)
			ns[C[i].st]++;
		for (int i = 0; i < ntab; i++)
			q += tab[i].qlen;
		snprintf(s, sizeof s, "node=%s ring=%s clock=%lld hops=%llu records=%llu "
			 "expired=%llu dropped_full=%llu misrouted=%llu ops=%d queued_bytes=%lu "
			 "conns(read/wait/hold/pin/write/connect/send/resp)=%d/%d/%d/%d/%d/%d/%d/%d\n",
			 self, dring, now_s(), st_hops, st_recs, st_ttl, st_full, st_bad, nops,
			 (unsigned long)q,
			 ns[1], ns[2], ns[3], ns[4], ns[5], ns[6], ns[7], ns[8]);
		text(c, 200, s);
	} else {
		text(c, 404, "no such path\n");
	}
}

/* ---- event loop ---- */

static void step(struct conn *c)
{
	ssize_t r;

	switch (c->st) {
	case IN_READ:
		r = read(c->fd, c->rx + c->rlen, BUFSZ - c->rlen);
		if (r <= 0) {
			if (r < 0 && errno == EAGAIN)
				return;
			cclose(c);
			return;
		}
		c->rlen += r;
		c->rx[c->rlen] = 0;
		handle(c);
		return;
	case OUT_CONN: {
		int err = 0;
		socklen_t l = sizeof err;
		getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &l);
		if (err) {
			printf("connect to %s failed: %d\n", c->ep->host, err);
			c->ep->ok = 0;
			if (c->pin)
				cclose(c); /* re-pinned next round */
			else
				retry(c);
			return;
		}
		EV(c->pin ? "pin connected" : "hop connected");
		c->st = OUT_SEND;
	}
	/* fallthrough */
	case OUT_SEND:
	case IN_WRITE:
		r = write(c->fd, c->tx + c->toff, c->tlen - c->toff);
		if (r < 0) {
			if (errno == EAGAIN)
				return;
			if (c->st == OUT_SEND)
				retry(c);
			else
				cclose(c);
			return;
		}
		c->toff += r;
		if (c->toff < c->tlen)
			return;
		if (c->st == IN_WRITE) {
			cclose(c);
			return;
		}
		if (!c->pin)
			EV("hop written");
		c->st = OUT_RESP;
		c->rlen = 0;
		return;
	case OUT_RESP: {
		r = read(c->fd, c->rx + c->rlen, BUFSZ - c->rlen);
		if (r <= 0) {
			if (r < 0 && errno == EAGAIN)
				return;
			if (c->rlen == 0 && !c->pin) /* closed before answering */
				retry(c);
			else
				cclose(c);
			return;
		}
		c->rlen += r;
		c->rx[c->rlen] = 0;
		char *e = strstr(c->rx, "\r\n\r\n");
		if (!e)
			return;
		const char *v = hdr(c->rx, e, "content-length");
		long cl = v ? strtol(v, NULL, 10) : -1;
		size_t have = c->rlen - (size_t)(e + 4 - c->rx);
		if (cl >= 0 && have < (size_t)cl)
			return;
		if (atoi(c->rx + 9) != 200 && !c->pin) {
			printf("hop refused (%.12s), retrying\n", c->rx + 9);
			retry(c);
			return;
		}
		cclose(c); /* a hop: acknowledged, the peer has it. a pin: done */
		return;
	}
	}
}

/* Records we have not yet written to the next node. */
static int busy(void)
{
	for (int i = 0; i < ntab; i++)
		if (tab[i].qlen)
			return 1;
	for (int i = 0; i < MAXC; i++)
		if ((C[i].st == OUT_CONN || C[i].st == OUT_SEND) && !C[i].pin)
			return 1;
	for (int i = 0; i < HOLD; i++)
		if (H[i].used)
			return 1;
	return 0;
}

static void start_pin(void)
{
	struct conn *c = slot();

	if (!c || dial(c, me) < 0) {
		if (c)
			cclose(c);
		return;
	}
	c->pin = 1;
	c->tlen = snprintf(c->tx, TXSZ, "GET /pin HTTP/1.1\r\nHost: %s\r\n"
			   "Connection: close\r\n\r\n", me->host);
	c->toff = 0;
	c->rlen = 0;
	c->t = now_ms();
}

/* Pin while busy, unpin when not; answer held hops and PUTs once that is safe. */
static void pin_update(void)
{
	int need = busy(), pinned = 0, pinning = 0;

	for (int i = 0; i < MAXC; i++) {
		pinned |= C[i].st == IN_PIN;
		pinning |= C[i].st >= OUT_CONN && C[i].pin;
	}
	if (need && !pinned && !pinning)
		start_pin();
	for (int i = 0; i < MAXC; i++) {
		if (C[i].st == IN_HOLD && (pinned || !need))
			EV("answer hop"), C[i].st = IN_WRITE;
		else if (C[i].st == IN_PIN && !need)
			EV("unpin"), C[i].st = IN_WRITE; /* unpin: the proxy closes, then the cooldown */
	}
}

static void tick(void)
{
	ll t = now_ms();

	for (int i = 0; i < MAXC; i++) {
		struct conn *c = &C[i];
		if (c->st >= OUT_CONN && c->pin) {
			if (t - c->t > PIN_MAX_MS)
				cclose(c); /* the proxy will time it out; re-pin */
		} else if (c->st == OUT_RESP && t - c->t > HOP_TIMEOUT_MS) {
			cclose(c); /* written long ago and unanswered: assume delivered, never resend */
		} else if (c->st >= OUT_CONN && t - c->t > HOP_TIMEOUT_MS) {
			printf("hop stuck in state %d (sent %lu/%lu)\n", c->st,
			       (unsigned long)c->toff, (unsigned long)c->tlen);
			c->t = t;
			retry(c);
		} else if (c->st == IN_HOLD && t - c->t > 3 * HOP_TIMEOUT_MS) {
			c->st = IN_WRITE; /* never got pinned: answer anyway */
		} else if (c->st == IN_PIN && t - c->t > PIN_MAX_MS) {
			c->st = IN_WRITE;
		} else if ((c->st == IN_READ || c->st == IN_WRITE) && t - c->t > 2 * HOP_TIMEOUT_MS) {
			cclose(c);
		}
	}

	for (int i = 0; i < MAXOPS; i++)
		if (ops[i].type && t - ops[i].t > OP_TIMEOUT_MS) {
			if (ops[i].conn >= 0)
				text(&C[ops[i].conn], 504, "marker lost\n");
			op_free(&ops[i]);
		}
	for (int i = 0; i < HOLD; i++)
		if (H[i].used && t - H[i].t > GAP_TIMEOUT_MS) {
			skip_gap(H[i].sd);
			break;
		}
}

int main(int argc, char **argv)
{
	static struct pollfd p[MAXC + 1];
	int map[MAXC + 1];

	self = argc > 1 ? argv[1] : "";
	for (int i = 3; i < argc && argc <= 3 + MAXM && strlen(self) < 16 &&
			strlen(argv[2]) < 200; i++) {
		struct ep *e = &tab[ntab];
		const char *eq = strchr(argv[i], '=');
		unsigned ip[4];
		int port = 80, k;
		if (!eq || eq == argv[i] || eq - argv[i] > 15 ||
		    strlen(dring) + (size_t)(eq - argv[i]) + 2 > RINGMAX)
			break;
		memcpy(e->name, argv[i], eq - argv[i]);
		snprintf(e->host, sizeof e->host, "%s.%s.unikraft.app", argv[2], e->name);
		k = sscanf(eq + 1, "%u.%u.%u.%u:%d", &ip[0], &ip[1], &ip[2], &ip[3], &port);
		e->port = port;
		e->addr.sin_family = AF_INET;
		e->addr.sin_port = htons(port);
		unsigned char *a = (unsigned char *)&e->addr.sin_addr;
		for (int j = 0; j < 4; j++)
			a[j] = (unsigned char)ip[j];
		e->ok = k >= 4 && ip[0] < 256 && ip[1] < 256 && ip[2] < 256 && ip[3] < 256;
		snprintf(dring + strlen(dring), RINGMAX - strlen(dring), "%s%s",
			 ntab ? "," : "", e->name);
		if (!strcmp(e->name, self))
			me = e;
		ntab++;
	}
	if (!me || ntab < 2 || ntab != argc - 3) {
		printf("usage: kv NAME DOMAIN METRO=IP[:PORT] ... (2-%d metros, NAME among them)\n", MAXM);
		return 1;
	}
	boot = ukplat_wall_clock() ^ __builtin_ia32_rdtsc(); /* unique per boot */
	if (!boot)
		boot = 1;
	for (int i = 0; i < MAXC; i++)
		C[i].fd = -1;

	int lfd = socket(AF_INET, SOCK_STREAM, 0), one = 1;
	struct sockaddr_in a = { .sin_family = AF_INET, .sin_port = htons(PORT) };
	setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
	if (lfd < 0 || bind(lfd, (struct sockaddr *)&a, sizeof a) || listen(lfd, 64)) {
		printf("cannot listen on %d\n", PORT);
		return 1;
	}
	nonblock(lfd);
	printf("kv %s: ring %s, clock %lld, listening on %d\n", self, dring, now_s(), PORT);
	EV("listening");

	for (;;) {
		int n = 0;
		p[n].fd = lfd;
		p[n].events = POLLIN;
		map[n++] = -1;
		for (int i = 0; i < MAXC; i++) {
			int st = C[i].st;
			if (st == FREE || st == IN_WAIT || st == IN_HOLD || st == IN_PIN)
				continue;
			p[n].fd = C[i].fd;
			p[n].events = (st == IN_READ || st == OUT_RESP) ? POLLIN : POLLOUT;
			p[n].revents = 0;
			map[n++] = i;
		}
		p[0].revents = 0;
		if (poll(p, n, 100) < 0 && errno != EINTR)
			printf("poll: %d\n", errno);
		if (p[0].revents & POLLIN)
			for (int fd; (fd = accept(lfd, NULL, NULL)) >= 0;) {
				struct conn *c = slot();
				if (!c) {
					close(fd);
					continue;
				}
				nonblock(fd);
				c->fd = fd;
				c->st = IN_READ;
				c->rlen = 0;
				c->t = now_ms();
			}
		for (int k = 1; k < n; k++)
			if (p[k].revents && C[map[k]].fd == p[k].fd)
				step(&C[map[k]]);
		tick();
		flush();
		pin_update();
	}
}

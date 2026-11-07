# inflight-kv: a key-value store with no storage

Values live only in flight. One stateless C unikernel ([Unikraft](https://unikraft.org), nolibc + lwip)
runs per metro on Unikraft Cloud. Every value is forwarded hop to hop around a ring of metros until its
TTL runs out. No node keeps anything between boots, every node scales to zero between hops, and an empty
store means zero running instances.

The goal was to keep the nodes **down** for as much of the time as possible, waking only to bounce a
value onward. This repo is the store plus the experiments that measured how close hosted Unikraft gets.
Short answer: about **50% awake per node**. The blocker is the transport, which is TCP-only (see [UDP](#the-blocker-udp)).

## How it works

```
client ──PUT──▶ sfo ──▶ sin ──▶ was ──▶ fra ──▶ dal ──┐
                 ▲                                    │
                 └────────────────────────────────────┘   one lap ≈ 1.1 s
```

- **Each record carries its own ring** (`R <expiry> <node> <id> <len> <ring> <key>\n<value>`).
  A node sends it to the metro after itself in that ring. Rings can be chosen per value at runtime.
- **The deploy hands every node every metro's IP** in its kernel args (`kv NAME DOMAIN METRO=IP ...`),
  so a boot needs no DNS. Rings may only name metros from that table, so clients can't aim nodes at
  arbitrary addresses.
- **Hops are plain HTTP POSTs on port 80 through each metro's proxy.** The proxy is also what wakes a
  node from zero.
- **Staying up long enough to forward.** A node stays up only while an inbound request is open. So
  while it holds unsent records, it pins itself with an unanswered `GET /pin` to its own address. It
  unpins as soon as its forward is written.
- **Hops are fire-and-forget.** A node doesn't wait for the next node's reply, because that would keep
  it up for another round trip.
- **Ordered delivery.** Hops are numbered per sender boot and processed in order.
- **GET and DELETE send a marker around the ring.** When the marker returns, every record on that ring
  has passed this node once. So "not seen" means 404, and a DELETE's kill window is closed.
- **TTL is whole seconds**, stored as an absolute expiry.

## Run it

Needs `kraft`, `unikraft` (logged in), `jq`. Set `UKC_TOKEN` in your environment; nothing in this repo
holds credentials.

```sh
make login     # once: registry credentials from $UKC_TOKEN (fixes 401 on push)
make build     # build the unikernel and push $(USER_NS)/kv:latest
make deploy    # (re)create one node per metro in RING
make status    # running / standby per node
make measure   # seconds each node was awake over the next SECS (default 60)
make logs M=sin
make down      # delete every node
```

Overrides:

| Variable | Meaning |
|---|---|
| `USER_NS=you` | your Unikraft Cloud namespace |
| `RING="sfo sin was fra dal"` | metros to deploy; their order is the default ring |
| `COOLDOWN=100` | ms; the platform minimum is 100 |

`deploy` deletes each node before recreating it, so a redeploy drops everything in flight.

## Use it

```sh
N=http://$USER_NS-kv.sfo.unikraft.app                # or sin / was / fra / dal

curl -X PUT --data 'hello' "$N/kv/greeting?ttl=60"   # 201; ttl in whole seconds, 1..86400
curl "$N/kv/greeting"                                # 200 value | 404, answered within one lap
curl -X DELETE "$N/kv/greeting"                      # 200 after one lap
curl "$N/"                                           # this boot's stats

# a ring per value (metros from the deploy, including the one you call)
curl -X PUT --data 'hi' "$N/kv/k?ttl=60&ring=sfo,was,fra,sin"
curl "$N/kv/k?ring=sfo,was,fra,sin"                  # GET/DELETE must name the same ring
```

Keys are 1–127 characters of `[A-Za-z0-9._-]`. Values are at most ~30 KB.

The console prints `@<ms since boot> <event>` trace lines: listening, hop in, pinned, hop connected,
hop written, unpin. These timelines are the basis for the analysis below.

## What was tested

All numbers come from the platform's per-instance uptime counters (`make measure` diffs two snapshots),
with one value circling and a 100 ms cooldown unless noted.

| Scenario | Avg awake per node | Notes |
|---|---|---|
| 2 nodes (sin ⇄ was), waiting for each hop's ack | ~100% | an in-memory store with extra steps; the reason for the ring |
| 5-metro ring, west to east `sfo,dal,was,fra,sin` | 56–58% | sfo 40, dal 39, was 59, fra 73, sin 76 |
| Same ring, cooldown 300 ms | worse: 60–100% | removed the double wake (below) but added 200 ms per visit |
| 4 metros, no sin | ~75% | shorter lap, same per-visit cost |
| US only `sfo,dal,was` | ~97% | the lap is shorter than one wake |
| IP table in args instead of DNS at boot | ~unchanged | DNS was never the cost |
| Runtime ring `sfo,was,fra,sin` on a 5-node deploy | ~66% | dal, not in the ring, stays at 0% |
| **Hill-climb over 16 rings → zigzag `sfo,sin,was,fra,dal`** | **51%** | best found; re-measured clean ([log](experiments/hill.log)) |

**How the experiments were run**

- **Hill-climb** ([`experiments/hill.py`](experiments/hill.py)):
  - Neighbor moves: swap two metros, drop one, or insert one.
  - Each ring got 8 s of warm-up, then a 40 s window.
  - Rankings below the winner are unreliable. A DELETE between runs didn't always stop the previous
    value, so "idle" nodes show up awake in the log.
- **Proxy abort test** ([`experiments/abort.py`](experiments/abort.py)): can a sender hang up right
  after writing a hop?
  - No. If the client sends FIN or RST before ~40 ms, the request is never delivered.
  - An RST at 40 ms and later is delivered, because by then the proxy has woken the target and
    forwarded the request.
- **ICMP:** pings to a node's address are answered by the metro proxy in ~9 ms, and the node never
  wakes. There is no path in below TCP.
- **Bare wake cost:** five lone `GET /` requests to an idle node added 1122 ms of uptime, about
  **224 ms per wake**, even though each answer took ~70 ms. That is boot (~15 ms), plus the 100 ms
  cooldown, plus ~100 ms of platform start/stop accounting (inferred from counters, not documented).

**Where a visit's time goes** (trace, zigzag ring, ms since boot)

| Node | Hop in | Pinned | Forward connected and written | Charged per wake |
|---|---|---|---|---|
| dal → sfo | 21 | 23 | 63 | ~360 |
| sfo → sin | 15 | 16 | ~190 | double wake |
| sin → was | 14 | 15 | ~240 | double wake |

The node's own work is just the TCP handshake to the next metro: 40–225 ms. The pin costs 1–2 ms.
Everything else is the platform:

- the ~224 ms wake floor, and
- **the hop's reply.** TCP and HTTP force the next proxy to answer. On short hops the reply lands
  during the cooldown and stretches it. On long hops (sfo→sin, sin→was, fra→dal) it lands after the
  node has stopped, and wakes it again for nothing.

That is why sfo, sin and fra wake twice per lap.

## The blocker: UDP

Unikraft Cloud's hosted metros accept only TCP through the metro proxy: TLS on any port, or plain
HTTP on port 80 when it is the only port. Asking the API for a UDP port fails with
`'udp' is not a valid flag for 'handlers'`. UDP services are a Limited Access feature for BYOC and
on-prem only. Kraftlet (Kubernetes) doesn't help either: it maps
Services onto the same proxy, and its pods aren't reachable directly.

**What UDP would change**

- **Nothing to wait for.** A datagram needs no handshake, so a node would boot, read the datagram,
  `sendto()` the next metro, and be done in ~20 ms instead of 60–240 ms.
- **No reply.** The double wake and the stretched cooldown disappear, and so does the pin.
- **On hosted Unikraft alone, that's not enough.** Every visit still pays the ~224 ms wake floor. And
  without handshakes the lap shrinks to roughly the one-way flight time, about 0.45 s for the zigzag.
  So the ratio would land around 45–50%, about where it is now. The floor, not the protocol, is the
  limit here.
- **The real change is UDP on a platform where a wake costs milliseconds.** BYOC/on-prem Unikraft
  qualifies if its cooldown can go below 100 ms, and so does self-hosted Firecracker with snapshot
  restore. There, per-visit cost approaches boot + forward (~20–40 ms) against a ~450 ms lap: each
  node is down roughly **90–95%** of the time while a value circles, and adding metros lengthens the
  flight without adding much awake time.

These are estimates from the counters and traces above, not measurements. The ~100 ms of platform
start/stop and the UDP lap time are inferred.

On hosted TCP, the remaining gap (51% now, ~33% if replies were free) is the reply that TCP plus the
proxy force on every hop. The only reply-free trick left is never answering hops and resetting ~95 ms
after the write. It was rejected: any wake slower than 95 ms loses the hop (about 1 in 140 per the
platform's wake histogram), so a value would survive ~20 s.

## Know before relying on it

- **Hops are fire-and-forget:** a packet lost after a node stops loses the values in it.
- **Concurrent writes** to the same key from different metros within one lap can both be dropped.
- **A GET on a different ring** than the PUT can give a false 404.
- **A DELETE right after changing rings** doesn't always kill the value.
- **Plaintext HTTP, no auth:** anyone can read, write, or POST to `/hop` and `/pin`, and every request
  wakes (and bills) a node.

## Files

| File | What it is |
|---|---|
| `kv.c` | the whole node: HTTP server, ring forwarding, pin, reorder buffer |
| `Kraftfile` | Unikraft build config (`kraftcloud` and `qemu` targets) |
| `Makefile.uk` | Unikraft build config |
| `Makefile` | build, deploy, measure |
| `experiments/` | hill-climb, its log, and the proxy abort test |

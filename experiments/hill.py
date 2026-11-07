#!/usr/bin/env python3
"""Hill-climb kv rings: minimize the mean awake fraction of the ring's nodes.

One value circles the candidate ring; awake time comes from the platform's
per-instance uptime counters, diffed over a window. Neighbors: swap two
metros, drop one (min 3), or add a missing one at any position.
"""
import json, random, subprocess, sys, threading, time, urllib.error, urllib.request

METROS = ['sfo', 'dal', 'was', 'fra', 'sin']
DOMAIN = 'owen-gordon-kv'
WARM, WIN, MAX_EVALS, MARGIN = 8, 40, 30, 0.02
random.seed(1)


def log(*a):
    print(*a, flush=True)


def req(method, metro, path, data=None):
    r = urllib.request.Request(f'http://{DOMAIN}.{metro}.unikraft.app{path}', data=data, method=method)
    try:
        with urllib.request.urlopen(r, timeout=20) as f:
            return f.status, f.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:
        return 0, str(e)


def metrics(m):
    for _ in range(3):
        try:
            out = subprocess.run(['unikraft', 'api', '--metro', m, '/v1/instances/metrics'],
                                 capture_output=True, text=True, timeout=30).stdout
            for i in json.loads(out)['data']['instances']:
                if i['name'] == 'kv':
                    return time.monotonic(), i['uptime_ms'], i['start_count']
        except Exception:
            pass
    return None


def snap():
    res = {}
    ts = [threading.Thread(target=lambda m=m: res.__setitem__(m, metrics(m))) for m in METROS]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    return res


def canon(ring):
    """Same cycle regardless of rotation or direction."""
    n = len(ring)
    rots = [tuple(r[i:] + r[:i]) for r in (list(ring), list(reversed(ring))) for i in range(n)]
    return min(rots)


cache, evals = {}, 0


def evaluate(ring):
    global evals
    k = canon(ring)
    if k in cache:
        return cache[k]
    evals += 1
    rs, key = ','.join(ring), f'hill{evals}'
    st, body = req('PUT', ring[0], f'/kv/{key}?ttl=150&ring={rs}', b'climbing')
    if st != 201:
        log(f'  PUT failed on {rs}: {st} {body.strip()[:80]}')
        cache[k] = 9.0
        return 9.0
    time.sleep(WARM)
    a = snap()
    time.sleep(WIN)
    b = snap()
    gst, gbody = req('GET', ring[-1], f'/kv/{key}?ring={rs}')
    req('DELETE', ring[0], f'/kv/{key}?ring={rs}')
    per = {}
    for m in METROS:
        if a.get(m) and b.get(m):
            el = b[m][0] - a[m][0]
            per[m] = ((b[m][1] - a[m][1]) / 1000 / el, b[m][2] - a[m][2])
    if any(m not in per for m in ring):
        log(f'  metrics missing for {rs}; skipping')
        cache[k] = 9.0
        return 9.0
    score = sum(per[m][0] for m in ring) / len(ring)
    alive = gst == 200 and gbody == 'climbing'
    if not alive:
        score = 9.0  # the value was lost: disqualify
    cache[k] = score
    detail = '  '.join(f'{m} {per[m][0]*100:3.0f}%/{per[m][1]}w' for m in ring)
    idle = [m for m in METROS if m not in ring and per.get(m, (0,))[0] > 0.01]
    log(f'#{evals:2d} {rs:<22} mean {score*100:5.1f}%  [{detail}]'
        + ('' if alive else '  VALUE LOST') + (f'  (idle nodes awake: {idle})' if idle else ''))
    return score


def neighbors(ring):
    out = []
    n = len(ring)
    for i in range(n):
        for j in range(i + 1, n):
            r = list(ring)
            r[i], r[j] = r[j], r[i]
            out.append(r)
    if n > 3:
        out += [ring[:i] + ring[i + 1:] for i in range(n)]
    for m in METROS:
        if m not in ring:
            out += [ring[:i] + [m] + ring[i:] for i in range(1, n + 1)]
    seen, uniq = {canon(ring)}, []
    for r in out:
        if canon(r) not in seen:
            seen.add(canon(r))
            uniq.append(r)
    random.shuffle(uniq)
    return uniq


# clear the leftover test value from the last manual run
req('DELETE', 'sfo', '/kv/nodal?ring=sfo,was,fra,sin')
time.sleep(3)

cur = ['sfo', 'dal', 'was', 'fra', 'sin']
cur_s = evaluate(cur)
log(f'start {",".join(cur)} {cur_s*100:.1f}%')
improved = True
while improved and evals < MAX_EVALS:
    improved = False
    for nb in neighbors(cur):
        if evals >= MAX_EVALS:
            break
        s = evaluate(nb)
        if s < cur_s - MARGIN:
            log(f'>> move to {",".join(nb)} ({cur_s*100:.1f}% -> {s*100:.1f}%)')
            cur, cur_s, improved = nb, s, True
            break
log(f'\nbest {",".join(cur)} mean awake {cur_s*100:.1f}%  ({evals} rings measured'
    + (', budget hit' if evals >= MAX_EVALS else ', local optimum') + ')')
log('all measured, best first:')
for k, v in sorted(cache.items(), key=lambda kv: kv[1]):
    log(f'  {",".join(k):<22} {v*100:5.1f}%' if v < 9 else f'  {",".join(k):<22} failed')

#!/usr/bin/env python3
"""Does the Unikraft proxy deliver a request whose client closes/resets right after sending?"""
import socket, struct, time, urllib.request

HOST = 'owen-gordon-kv.dal.unikraft.app'
RING = 'sfo,sin,was,fra,dal'


def put(key, mode):
    body = b'v-' + mode.encode()
    req = (f'PUT /kv/{key}?ttl=40&ring={RING} HTTP/1.1\r\nHost: {HOST}\r\n'
           f'Content-Length: {len(body)}\r\nConnection: close\r\n\r\n').encode() + body
    s = socket.create_connection((HOST, 80), timeout=10)
    t0 = time.monotonic()
    s.sendall(req)
    if mode == 'normal':
        resp = s.recv(200).split(b'\r\n')[0].decode()
        s.close()
        return resp
    if mode == 'rst-after-ack':
        time.sleep(0.05)  # ~5x our RTT to the proxy: the data is ACKed by now
    if mode.startswith('rst'):
        s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
    s.close()  # fin: orderly close; rst*: abortive close
    return f'closed after {1000*(time.monotonic()-t0):.0f} ms'


def get(key):
    try:
        with urllib.request.urlopen(f'http://owen-gordon-kv.fra.unikraft.app/kv/{key}?ring={RING}', timeout=15) as f:
            return f.status, f.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode().strip()


for mode in ['normal', 'fin', 'rst', 'rst-after-ack']:
    key = 'abort-' + mode
    print(f'{mode:<14} PUT: {put(key, mode)}', flush=True)
    time.sleep(3)
    print(f'{"":<14} GET from fra: {get(key)}', flush=True)

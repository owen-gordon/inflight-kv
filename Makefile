# Build kv and run one scale-to-zero node per metro on Unikraft Cloud.
# Needs: kraft, unikraft (logged in), jq, getent. Credentials come only from the UKC_TOKEN env var.

USER_NS  ?= owen-gordon# your Unikraft Cloud user/namespace
IMAGE    ?= $(USER_NS)/kv:latest
DOMAIN   ?= $(USER_NS)-kv# node at $(DOMAIN).<metro>.unikraft.app
RING     ?= sfo sin was fra dal# metros to deploy; their order is the default ring (best measured)
COOLDOWN ?= 100# ms; the platform minimum
SECS     ?= 60

.PHONY: help login build deploy status down measure logs

help:
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-8s %s\n", $$1, $$2}'

# UKC_TOKEN is base64("robot$user:secret"); the registry wants exactly those credentials.
login: ## store UKC_TOKEN's registry creds for kraft (fixes 401 on push)
	@test -n "$$UKC_TOKEN" || { echo 'UKC_TOKEN is not set' >&2; exit 1; }
	@c=$$(printf %s "$$UKC_TOKEN" | base64 -d) && kraft login -u "$${c%%:*}" -t "$${c#*:}" index.unikraft.io

build: ## build the unikernel for kraftcloud and push IMAGE
	kraft build --plat kraftcloud --arch x86_64 --no-prompt
	kraft pkg --as oci --plat kraftcloud --arch x86_64 --name index.unikraft.io/$(IMAGE) --push

deploy: ## (re)deploy one node per metro in RING; each gets every metro's IP (no DNS at boot)
	@test $(COOLDOWN) -ge 100 || { echo "COOLDOWN: the platform minimum is 100 ms" >&2; exit 1; }
	@tab=; for m in $(RING); do \
	  ip=$$(getent ahostsv4 $(DOMAIN).$$m.unikraft.app | awk 'NR==1{print $$1}'); \
	  test -n "$$ip" || { echo "cannot resolve $(DOMAIN).$$m.unikraft.app" >&2; exit 1; }; \
	  tab="$$tab $$m=$$ip"; done; \
	for m in $(RING); do \
	  unikraft instances delete $$m/kv >/dev/null 2>&1; \
	  unikraft run --metro $$m -n kv --image $(IMAGE) -m 64M -p 80:8080/http --domain $(DOMAIN) \
	    --scale-to-zero policy=on,cooldown-time=$(COOLDOWN) --args "$$m $(DOMAIN)$$tab" >/dev/null \
	    && echo "$$m up" || echo "$$m: deploy failed"; done; \
	echo "table:$$tab"

status: ## state of every node
	@unikraft instances list | awk 'NR==1 || $$2=="kv"'

down: ## delete every node
	@for m in $(RING); do unikraft instances delete $$m/kv; done

logs: ## console of one node's last boots, e.g. make logs M=sin
	unikraft instances logs $(M)/kv --tail 80

measure: ## awake time per node over the next SECS seconds (counters have no reset, so this diffs two snapshots)
	@one() { unikraft api --metro $$1 /v1/instances/metrics 2>/dev/null \
	  | jq -r --arg m $$1 '.data.instances[] | select(.name=="kv") | "\($$m) \(.uptime_ms) \(.start_count)"'; }; \
	snap() { for m in $(RING); do one $$m | grep . || one $$m; done; }; \
	a=$$(snap); sleep $(SECS); b=$$(snap); \
	for m in $(RING); do printf '%s\n%s\n' "$$a" "$$b" | awk -v m=$$m -v s=$(SECS) \
	  '$$1==m { n++; u[n]=$$2; c[n]=$$3 } END { if (n<2) printf "%-4s no data (metrics API call failed)\n", m; \
	  else printf "%-4s awake %6.1fs of %ds (%4.1f%%)  wakes %d\n", m, (u[2]-u[1])/1000, s, (u[2]-u[1])/(10*s), c[2]-c[1] }'; done

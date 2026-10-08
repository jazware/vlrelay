# vlrelay: dev loop, local network and e2e (docs/devloop.md)

set shell := ["bash", "-euo", "pipefail", "-c"]

import? 'internal/justfile'

target_dir := env_var_or_default("CARGO_TARGET_DIR", "target")
bin := target_dir / "debug"

default:
    @just --list --unsorted

# Type-check everything (the fastest edit loop)
check:
    cargo check --all-targets

# Build the relay (dev profile)
build:
    cargo build --bin vlrelay

# Unit and integration tests (nextest when installed: one process per test, parallel)
test *args:
    if command -v cargo-nextest >/dev/null; then cargo nextest run {{args}}; else cargo test {{args}}; fi

# Upstream tests against a real in-process vlpds (interop/: its own package, so `test` doesn't build the PDS)
test-interop *args:
    cd interop && cargo test {{args}}

# Re-check on every save (bacon when installed, else cargo-watch, else a polling loop)
watch job="check":
    scripts/watch.sh {{job}}

fmt:
    cargo fmt

clippy:
    cargo clippy --all-targets -- -D warnings

# Time a cold build, a no-op build and an incremental one-module edit (scripts/buildtime.sh)
buildtime *args:
    scripts/buildtime.sh {{args}}

# ---- local network -------------------------------------------------------

# MinIO + PLC + reference PDS (docker) and DEV_PDS (2) native vlpds upstreams, all memory-capped
dev-up:
    dev/up.sh

# Stop and delete everything dev-up started (and dev/state)
dev-down:
    dev/down.sh

# Create N accounts round-robin across every upstream (appends to dev/state/accounts.json)
dev-seed n="30":
    cargo build --quiet --bin devnet
    {{bin}}/devnet seed --accounts {{n}} $(sed 's/^/--host /' dev/state/hosts | tr '\n' ' ')

# Continuous writes at RATE/s plus handle changes and deactivations (DURATION 0 = until ^C)
dev-load rate="20" duration="0" *args:
    cargo build --quiet --bin devnet
    {{bin}}/devnet load --rate {{rate}} --duration {{duration}} {{args}}

# The relay against the local network, under a memory cap (the e2e's contract, docs/devloop.md)
relay *args:
    cargo build --quiet --bin vlrelay
    source dev/ports.sh && dev/capped.sh ${RELAY_MEM_MB:-4096} {{bin}}/vlrelay \
        --listen 127.0.0.1:$RELAY_PORT --memory --plc-url http://127.0.0.1:$PLC_PORT --qlog-listen 127.0.0.1:0 \
        $(sed 's/^/--host /' dev/state/hosts | tr '\n' ' ') {{args}}

# Compare a relay's firehose with every local upstream's (e2e_check; extra flags e.g. --duration 60)
e2e-check relay="http://127.0.0.1:2980" *args:
    cargo build --quiet --bin e2e_check
    {{bin}}/e2e_check $(sed 's/^/--upstream /' dev/state/hosts | tr '\n' ' ') --relay {{relay}} {{args}}

# The checker against the upstreams themselves (proves the env and the checker without a relay)
e2e-self *args:
    cargo build --quiet --bin e2e_check
    {{bin}}/e2e_check $(sed 's/^/--upstream /' dev/state/hosts | tr '\n' ' ') $(sed 's/^/--relay /' dev/state/hosts | tr '\n' ' ') {{args}}

# Full e2e: dev-up, seed, load, relay, checker (tests/e2e/run.sh; KEEP=1 leaves the network up)
e2e *args:
    tests/e2e/run.sh {{args}}

# Policy e2e: one relay vs a fakepds fleet with faults (auto-throttle, cases, a ban rule, clean hosts untouched)
e2e-policy *args:
    tests/e2e/policy.sh {{args}}

# Ecosystem compat: vlRelay beside indigo's relay with goat, indigo's consumer, @atproto/sync and Jetstream on both (docs/compat.md)
compat *args:
    tests/compat/run.sh {{args}}

# The quorum log under kill -9, partitions and SIGSTOP on a local 3-node cluster, every node's stream checked (tests/qlog/chaos.sh, docs/quorum.md "Implementation notes"; `just qlog-chaos list`)
qlog-chaos scenario *args:
    tests/qlog/chaos.sh {{scenario}} {{args}}

# The relay on the quorum log under chaos: fakepds -> three relays -> every node's stream checked, the manifest verified and every upstream event matched (tests/qlog/relay-chaos.sh; `just relay-chaos list`)
relay-chaos scenario *args:
    tests/qlog/relay-chaos.sh {{scenario}} {{args}}

# ---- images ----------------------------------------------------------------

# Production image for this machine's platform (tools=1 adds fakepds and e2e_check; the context is .. while vlsync is a path dependency)
docker-build tag="vlrelay:local" tools="":
    ctx=.; if grep -q '^vlsync-store = { path' Cargo.toml; then ctx=..; fi; \
    docker buildx build -f Dockerfile --build-arg VLRELAY_TOOLS={{tools}} -t {{tag}} --load $ctx

# Regenerate docs/operations/configuration.md from `vlrelay --help` (run after changing a flag)
config-doc:
    cargo build --quiet --bin vlrelay
    {{bin}}/vlrelay --help | python3 build/config_doc.py > docs/operations/configuration.md

# ---- docs site (/docs; docs/_style.md) -----------------------------------------

# Validate the docs site (front matter, heroes, diagrams, links, no private names) and print its nav
docs-check:
    cd ui && npm install --no-audit --no-fund && npm run check-docs

# Live preview of the docs and dashboard on :5790, reloading when a page changes
dev-ui:
    cd ui && npm install --no-audit --no-fund && npm run dev

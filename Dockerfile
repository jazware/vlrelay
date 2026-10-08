# syntax=docker/dockerfile:1.7
# vlRelay image: a release build of the vlrelay binary and the dashboard (ui/,
# built with node, served from --ui-dir) on a slim non-root runtime. The two
# builds are independent stages, so a UI-only change reuses the cached binary
# and rebuilds only the last layer.
#
#   docker build -t vlrelay:local .
#   docker run -p 2980:2980 vlrelay:local --memory --host morel.us-east.host.bsky.network --admin-token dev
#
# Configuration is the CLI flags and their VLRELAY_* env vars (`vlrelay --help`,
# docs/operations/configuration.md). /metrics and /admin share the --listen port.

# --- dashboard ----------------------------------------------------------------
FROM node:26-bookworm-slim AS ui
WORKDIR /src/ui
COPY ui/package.json ui/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY ui/ ./
# the docs site (/docs) is rendered from ../docs at build time
COPY docs/ /src/docs/
RUN npm run build

# --- rust release build -------------------------------------------------------
FROM rust:1.99.0-bookworm AS build

# cmake/clang: aws-lc-sys (rustls) and the vendored libsecp256k1 / jemalloc C builds
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake clang \
    && rm -rf /var/lib/apt/lists/*
# .cargo/ isn't copied: its target-cpu=native would tie the image to the
# build machine's CPU.
WORKDIR /src
COPY rust-toolchain.toml ./
# installs the pinned toolchain if the base image's differs
RUN rustup show active-toolchain
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# No debug info in the image (Cargo.toml keeps debug = 1 for local profiling).
# Symbols stay, so panics and backtraces still name functions.
ENV CARGO_PROFILE_RELEASE_DEBUG=0
# --build-arg VLRELAY_TOOLS=1 adds fakepds (a synthetic upstream fleet and
# firehose consumer, docs/loadfleet.md) and e2e_check (docs/devloop.md). One
# cargo invocation, so they reuse the compiled lib.
ARG VLRELAY_TOOLS=""
# vlsync and vlatproto are git dependencies on jazware/vlsync and
# jazware/vlatproto (Cargo.toml), fetched here.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --bin vlrelay ${VLRELAY_TOOLS:+--bin fakepds --bin e2e_check} \
    && mkdir -p /out \
    && cp target/release/vlrelay /out/ \
    && if [ -n "$VLRELAY_TOOLS" ]; then cp target/release/fakepds target/release/e2e_check /out/; fi \
    && /out/vlrelay --help >/dev/null

# --- runtime ------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 vlrelay \
    && useradd --system --uid 10001 --gid vlrelay --home-dir /var/lib/vlrelay --create-home vlrelay
USER vlrelay:vlrelay
WORKDIR /var/lib/vlrelay
ENV VLRELAY_LISTEN=0.0.0.0:2980 \
    RUST_LOG=info,slatedb=warn
# 2980: subscribeRepos, the sync API, requestCrawl, /admin, /metrics
# 2978: the quorum log's peer port (--qlog-listen; private network only)
EXPOSE 2980 2978
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=3 \
    CMD curl -sf http://127.0.0.1:2980/xrpc/_health || exit 1
# --ui-dir is here because it has no env var. tini forwards SIGTERM to vlrelay.
ENTRYPOINT ["/usr/bin/tini", "--", "vlrelay", "--ui-dir", "/usr/share/vlrelay/ui"]
COPY --from=build /out/ /usr/local/bin/
# Last: the layer a UI-only change replaces.
COPY --from=ui /src/ui/dist /usr/share/vlrelay/ui

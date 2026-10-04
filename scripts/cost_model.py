#!/usr/bin/env python3
"""vlRelay cost model: the tables in docs/cost.md, from measured inputs and list prices.

    python3 scripts/cost_model.py          # every table, as markdown
    python3 scripts/cost_model.py --json   # the scenario numbers

Inputs are measured unless marked "code" (counted from the source, not measured) or
"assumed". Each one names its doc. Prices are list prices with the date and URL they came
from. Change an input or a price and rerun; nothing else is hard-coded in the tables.
"""
import json
import math
import sys

MONTH_S = 30.4375 * 86400
GB = 1e9
TB = 1e12
MIB = 1 << 20

# ---------------------------------------------------------------- inputs
FRAME_B = 5_300            # mean frame, reference-notes.md "Frame sizes and rates" (5,283-5,323 B)
ZSTD_RATIO = 1.56          # zstd -1 on production frames, perf.md iteration 5
FWD_EXTRA_B = 90           # forward hop adds DID, host, seq, meta: perf.md iteration 6
RATE_AVG = 350             # events/s, 7-day average, reference-notes.md (ClickHouse repo_records)
RATE_PEAK_HOUR = 480
RATE_LOW_HOUR = 180
BURST = 2.0                # minute bursts over the peak hour (assumed, as in vlpds's model)
CPU_TARGET = 0.7           # size so a peak-hour minute burst runs at <=70% CPU

# CPU per event (perf.md). Thread = hardware thread (SMT sibling counts), core = physical core.
US_SINGLE_CORE = 70        # one node, 8 physical cores, iteration 5 (65-70)
US_SINGLE_THREAD = 90      # one node on 3 cores + SMT, iteration 6 (89-94)
US_CLUSTER_THREAD = 150    # 3-node cluster, all nodes' CPU per event, iteration 6 (142-156)
US_CLUSTER_CORE = 117      # the same scaled to physical cores, iteration 6 extrapolation
# Iteration 6's split of the cluster's extra ~60 us/event (thread seconds), used to scale with N.
US_FWD = 15                # per forwarded event: send + receive + HTTP/2 + TLS
US_LOG_COPY = 8.25         # per event per peer copy of a log stream (serve + receive): 16.5 us at 2 peers
US_MERGE_NODE = 1.2        # per event per node merging it: 3.5 us at 3 nodes
US_CLUSTER_OTHER = 30      # the rest of the +60 (SlateDB writes, apply_did, hash maps, clock)
US_ARCHIVE = 130           # archival apply, tree reopened each commit (69 when kept), archival.md

LINGER_S = 0.025           # --linger-ms default
SEAL_OVERHEAD_S = 0.0028   # vlpds measured seal-to-next-open overhead (cost-model-2026-10-02)
SEG_CAP_B = 8 * MIB        # --max-segment-mb, raw bytes
HEDGE = 0.0095             # hedged segment PUTs, vlpds cost model
CKPT_S = 5.0               # DID shard checkpoint (node.rs checkpoint_interval)
FLUSH_A, FLUSH_B = 4.42, 9.07  # Class A/B per L0 flush incl. the compaction it causes (vlpds)
POLL_B_PER_SHARD = 0.40    # SlateDB manifest 10 s + compactor/worker 30 s polls (vlpds defaults)
DID_SHARDS_SINGLE, DID_SHARDS_CLUSTER = 4, 24
HOST_SHARDS = 64           # --host-shards
HOSTCK_S = 2.0             # host cursor checkpoints (cluster checkpoint_every), code
TICK_S = 5.0               # cluster tick: hostck refresh, host counts flush, registry reload, code
CTL_A_PER_NODE, CTL_B_PER_NODE = 1.5, 1.0  # vlpds lease CAS + LISTs per node (vlpds measured 4.5 A at 3)
SEQCK_S = 10.0             # seq checkpoints: a LIST per core and one PUT per interval (seq.md)
RETENTION_S = 72 * 3600
DID_STATE_B = 121.8        # per-DID sync state in SSTs: tests/state_bulk.rs, 1M DIDs (2026-10-04)
PLC_SEED_B_PER_DID = 54    # ~3 GB in SSTs for 56M, policy.md "PLC export seeding"
TRANSIENT = 1.25           # replaced SSTs awaiting GC, vlpds model's budget
DIDS_TODAY = 56e6          # repos on Bluesky PDSes (vlpds cost model, PLC query)
PLC_DIDS_TODAY = 89.9e6    # every PLC DID (seeds cover all of them)
RECORDS_TODAY = 23.9e9     # vlpds cost model, crawl + tail
ARCH_B_PER_RECORD, ARCH_B_PER_REPO = 154.2, 323  # vlpds storage-2026-10-02 (same layout)
REPLAY_H_PER_CONSUMER_DAY = 1.0  # backfill read from the bucket per consumer per day (assumed)
RING_B = 512 * MIB         # ServeConfig::ring_bytes
REPLICA_POLL_S, REPLICA_WINDOW, GET_LATENCY_S = 0.020, 2, 0.025  # cluster/follow.rs; latency assumed
NIC_UTIL = 0.7             # usable share of a NIC or a guaranteed port
CONSUMER_CORES_PER_GBPS = 0.025 / 1.6  # perf.md "Fan-out at 33k": 0.025 cores per 1.6 Gb/s consumer

LOADS = [("today", 1), ("10x", 10), ("100x", 100), ("1000x", 1000)]
SIZED = [(n, m, True) for n, m in LOADS] + [("1000x, merge tier (sketch)", 1000, False)]
CONSUMERS = [10, 100, 1000]

# ---------------------------------------------------------------- prices
# Object stores. S3, GCS and R2 are the vlpds cost model's (fetched 2026-10-01), still current.
STORES = {
    "s3": dict(name="S3 Standard us-east-1", a=5.00, b=0.40, gb=[(50e3, 0.023), (450e3, 0.022), (math.inf, 0.021)],
               egress=True, cas=True, src="aws.amazon.com/s3/pricing, 2026-10-01 (vlpds model)"),
    "gcs": dict(name="GCS Standard regional", a=5.00, b=0.40, gb=[(math.inf, 0.0200 * GB / (1 << 30))],
                egress=True, cas=True, src="cloud.google.com/storage/pricing, 2026-10-01 (vlpds model)"),
    "r2": dict(name="Cloudflare R2 Standard", a=4.50, b=0.36, gb=[(math.inf, 0.015)],
               egress=False, cas=True, src="developers.cloudflare.com/r2/pricing, 2026-10-01 (vlpds model)"),
    "tigris": dict(name="Tigris Standard", a=5.00, b=0.50, gb=[(math.inf, 0.020)],
                   egress=False, cas=True, src="tigrisdata.com/pricing, 2026-10-04"),
    "b2": dict(name="Backblaze B2", a=0.0, b=0.0, gb=[(math.inf, 0.00695)],
               egress=False, cas=False, src="backblaze.com/cloud-storage/pricing + transaction-pricing, 2026-10-04"),
    "wasabi": dict(name="Wasabi pay-go", a=0.0, b=0.0, gb=[(math.inf, 0.00799)], min_days=90, min_gb=1000,
                   egress=False, cas=False, src="wasabi.com/pricing + docs (90-day minimum), 2026-10-04"),
}
# AWS internet egress, us-east-1, $/GB by monthly TB tier (first 100 GB free ignored).
AWS_EGRESS = [(10, 0.09), (40, 0.085), (100, 0.07), (math.inf, 0.05)]
AWS_CROSS_AZ = 0.02        # $0.01/GB out of one AZ + $0.01/GB into the other

# Hosts, $/month. OVH from the public US catalog API (api.us.ovhcloud.com/1.0/order/catalog/
# public/baremetalServers?ovhSubsidiary=US, fetched 2026-10-04): no-commitment monthly renew price.
HOSTS = {
    "ovh-vps4": dict(usd=23.37, gbps=3, threads=8, note="VPS-4: 8 vCores, 24 GB, 3 Gb/s, unlimited traffic (us.ovhcloud.com/vps, 2026-10-04)"),
    "ovh-adv2": dict(usd=198, gbps=3, threads=16, cores=8, note="ADVANCE-2 (EPYC 4344P 8c/16t, 64 GB): 3 Gb/s guaranteed unmetered public, 25 Gb/s vRack"),
    "ovh-adv2-5g": dict(usd=198 + 280, gbps=5, note="ADVANCE-2 + 5 Gb/s guaranteed unmetered ($280)"),
    "ovh-scale-10g": dict(usd=765 + 621, gbps=10, note="SCALE-a1 2026 (EPYC 9135 16c/32t) + 10 Gb/s guaranteed unmetered ($621)"),
    "ovh-scale-25g": dict(usd=765 + 1400, gbps=25, threads=32, note="SCALE-a1 2026 + 25 Gb/s guaranteed unmetered ($1,400), 50 Gb/s private"),
    # Hetzner: AX42 at EUR 97.30 (June 2026 repricing; third-party, Hetzner's page didn't render),
    # converted at Hetzner's own USD/EUR addon ratio (48/43). 1 Gb/s unlimited traffic, or the 10G
    # uplink addon ($48/mo, docs.hetzner.com price-server-addons, 2026-10-04): 20 TB out included,
    # then $1.20/TB; inbound and internal traffic free.
    "hetzner-ax42": dict(usd=round(97.30 * 48 / 43), gbps=1, threads=16, cores=8, note="AX42 (Ryzen 7 PRO 8700GE 8c/16t, 64 GB), 1 Gb/s unlimited"),
    "hetzner-ax42-10g": dict(usd=round(97.30 * 48 / 43) + 48, gbps=10, included_tb=20, over_tb=1.20, note="AX42 + 10G uplink"),
    # AWS on-demand us-east-1 (Vantage/Holori listings of AWS prices, 2026-10-04), 730 h.
    "aws-c7g4x": dict(usd=0.580 * 730, gbps=15, threads=16, note="c7g.4xlarge: 16 Graviton3 cores, 32 GiB, up to 15 Gb/s (2xlarge is $0.290/h)"),
    "aws-c7gn2x": dict(usd=0.499 * 730, gbps=50, note="c7gn.2xlarge: 8 cores, up to 50 Gb/s (taken as 25 sustained)", sustained=25),
}


# ---------------------------------------------------------------- model
def seal_rate(c, linger=LINGER_S):
    """Segments/s of one log taking c events/s. A segment opens at its first event and seals a
    linger later, or at 8 MiB raw. 32 PUTs in flight never bind below ~1 s PUTs."""
    if c <= 0:
        return 0.0
    by_linger = 1.0 / (linger + SEAL_OVERHEAD_S + 1.0 / c)
    by_size = c * FRAME_B / SEG_CAP_B
    return max(by_linger, by_size)


def cluster_us(n, mesh=True, mergers=3):
    """CPU per event, all nodes, thread-seconds. mesh: every core streams its log to every
    other core (what's built). Otherwise cores stream only to `mergers` merge/edge nodes."""
    if n == 1:
        return US_SINGLE_THREAD
    fwd = US_FWD * (n - 1) / n
    if mesh:
        return US_SINGLE_THREAD + fwd + US_LOG_COPY * (n - 1) + US_MERGE_NODE * n + US_CLUSTER_OTHER
    return US_SINGLE_THREAD + fwd + US_LOG_COPY * mergers + US_CLUSTER_OTHER


def store_storage(st, gb):
    total, left, prev = 0.0, gb, 0.0
    for upto, price in st["gb"]:
        span = min(left, upto - prev)
        total += span * price
        left -= span
        prev = upto
        if left <= 0:
            break
    return total


def aws_egress(tb):
    total, left, prev = 0.0, tb, 0.0
    for upto, price in AWS_EGRESS:
        span = min(left, upto - prev)
        total += span * 1000 * price
        left -= span
        prev = upto
        if left <= 0:
            break
    return total


def control_plane(n, d, h=HOST_SHARDS):
    """Requests/s of everything but the log, cluster-wide. Cluster terms are counted from the
    code (cluster/hosts.rs, node/cluster.rs), not measured."""
    flushes = d / CKPT_S
    a = flushes * FLUSH_A + 1.0 / SEQCK_S
    b = flushes * FLUSH_B + d * POLL_B_PER_SHARD
    if n > 1:
        a += h / HOSTCK_S                     # hostck CAS per active host shard
        b += h / HOSTCK_S                     # its read-before-write
        a += n * 2 / TICK_S                   # LIST hostck/ and hosts/ per node per tick
        b += n * 2 * h / TICK_S               # GET every changed hostck/ and hosts/ object
        a += n * h / TICK_S + h / TICK_S      # host counts flush per node + registry flush (CAS)
        b += n * h / TICK_S + h / TICK_S      # their read-before-write
        a += n * (CTL_A_PER_NODE + 1.0 / SEQCK_S)
        b += n * CTL_B_PER_NODE
    return a, b


def scenario(mult, consumers=0, nodes=None, linger=LINGER_S, archival=False, mesh=True):
    r = RATE_AVG * mult
    prov = RATE_PEAK_HOUR * mult * BURST
    s = {"mult": mult, "rate": r, "prov_rate": prov, "mesh": mesh}
    s["ingest_gbps"] = r * FRAME_B * 8 / 1e9
    s["log_gbps_zstd"] = s["ingest_gbps"] / ZSTD_RATIO
    # nodes: smallest n >= 3 whose 8-core (16-thread) boxes keep prov under CPU_TARGET
    threads = 16
    if nodes is None:
        nodes = None
        for n in range(3, 400):
            us = cluster_us(n, mesh) + (US_ARCHIVE if archival else 0)
            if prov * us * 1e-6 <= n * threads * CPU_TARGET:
                nodes = n
                break
        if nodes is None:
            s["infeasible"] = True
            nodes = 399
    n = nodes
    s["nodes"] = n
    us = cluster_us(n, mesh) + (US_ARCHIVE if archival else 0)
    s["us_event"] = us
    s["busy_threads_avg"] = r * us * 1e-6
    s["busy_threads_prov"] = prov * us * 1e-6
    # peer bytes per event: forwards + log streams to every peer (raw frames)
    copies = (n - 1) if mesh else 3
    s["peer_b_event"] = (n - 1) / n * (FRAME_B + FWD_EXTRA_B) + copies * FRAME_B if n > 1 else 0
    s["peer_gbps_total"] = r * s["peer_b_event"] * 8 / 1e9
    s["peer_gbps_node"] = s["peer_gbps_total"] / n
    # bucket requests
    per_log = r / n
    seals = n * seal_rate(per_log, linger) * (1 + HEDGE)
    d = DID_SHARDS_CLUSTER if n > 1 else DID_SHARDS_SINGLE
    ca, cb = control_plane(n, d)
    s["seals_s"] = seals
    s["seg_events"] = r / seals if seals else 0
    s["seg_kb_zstd"] = s["seg_events"] * FRAME_B / ZSTD_RATIO / 1e3
    retention_a = n * 0.05
    s["req_a"] = seals + ca + retention_a
    s["req_b"] = cb
    # backfill: each consumer replays REPLAY_H of history a day from the bucket
    replay_frac = REPLAY_H_PER_CONSUMER_DAY / 24
    s["backfill_get_s"] = consumers * seals * replay_frac
    s["req_b"] += s["backfill_get_s"]
    s["bucket_read_tb_mo"] = consumers * r * FRAME_B / ZSTD_RATIO * replay_frac * MONTH_S / TB
    s["ring_s"] = RING_B / (r * FRAME_B)
    # storage, GB
    dids = DIDS_TODAY * mult
    s["log_gb"] = r * FRAME_B / ZSTD_RATIO * RETENTION_S / GB
    s["state_gb"] = dids * DID_STATE_B * TRANSIENT / GB
    s["seed_gb"] = PLC_DIDS_TODAY * mult * PLC_SEED_B_PER_DID * TRANSIENT / GB
    s["arch_gb"] = (RECORDS_TODAY * mult * ARCH_B_PER_RECORD + dids * ARCH_B_PER_REPO) * TRANSIENT / GB
    # consumers
    s["consumers"] = consumers
    s["egress_gbps"] = consumers * s["ingest_gbps"]
    s["egress_tb_mo"] = s["egress_gbps"] / 8 * MONTH_S / 1e3
    return s


def bucket_cost(s, key, archival=False):
    st = STORES[key]
    a_mo, b_mo = s["req_a"] * MONTH_S / 1e6, s["req_b"] * MONTH_S / 1e6
    log_gb = s["log_gb"] * (st.get("min_days", 3) / 3)
    gb = log_gb + s["state_gb"] + s["seed_gb"] + (s["arch_gb"] if archival else 0)
    gb = max(gb, st.get("min_gb", 0))
    return {"requests": a_mo * st["a"] + b_mo * st["b"], "storage": store_storage(st, gb), "gb": gb}


CORE_SKU = {"ovh": "ovh-adv2", "hetzner": "hetzner-ax42-10g", "aws": "aws-c7g4x"}
# Merge/edge nodes of the 1000x design sketch take the whole raw stream in (~15 Gb/s).
MERGER_SKU = {"ovh": "ovh-scale-25g", "hetzner": None, "aws": "aws-c7gn2x"}
COMBOS = [("ovh", "r2"), ("ovh", "tigris"), ("ovh", "s3"), ("hetzner", "r2"), ("aws", "s3")]


def core_spare_gbps(s, provider):
    """Public bandwidth the core nodes can give consumers: half of what's left at 70% after
    ingest. Hetzner's peer traffic shares the public port. OVH's goes over the vRack."""
    h = HOSTS[CORE_SKU[provider]]
    per_node = h["gbps"] * NIC_UTIL - s["ingest_gbps"] * 2.74 / s["nodes"]
    if provider == "hetzner":
        per_node -= s["peer_gbps_node"] * 2.74
    return max(0.0, 0.5 * per_node * s["nodes"])


def metered(provider, tb, servers):
    if provider == "aws":
        return aws_egress(tb)
    if provider == "hetzner":
        h = HOSTS["hetzner-ax42-10g"]
        return max(0.0, tb - servers * h["included_tb"]) * h["over_tb"]
    return 0.0


def edges(s, provider):
    """Edge hosts and metered bandwidth for s's consumers: on the core nodes while they fit
    there, else the cheapest edge SKU of the provider."""
    g = s["egress_gbps"]
    if g <= 0:
        return {"n": 0, "hosts": 0.0, "bw": 0.0, "sku": "-"}
    if g <= core_spare_gbps(s, provider):
        return {"n": 0, "hosts": 0.0, "bw": metered(provider, s["egress_tb_mo"], s["nodes"]), "sku": "on the cores"}
    opts = {
        "ovh": ["ovh-adv2", "ovh-adv2-5g", "ovh-scale-10g", "ovh-scale-25g"],
        "hetzner": ["hetzner-ax42", "hetzner-ax42-10g"],
        "aws": ["aws-c7gn2x"],
    }[provider]
    best = None
    for k in opts:
        h = HOSTS[k]
        n = math.ceil(g / (h.get("sustained", h["gbps"]) * NIC_UTIL))
        if provider == "hetzner":
            bw = n * max(0.0, s["egress_tb_mo"] / n - h["included_tb"]) * h["over_tb"] if "included_tb" in h else 0.0
        else:
            bw = metered(provider, s["egress_tb_mo"], 0)
        c = {"n": n, "hosts": n * h["usd"], "bw": bw, "sku": k}
        if best is None or c["hosts"] + c["bw"] < best["hosts"] + best["bw"]:
            best = c
    return best


def total(s, provider, store, archival=False):
    core = HOSTS[CORE_SKU[provider]]
    out = {"cores": s["nodes"] * core["usd"]}
    if not s.get("mesh", True):
        m = MERGER_SKU[provider]
        if m is None:
            return None
        out["cores"] += 3 * HOSTS[m]["usd"]
    bc = bucket_cost(s, store, archival)
    out["bucket_req"], out["bucket_storage"] = bc["requests"], bc["storage"]
    e = edges(s, provider)
    out["edges"], out["edge_n"], out["edge_sku"] = e["hosts"], e["n"], e["sku"]
    bw = e["bw"]
    if provider == "aws":
        bw += s["peer_gbps_total"] / 8 * MONTH_S * AWS_CROSS_AZ * (2 / 3)  # 2 of 3 peers in another AZ
    if STORES[store]["egress"] and provider != "aws":
        bw += aws_egress(s["bucket_read_tb_mo"])  # backfill GETs leave the cloud
    out["bandwidth"] = bw
    out["total"] = sum(out[k] for k in ("cores", "bucket_req", "bucket_storage", "edges", "bandwidth"))
    return out


# ---------------------------------------------------------------- tables
def money(x):
    if x >= 1e6:
        return f"${x / 1e6:,.1f}M"
    if x >= 1e4:
        return f"${x / 1e3:,.0f}k"
    return f"${x:,.0f}"


def gbps(x):
    return f"{x * 1000:,.0f} Mb/s" if x < 1 else f"{x:,.1f} Gb/s" if x < 100 else f"{x:,.0f} Gb/s"


def table(head, rows):
    print("| " + " | ".join(head) + " |")
    print("|" + "---|" * len(head))
    for r in rows:
        print("| " + " | ".join(str(c) for c in r) + " |")
    print()


def main():
    if "--json" in sys.argv:
        out = {name: scenario(m, 100) for name, m in LOADS}
        print(json.dumps(out, indent=1, default=float))
        return

    print("## Load levels\n")
    rows = []
    for name, m in LOADS:
        s = scenario(m)
        rows.append([name, f"{s['rate']:,.0f}", f"{RATE_PEAK_HOUR * m:,.0f}", f"{s['prov_rate']:,.0f}",
                     gbps(s["ingest_gbps"]), gbps(s["log_gbps_zstd"]), f"{s['ring_s']:,.0f} s"])
    table(["load", "events/s avg", "peak hour", "sized for (2x peak hour)", "ingest (5.3 KB)", "log after zstd -1", "512 MB ring holds"], rows)

    print("## CPU and nodes (full mesh, as built)\n")
    rows = []
    for name, m in LOADS:
        s = scenario(m)
        rows.append([name, s["nodes"] if not s.get("infeasible") else "doesn't converge",
                     f"{s['us_event']:.0f}", f"{s['busy_threads_avg']:.1f}", f"{s['busy_threads_prov']:.1f}",
                     f"{s['busy_threads_prov'] / (s['nodes'] * 16):.0%}"])
    table(["load", "core nodes (8c/16t)", "us/event (threads)", "busy threads avg", "at the sized rate", "of the cluster"], rows)
    rows = []
    for n in (3, 6, 12, 24, 48):
        rows.append([n, f"{cluster_us(n):.0f}", f"{cluster_us(n, mesh=False):.0f}",
                     f"{n * 16 * CPU_TARGET / cluster_us(n) * 1e6 / 1e3:,.0f}k",
                     f"{n * 16 * CPU_TARGET / cluster_us(n, mesh=False) * 1e6 / 1e3:,.0f}k"])
    table(["nodes", "us/event, full mesh", "us/event, 3 mergers", "events/s at 70%, mesh", "events/s at 70%, mergers"], rows)

    print("## Peer traffic\n")
    rows = []
    for name, m in LOADS:
        s = scenario(m)
        rows.append([name, s["nodes"], f"{s['peer_b_event'] / 1e3:.1f} KB", gbps(s["peer_gbps_total"]), gbps(s["peer_gbps_node"]),
                     money(s["peer_gbps_total"] / 8 * MONTH_S * AWS_CROSS_AZ * 2 / 3)])
    table(["load", "nodes", "peer bytes/event", "cluster total", "per node each way", "AWS cross-AZ $/mo"], rows)

    print("## Bucket requests (25 ms linger, 32 in flight, 8 MiB cap)\n")
    rows = []
    for name, m in LOADS:
        for n in ((1, None) if m <= 10 else (None,)):
            s = scenario(m, nodes=n)
            ca, cb = control_plane(s["nodes"], DID_SHARDS_CLUSTER if s["nodes"] > 1 else DID_SHARDS_SINGLE)
            rows.append([name, s["nodes"], f"{s['seals_s']:.0f}", f"{s['seg_events']:,.0f} ({s['seg_kb_zstd']:,.0f} KB)",
                         f"{ca:.0f}", f"{cb:.0f}", f"{s['req_a']:.0f}", f"{s['req_b']:.0f}",
                         money(bucket_cost(s, 's3')['requests']), money(bucket_cost(s, 'r2')['requests'])])
    table(["load", "nodes", "segment PUTs/s", "events per segment (zstd)", "other Class A/s", "Class B/s",
           "Class A/s", "Class B/s", "S3 req $/mo", "R2 req $/mo"], rows)
    d, h, n = DID_SHARDS_CLUSTER, HOST_SHARDS, 3
    cp = [
        ("DID shard checkpoints (L0 flush + compaction), every 5 s", d / CKPT_S * FLUSH_A, d / CKPT_S * FLUSH_B),
        ("SlateDB polls", 0, d * POLL_B_PER_SHARD),
        ("host cursor checkpoints (hostck/), per host shard every 2 s", h / HOSTCK_S, h / HOSTCK_S),
        ("host counts flush (hosts/), per node per host shard every 5 s", n * h / TICK_S, n * h / TICK_S),
        ("registry flush (hosts/), per host shard every 5 s", h / TICK_S, h / TICK_S),
        ("re-reading hostck/ and hosts/ (LIST + changed GETs), per node every 5 s", n * 2 / TICK_S, n * 2 * h / TICK_S),
        ("leases, assignments, seq checkpoints, retention", n * (CTL_A_PER_NODE + 2 / SEQCK_S) + n * 0.05, n * CTL_B_PER_NODE),
    ]
    table(["3 nodes: request source", "Class A/s", "Class B/s", "S3 $/mo"],
          [[k, f"{a:.1f}", f"{b:.1f}", money((a * 5 + b * 0.4) * MONTH_S / 1e6)] for k, a, b in cp])
    rows = []
    for r in (RATE_LOW_HOUR, RATE_AVG, RATE_PEAK_HOUR):
        rows.append([f"{r}/s", f"{seal_rate(r):.1f}", f"{3 * seal_rate(r / 3):.1f}"])
    table(["today", "1 node, PUTs/s", "3 nodes, PUTs/s"], rows)

    print("## Storage\n")
    rows = []
    for name, m in LOADS:
        s = scenario(m)
        rows.append([name, f"{s['log_gb']:,.0f}", f"{s['state_gb']:,.1f}", f"{s['seed_gb']:,.1f}", f"{s['arch_gb']:,.0f}",
                     money(store_storage(STORES['s3'], s['log_gb'] + s['state_gb'] + s['seed_gb'])),
                     money(store_storage(STORES['r2'], s['log_gb'] + s['state_gb'] + s['seed_gb'])),
                     money(store_storage(STORES['r2'], s['arch_gb']))])
    table(["load", "log 72 h GB", "per-DID state GB", "PLC seeds GB", "archival mirror GB", "S3 $/mo (no archive)", "R2 $/mo", "archive on R2"], rows)

    print("## Bucket per provider, nodes as sized, no consumers ($/mo: requests + storage)\n")
    rows = []
    for name, m in LOADS:
        s = scenario(m)
        row = [name]
        for k in ("s3", "gcs", "r2", "tigris", "b2", "wasabi"):
            c = bucket_cost(s, k)
            row.append(money(c["requests"] + c["storage"]) + ("" if STORES[k]["cas"] else " *"))
        rows.append(row)
    table(["load", "S3", "GCS", "R2", "Tigris", "B2 *", "Wasabi *"], rows)

    print("## Consumer egress\n")
    rows = []
    for name, m in LOADS:
        for c in CONSUMERS:
            s = scenario(m, c)
            g = s["egress_gbps"]
            rows.append([name, c, gbps(g), f"{s['egress_tb_mo']:,.0f}",
                         math.ceil(g / (10 * NIC_UTIL)), math.ceil(g / (25 * NIC_UTIL)), math.ceil(g / (100 * NIC_UTIL)),
                         f"{g * CONSUMER_CORES_PER_GBPS:,.1f}"])
    table(["load", "consumers", "egress", "TB/mo", "10 GbE nodes", "25 GbE nodes", "100 GbE nodes", "serve cores"], rows)
    rows = []
    for name, m in LOADS:
        for c in CONSUMERS:
            s = scenario(m, c)
            row = [name, c]
            for p in ("ovh", "hetzner", "aws"):
                e = edges(s, p)
                row.append(f"{money(e['hosts'] + e['bw'])} ({e['n']} x {e['sku']})")
            rows.append(row)
    table(["load", "consumers", "OVH", "Hetzner", "AWS (egress + c7gn)"], rows)

    print("## Monthly totals\n")
    rows = []
    for name, m, mesh in SIZED:
        for c in CONSUMERS:
            s = scenario(m, c, mesh=mesh)
            rows.append([name, c] + [money(t["total"]) if (t := total(s, p, st)) else "n/a" for p, st in COMBOS])
    table(["load", "consumers"] + [f"{p} + {st}" for p, st in COMBOS], rows)

    print("## Where the money goes (100 consumers)\n")
    for p, st in (("ovh", "r2"), ("aws", "s3")):
        rows = []
        for name, m, mesh in SIZED:
            s = scenario(m, 100, mesh=mesh)
            t = total(s, p, st)
            rows.append([name, money(t["cores"]), f"{money(t['edges'])} ({t['edge_n']})", money(t["bucket_req"]),
                         money(t["bucket_storage"]), money(t["bandwidth"]), money(t["total"])])
        print(f"{p} + {st}:\n")
        table(["load", "core nodes", "edges (n)", "bucket requests", "bucket storage", "metered bandwidth", "total"], rows)

    print("## Linger\n")
    rows = []
    for name, m in LOADS:
        s0 = scenario(m)
        row = [name, s0["nodes"]]
        for L in (0.010, 0.025, 0.050, 0.100, 0.250):
            s = scenario(m, nodes=s0["nodes"], linger=L)
            row.append(f"{s['seals_s']:.0f} / {money(s['seals_s'] * MONTH_S / 1e6 * 5)}")
        rows.append(row)
    table(["load", "nodes", "10 ms", "25 ms (default)", "50 ms", "100 ms", "250 ms"], rows)
    print("(segment PUTs/s / their S3 $/mo; R2 is 10% less)\n")

    print("## Replicas and public segments\n")
    rows = []
    for name, m in LOADS:
        s = scenario(m)
        poll = s["nodes"] * REPLICA_WINDOW / (REPLICA_POLL_S + GET_LATENCY_S)
        gets = max(poll, s["seals_s"])
        tb = s["log_gbps_zstd"] / 8 * MONTH_S / 1e3
        rows.append([name, s["nodes"], f"{gets:.0f}", money(gets * MONTH_S / 1e6 * 0.36), f"{tb:,.0f}",
                     money(gets * MONTH_S / 1e6 * 0.40 + aws_egress(tb))])
    table(["load", "logs", "GETs/s per replica", "R2 $/mo", "TB/mo pulled", "S3 $/mo, replica outside AWS"], rows)
    rows = []
    for name, m in LOADS:
        s = scenario(m)
        day_tb = s["rate"] * FRAME_B / ZSTD_RATIO * 86400 / TB
        gets = s["seals_s"] * 86400
        rows.append([name, f"{day_tb:,.2f}", f"{gets / 1e6:,.1f}M", money(gets / 1e6 * 0.36), money(aws_egress(day_tb)),
                     f"{day_tb * 8e3 / (3 * NIC_UTIL) / 3600:,.1f} h"])
    table(["load", "one 24 h replay, TB (zstd)", "GETs", "R2 public", "S3 / AWS egress", "an OVH 3 Gb/s port"], rows)

    print("## vlpds comparison\n")
    s = scenario(1)
    for k in ("s3", "r2"):
        c = bucket_cost(s, k)
        print(f"relay today, 3 nodes, {k}: ${c['requests'] + c['storage']:,.0f}/mo = "
              f"${(c['requests'] + c['storage']) / (RATE_AVG * MONTH_S / 1e6):.2f} per M events")
    print()


if __name__ == "__main__":
    main()

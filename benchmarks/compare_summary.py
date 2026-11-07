#!/usr/bin/env python3
"""Summarise compare_stacks.sh output: Python (py) vs Go (go).

usage: python3 benchmarks/compare_summary.py [results_dir]   (default benchmarks/results_compare)
Standard library only.
"""
import csv
import os
import re
import statistics
import sys

root = sys.argv[1] if len(sys.argv) > 1 else "benchmarks/results_compare"
STACKS = ["py", "go"]


def to_mi(s):
    m = re.fullmatch(r"(\d+(?:\.\d+)?)(Ki|Mi|Gi)?", s)
    if not m:
        raise ValueError(s)
    return float(m.group(1)) * {"Ki": 1 / 1024, "Mi": 1, "Gi": 1024, None: 1 / 1048576}[m.group(2)]


def to_millicores(s):
    return float(s[:-1]) if s.endswith("m") else float(s) * 1000


def parse_top(path):
    """-> list of samples: {"pods": {pod: {container: (cpu_m, mem_mi)}}, "node": (used_mi, avail_mi)}"""
    samples, cur = [], None
    if not os.path.exists(path):
        return samples
    for line in open(path):
        p = line.split()
        if not p:
            continue
        if p[0] == "==":
            cur = {"pods": {}, "node": None}
            samples.append(cur)
        elif cur is None:
            continue
        elif p[0] == "Mem:" and len(p) >= 7:
            cur["node"] = (float(p[2]), float(p[6]))  # used, available
        elif len(p) == 4:
            pod, container, cpu, mem = p
            try:
                cur["pods"].setdefault(pod, {})[container] = (to_millicores(cpu), to_mi(mem))
            except ValueError:
                pass  # error line from kubectl, not a data row
    return samples


def collect(samples):
    m = {k: [] for k in ("unit_mem", "unit_cpu", "worker_mem", "llama_mem", "router_mem", "node_used", "node_avail")}
    for s in samples:
        for pod, cs in s["pods"].items():
            if pod.startswith("worker"):
                m["unit_mem"].append(sum(v[1] for v in cs.values()))  # whole Pod, all containers
                m["unit_cpu"].append(sum(v[0] for v in cs.values()))
                if "worker" in cs:
                    m["worker_mem"].append(cs["worker"][1])
                if "llama-server" in cs:
                    m["llama_mem"].append(cs["llama-server"][1])
            elif pod.startswith("router"):
                m["router_mem"].append(sum(v[1] for v in cs.values()))
        if s["node"]:
            m["node_used"].append(s["node"][0])
            m["node_avail"].append(s["node"][1])
    return m


def mean(xs):
    return statistics.fmean(xs) if xs else None


def peak(xs):
    return max(xs) if xs else None


def low(xs):
    return min(xs) if xs else None


def size_mb(s):
    m = re.fullmatch(r"([\d.]+)\s*([kKMGT]?B)", s)
    if not m:
        return None
    return float(m.group(1)) * {"B": 1e-6, "kB": 1e-3, "KB": 1e-3, "MB": 1, "GB": 1e3, "TB": 1e6}[m.group(2)]


IMAGES = {
    "py": {"router": [r"(^|/)llm-router$"], "worker": [r"(^|/)llm-worker$"]},
    "go": {"router": [r"(^|/)llm-router-go$"], "worker": [r"(^|/)llm-worker-go$", r"llama\.cpp"]},
}


def image_sizes(path, stack):
    out = {"router": None, "worker": None}
    if not os.path.exists(path):
        return out
    seen = set()
    for line in open(path):
        p = line.split()
        if len(p) < 4:
            continue
        image, image_id, size = p[0], p[2], size_mb(p[-1])
        if size is None or (image, image_id) in seen:
            continue
        for role, pats in IMAGES[stack].items():
            if any(re.search(pt, image) for pt in pats):
                seen.add((image, image_id))
                out[role] = (out[role] or 0) + size
    return out


def read_meta(path):
    meta = {}
    if os.path.exists(path):
        for line in open(path):
            for kv in line.split():
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    meta[k] = v
    return meta


def locust(d):
    p = os.path.join(d, "load_stats.csv")
    if not os.path.exists(p):
        return {}
    rows = {r["Name"]: r for r in csv.DictReader(open(p))}
    agg = rows.get("Aggregated")
    if not agg or float(agg["Request Count"]) == 0 or float(agg["Requests/s"]) == 0:
        return {}
    duration = float(agg["Request Count"]) / float(agg["Requests/s"])
    ok = fail = 0
    p50 = p95 = None
    good, bad = rows.get("/generate"), rows.get("/generate (failed)")
    if good:
        ok = int(good["Request Count"]) - int(good["Failure Count"])
        fail += int(good["Failure Count"])
        p50, p95 = float(good["50%"]) / 1000, float(good["95%"]) / 1000
    if bad:
        fail += int(bad["Request Count"])
    return {"ok": ok, "fail": fail, "p50": p50, "p95": p95, "rps": ok / duration}


def num(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def summarize(stack):
    d = os.path.join(root, stack)
    if not os.path.isdir(d):
        return None
    idle = collect(parse_top(os.path.join(d, "idle_top.txt")))
    load = collect(parse_top(os.path.join(d, "load_top.txt")))
    meta = read_meta(os.path.join(d, "meta.txt"))
    img = image_sizes(os.path.join(d, "images.txt"), stack)
    lc = locust(d)
    cpu = mean(load["unit_cpu"])
    return {
        "mem_idle": mean(idle["unit_mem"]),
        "mem_load": mean(load["unit_mem"]),
        "mem_peak": peak(load["unit_mem"]),
        "mem_worker": mean(load["worker_mem"]),
        "mem_llama": mean(load["llama_mem"]),
        "cpu": cpu / 1000 if cpu is not None else None,
        "router_idle": mean(idle["router_mem"]),
        "router_peak": peak(load["router_mem"]),
        "node_used_peak": peak(load["node_used"]),
        "node_avail_min": low(load["node_avail"]),
        "img_router": img["router"],
        "img_worker": img["worker"],
        "cold": num(meta.get("cold_start_s")),
        "rp_data": num(meta.get("redpanda_data_mb")),
        "ok": lc.get("ok"),
        "fail": lc.get("fail"),
        "p50": lc.get("p50"),
        "p95": lc.get("p95"),
        "rps": lc.get("rps"),
    }


ROWS = [
    ("mem_idle", "Worker Pod memory, idle (MiB)", 0),
    ("mem_load", "Worker Pod memory, under load, mean (MiB)", 0),
    ("mem_peak", "Worker Pod memory, under load, peak (MiB)", 0),
    ("mem_worker", "  of which worker container (MiB)", 0),
    ("mem_llama", "  of which llama-server container (MiB)", 0),
    ("cpu", "Worker Pod CPU, under load, mean (cores)", 2),
    ("router_idle", "Router memory, idle (MiB)", 0),
    ("router_peak", "Router memory, under load, peak (MiB)", 0),
    ("node_used_peak", "Node memory used, peak (MiB)", 0),
    ("node_avail_min", "Node memory available, minimum (MiB)", 0),
    ("img_router", "Router image on disk (MB)", 0),
    ("img_worker", "Worker-side images on disk (MB)", 0),
    ("cold", "Worker Pod cold start (s, single sample)", 0),
    ("rp_data", "Redpanda data after the run (MB)", 0),
    ("ok", "Successful requests", 0),
    ("fail", "Failed requests", 0),
    ("p50", "Median latency, successful (s)", 1),
    ("p95", "p95 latency, successful (s)", 1),
    ("rps", "Successful throughput (req/s)", 3),
]


def fmt(v, dec):
    return "-" if v is None else f"{v:,.{dec}f}"


res = {s: summarize(s) for s in STACKS}
present = [s for s in STACKS if res[s]]
if not present:
    sys.exit(f"no results under {root}/py or {root}/go")

for s in present:
    meta = read_meta(os.path.join(root, s, "meta.txt"))
    print(f"{s}: workers={meta.get('workers', '?')} users={meta.get('users', '?')} duration={meta.get('duration', '?')}")
print()

w = 46
head = f"{'':{w}}" + "".join(f"{s:>12}" for s in present) + (f"{'go vs py':>12}" if len(present) == 2 else "")
print(head)
print("-" * len(head))
for key, label, dec in ROWS:
    line = f"{label:{w}}" + "".join(f"{fmt(res[s][key], dec):>12}" for s in present)
    if len(present) == 2:
        a, b = res["py"][key], res["go"][key]
        line += f"{(b - a) / a * 100:>+11.0f}%" if a not in (None, 0) and b is not None else f"{'':>12}"
    print(line)

print(
    "\nNotes: memory/CPU come from kubectl top (metrics-server, smoothed over ~15 s windows). "
    "'Worker Pod' = all containers in the Pod: the Python worker alone vs the Go worker + llama-server. "
    "Image sizes are what containerd reports. Small samples: one run per stack."
)

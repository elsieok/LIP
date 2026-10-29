import glob
import os
import sys

import matplotlib.pyplot as plt
import pandas as pd

d = sys.argv[1] if len(sys.argv) > 1 else "benchmarks/results_v2"
rows = []
for f in sorted(glob.glob(os.path.join(d, "*_stats.csv"))):
    run = os.path.basename(f)[: -len("_stats.csv")]
    st = pd.read_csv(f)
    agg = st[st["Name"] == "Aggregated"]
    if agg.empty or agg.iloc[0]["Request Count"] == 0:
        print(f"skipping {run}: no requests recorded")
        continue
    agg = agg.iloc[0]
    duration = agg["Request Count"] / agg["Requests/s"]
    ok_row = st[st["Name"] == "/generate"]
    bad_row = st[st["Name"] == "/generate (failed)"]
    ok_n = fail_n = 0
    p50 = p95 = float("nan")
    if len(ok_row):
        o = ok_row.iloc[0]
        ok_n = int(o["Request Count"] - o["Failure Count"])
        fail_n += int(o["Failure Count"])
        p50, p95 = o["50%"] / 1000, o["95%"] / 1000
    if len(bad_row):
        fail_n += int(bad_row.iloc[0]["Request Count"])
    if fail_n and not len(bad_row):
        print(f"warning: {run}: failures are mixed into the latency columns")
    rejected = 0
    ff = os.path.join(d, run + "_failures.csv")
    if os.path.exists(ff):
        fl = pd.read_csv(ff)
        if len(fl):
            rejected = int(fl[fl["Error"].astype(str).str.contains("503")]["Occurrences"].sum())
    rows.append({
        "run": run, "ok": ok_n, "rejected_503": rejected, "timeouts": fail_n - rejected,
        "ok_rps": round(ok_n / duration, 3), "p50_s": round(p50, 1), "p95_s": round(p95, 1),
    })

res = pd.DataFrame(rows).set_index("run")
print(res.to_string())

fig, ax = plt.subplots(1, 3, figsize=(17, 4.8))
res["ok_rps"].plot.bar(ax=ax[0], color="#2a6f97", title="Successful requests/s")
res["p50_s"].plot.bar(ax=ax[1], color="#6a994e", title="Median latency, successful requests (s)")
res[["rejected_503", "timeouts"]].plot.bar(ax=ax[2], stacked=True, color=["#e9a03b", "#c0392b"], title="Failed requests")
for a in ax:
    a.tick_params(axis="x", rotation=55)
    a.set_xlabel("")
plt.tight_layout()
plt.savefig(os.path.join(d, "summary.png"), dpi=150)
print("wrote", os.path.join(d, "summary.png"))
function table(el, head, rows) {
  el.innerHTML = `<thead><tr>${head.map(([h, cls]) => `<th class="${cls || ""}">${h}</th>`).join("")}</tr></thead>
    <tbody>${rows.map((r) => `<tr>${r.map(([v, cls]) => `<td class="${cls || ""}">${v}</td>`).join("")}</tr>`).join("")}</tbody>`;
}

function render(m) {
  document.getElementById("run-meta").textContent =
    `Run: ${new Date(m.generated_at).toUTCString()} · ${m.environment} · ` +
    `${m.params.rounds} simulated hours, ${m.params.customers} customers, seed ${m.params.seed}.`;

  const det = Object.values(m.detection);
  const planted = det.reduce((a, r) => a + r.planted, 0);
  const caught = det.reduce((a, r) => a + r.caught, 0);
  const lag = m.rounds.map((r) => r.capture_job_lag_s);
  document.getElementById("tiles").innerHTML = [
    [fmt.int(m.table_counts.cdc_event_audit), "events published to Kafka"],
    [fmt.secs(m.latency_seconds.p50), `median latency (p95 ${fmt.secs(m.latency_seconds.p95)})`],
    [`${caught}/${planted}`, "planted fraud cases caught"],
    [fmt.int(m.rejects_to_dlq), "malformed partner rows sent to the DLQ"],
    [fmt.secs(Math.max(...lag)), "worst SQL Server capture-job lag (heartbeat)"],
    [fmt.int(m.table_counts.cdc_file_manifest), "partner files processed"],
  ].map(([v, l]) => `<div class="card stat"><div class="value">${v}</div><div class="label">${l}</div></div>`).join("");

  const rules = Object.keys(m.detection);
  const lat = m.latency_seconds.by_alert || [];
  if (lat.length) dotStrip(document.getElementById("latency-chart"), document.getElementById("latency-legend"), lat, rules);
  else document.getElementById("latency-chart").innerHTML =
    `<p class="empty">p50 ${fmt.secs(m.latency_seconds.p50)}, p95 ${fmt.secs(m.latency_seconds.p95)}, max ${fmt.secs(m.latency_seconds.max)} over ${m.latency_seconds.n} alerts.</p>`;

  table(document.getElementById("detection-table"),
    [["Rule"], ["Alerts", "num"], ["True positives", "num"], ["False positives", "num"],
     ["Planted", "num"], ["Caught", "num"], ["Precision", "num"], ["Recall", "num"]],
    Object.entries(m.detection).map(([rule, r]) => [
      [`<code>${rule}</code>`], [r.alerts, "num"], [r.true_positives, "num"], [r.false_positives, "num"],
      [r.planted, "num"], [r.caught, "num"], [fmt.pct(r.precision), "num"], [fmt.pct(r.recall), "num"],
    ]));

  const ops = { c: "insert (c)", u: "update (u)", d: "delete (d)", r: "snapshot (r)" };
  const byTable = {};
  m.events_by_table_op.forEach((e) => {
    byTable[e.source_table] ??= {};
    byTable[e.source_table][ops[e.op]] = e.n;
  });
  stackedBars(document.getElementById("ops-chart"), document.getElementById("ops-legend"),
    Object.entries(byTable).map(([label, values]) => ({ label, values })),
    ["insert (c)", "update (u)", "delete (d)"]);

  const stepGroups = { capture: (k) => k.startsWith("capture:"), bronze: (k) => k === "bronze",
    silver: (k) => k === "silver", "gold + reconcile": (k) => k === "gold" || k === "reconcile" };
  stackedBars(document.getElementById("steps-chart"), document.getElementById("steps-legend"),
    m.rounds.map((r) => ({
      label: r.label,
      values: Object.fromEntries(Object.entries(stepGroups).map(([g, f]) =>
        [g, Object.entries(r.step_seconds).filter(([k]) => f(k)).reduce((a, [, v]) => a + v, 0)])),
    })),
    Object.keys(stepGroups), { unit: " s" });

  table(document.getElementById("recon-table"),
    [["Table"], ["Check"], ["Source", "num"], ["Delta", "num"], ["Status"]],
    m.rounds.at(-1).reconciliation.map((c) => [
      [`<code>${fmt.esc(c.table_name)}</code>`], [fmt.esc(c.check_name)],
      [fmt.esc(c.source_value), "num"], [fmt.esc(c.target_value), "num"],
      [`<span class="tag ${c.status === "PASS" ? "ok" : "bad"}">${c.status}</span>`],
    ]));

  table(document.getElementById("counts-table"), [["Table"], ["Rows", "num"]],
    Object.entries(m.table_counts).map(([t, n]) => [[`<code>${t}</code>`], [fmt.int(n), "num"]]));
}

let metrics;
loadMetrics().then((m) => { metrics = m; render(m); }).catch((e) => {
  document.getElementById("tiles").innerHTML =
    `<div class="card"><p class="empty">Run results not available (${fmt.esc(e.message)}).</p></div>`;
});
document.addEventListener("themechange", () => metrics && render(metrics));

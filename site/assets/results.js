function table(el, head, rows) {
  el.innerHTML = `<thead><tr>${head.map(([h, cls]) => `<th class="${cls || ""}">${h}</th>`).join("")}</tr></thead>
    <tbody>${rows.map((r) => `<tr>${r.map(([v, cls]) => `<td class="${cls || ""}">${v}</td>`).join("")}</tr>`).join("")}</tbody>`;
}

const RULE_NAMES = { card_velocity: "Card velocity", impossible_travel: "Impossible travel",
  structuring: "Structuring" };
const RULE_TERMS = { card_velocity: "velocity", impossible_travel: "impossible-travel",
  structuring: "structuring" };
const CHECK_NAMES = { row_count: "rows still live", balance_sum: "sum of balances",
  audit_to_bronze_missing: "events sent but not in Bronze" };

function render(m) {
  document.getElementById("run-meta").textContent =
    `Run: ${new Date(m.generated_at).toUTCString()} · ${m.environment} · ` +
    `${m.params.rounds} simulated hours, ${m.params.customers} customers, seed ${m.params.seed}.`;

  const det = Object.values(m.detection);
  const planted = det.reduce((a, r) => a + r.planted, 0);
  const caught = det.reduce((a, r) => a + r.caught, 0);
  const lag = m.rounds.map((r) => r.capture_job_lag_s);
  document.getElementById("tiles").innerHTML = [
    [fmt.int(m.table_counts.cdc_event_audit), "changes published to Kafka"],
    [fmt.secs(m.latency_seconds.p50), `typical alert time (p50); slowest 5% above ${fmt.secs(m.latency_seconds.p95)} (p95)`],
    [`${caught}/${planted}`, "planted fraud cases caught"],
    [fmt.int(m.rejects_to_dlq), "bad partner rows set aside in the dead-letter queue"],
    [fmt.secs(Math.max(...lag)), "longest SQL Server capture delay (heartbeat)"],
    [fmt.int(m.table_counts.cdc_file_manifest), "partner files processed"],
  ].map(([v, l]) => `<div class="card stat"><div class="value">${v}</div><div class="label">${l}</div></div>`).join("");

  const rules = Object.keys(m.detection);
  const lat = m.latency_seconds.by_alert || [];
  if (lat.length) dotStrip(document.getElementById("latency-chart"), document.getElementById("latency-legend"), lat, rules);
  else document.getElementById("latency-chart").innerHTML =
    `<p class="empty">p50 ${fmt.secs(m.latency_seconds.p50)}, p95 ${fmt.secs(m.latency_seconds.p95)}, max ${fmt.secs(m.latency_seconds.max)} over ${m.latency_seconds.n} alerts.</p>`;

  table(document.getElementById("detection-table"),
    [["Rule"], ["Alerts", "num"], ["Real fraud", "num"],
     ['<span data-term="false-positive">False alarms</span>', "num"],
     ["Planted", "num"], ["Caught", "num"],
     ['<span data-term="precision">Precision</span>', "num"],
     ['<span data-term="recall">Recall</span>', "num"]],
    Object.entries(m.detection).map(([rule, r]) => [
      [`<span data-term="${RULE_TERMS[rule] || ""}">${RULE_NAMES[rule] || rule}</span>`], [r.alerts, "num"], [r.true_positives, "num"], [r.false_positives, "num"],
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
    [["Table"], ["Check"], ["Source system", "num"], ["Copy in Delta", "num"], ["Result"]],
    m.rounds.at(-1).reconciliation.map((c) => [
      [`<code>${fmt.esc(c.table_name)}</code>`], [CHECK_NAMES[c.check_name] || fmt.esc(c.check_name)],
      [fmt.esc(c.source_value), "num"], [fmt.esc(c.target_value), "num"],
      [`<span class="tag ${c.status === "PASS" ? "ok" : "bad"}">${c.status}</span>`],
    ]));

  table(document.getElementById("counts-table"), [["Table"], ["Rows", "num"]],
    Object.entries(m.table_counts).map(([t, n]) => [[`<code>${t}</code>`], [fmt.int(n), "num"]]));
  if (window.enhanceTerms) window.enhanceTerms(document.querySelector("main"));
}

let metrics;
loadMetrics().then((m) => { metrics = m; render(m); }).catch((e) => {
  document.getElementById("tiles").innerHTML =
    `<div class="card"><p class="empty">Run results not available (${fmt.esc(e.message)}).</p></div>`;
});
document.addEventListener("themechange", () => metrics && render(metrics));

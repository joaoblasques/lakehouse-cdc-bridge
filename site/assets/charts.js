// Minimal SVG charts (no library). Colors come from CSS tokens so both themes work.
const SERIES = ["--s1", "--s2", "--s3", "--s4"];
const cssVar = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();

function niceMax(v) {
  if (v <= 0) return 1;
  const p = 10 ** Math.floor(Math.log10(v));
  return [1, 2, 2.5, 5, 10].map((m) => m * p).find((m) => m >= v);
}

/** Horizontal stacked bars. rows: [{label, values: {seriesName: n}}] */
function stackedBars(el, legendEl, rows, seriesNames, { unit = "" } = {}) {
  const W = 760, rowH = 30, left = 190, right = 60, top = 8;
  const H = top + rows.length * rowH + 26;
  const max = niceMax(Math.max(...rows.map((r) => seriesNames.reduce((a, s) => a + (r.values[s] || 0), 0))));
  const x = (v) => left + (v / max) * (W - left - right);
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((t) => t * max);
  let svg = `<svg class="chart" viewBox="0 0 ${W} ${H}" role="img">`;
  ticks.forEach((t) => {
    svg += `<line class="grid-line" x1="${x(t)}" x2="${x(t)}" y1="${top}" y2="${H - 22}"/>`;
    svg += `<text x="${x(t)}" y="${H - 6}" text-anchor="middle">${fmt.int(Math.round(t))}${unit}</text>`;
  });
  rows.forEach((r, i) => {
    const y = top + i * rowH + 5;
    let acc = 0;
    svg += `<text x="${left - 10}" y="${y + 15}" text-anchor="end">${fmt.esc(r.label)}</text>`;
    seriesNames.forEach((s, si) => {
      const v = r.values[s] || 0;
      if (!v) return;
      const w = x(acc + v) - x(acc);
      svg += `<rect x="${x(acc)}" y="${y}" width="${Math.max(w - 1, 1)}" height="20" rx="3"
               fill="${cssVar(SERIES[si % SERIES.length])}"><title>${fmt.esc(r.label)} · ${s}: ${fmt.int(v)}${unit}</title></rect>`;
      acc += v;
    });
    svg += `<text x="${x(acc) + 6}" y="${y + 15}">${unit ? acc.toFixed(1) : fmt.int(acc)}${unit}</text>`;
  });
  el.innerHTML = svg + "</svg>";
  legendEl.innerHTML = seriesNames.map((s, si) =>
    `<span><i style="background:${cssVar(SERIES[si % SERIES.length])}"></i>${fmt.esc(s)}</span>`).join("");
}

/** Dot strip: one dot per alert on a seconds axis, one lane per rule. */
function dotStrip(el, legendEl, points, lanes) {
  const W = 760, laneH = 34, left = 150, right = 30, top = 10;
  const H = top + lanes.length * laneH + 26;
  const max = niceMax(Math.max(1, ...points.map((p) => p.seconds)));
  const x = (v) => left + (v / max) * (W - left - right);
  let svg = `<svg class="chart" viewBox="0 0 ${W} ${H}" role="img">`;
  [0, 0.25, 0.5, 0.75, 1].map((t) => t * max).forEach((t) => {
    svg += `<line class="grid-line" x1="${x(t)}" x2="${x(t)}" y1="${top}" y2="${H - 22}"/>`;
    svg += `<text x="${x(t)}" y="${H - 6}" text-anchor="middle">${t.toFixed(0)} s</text>`;
  });
  lanes.forEach((lane, li) => {
    const y = top + li * laneH + laneH / 2;
    svg += `<text x="${left - 10}" y="${y + 4}" text-anchor="end">${fmt.esc(lane)}</text>`;
    points.filter((p) => p.rule === lane).forEach((p, pi) => {
      const jitter = ((pi % 5) - 2) * 3;
      svg += `<circle cx="${x(p.seconds)}" cy="${y + jitter}" r="5" fill-opacity="0.75"
                fill="${cssVar(SERIES[li % SERIES.length])}"><title>${fmt.esc(lane)}: ${p.seconds.toFixed(1)} s</title></circle>`;
    });
  });
  el.innerHTML = svg + "</svg>";
  legendEl.innerHTML = `<span>One dot per alert. Hover for the exact value.</span>`;
}

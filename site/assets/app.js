// Shared header, footer and theme toggle. Pages declare <body data-page="...">.
const REPO = "https://github.com/joaoblasques/lakehouse-cdc-bridge";
const PAGES = [
  ["index.html", "Overview", "overview"],
  ["architecture.html", "Architecture", "architecture"],
  ["cdc.html", "CDC patterns", "cdc"],
  ["results.html", "Results", "results"],
  ["lineage.html", "Traceability", "lineage"],
  ["decisions.html", "Decisions", "decisions"],
  ["setup.html", "Setup", "setup"],
  ["run.html", "Deploy", "run"],
  ["glossary.html", "Glossary", "glossary"],
];

function storageGet(key) {
  try { return localStorage.getItem(key); } catch { return null; }
}
function storageSet(key, value) {
  try { localStorage.setItem(key, value); } catch { /* private mode: theme just won't persist */ }
}

function applyTheme(theme) {
  if (theme) document.documentElement.dataset.theme = theme;
  else delete document.documentElement.dataset.theme;
}
applyTheme(storageGet("theme"));

function currentTheme() {
  const set = document.documentElement.dataset.theme;
  if (set) return set;
  return matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
}

function renderChrome() {
  const page = document.body.dataset.page;
  const header = document.createElement("header");
  header.className = "site-header";
  header.innerHTML = `
    <nav class="nav" aria-label="Main">
      <a class="brand" href="index.html">Lakehouse <span>CDC</span> Bridge</a>
      <div class="nav-links">
        ${PAGES.map(([href, label, key]) =>
          `<a href="${href}"${key === page ? ' aria-current="page"' : ""}>${label}</a>`).join("")}
      </div>
      <button class="icon-btn" id="theme-toggle" type="button" aria-label="Toggle dark mode"><svg viewBox="0 0 16 16" aria-hidden="true"><path d="M8 1a7 7 0 1 0 0 14A7 7 0 0 0 8 1Zm0 1.5v11a5.5 5.5 0 0 1 0-11Z"/></svg></button>
    </nav>`;
  document.body.prepend(header);

  const footer = document.createElement("footer");
  footer.className = "site-footer";
  footer.innerHTML = `<div>
      <span>Lakehouse CDC Bridge · João Blasques · synthetic data only, no real customers</span>
      <span><a href="${REPO}">Source</a> · <a href="${REPO}/blob/main/docs/design.md">Design doc</a></span>
    </div>`;
  document.body.append(footer);

  document.getElementById("theme-toggle").addEventListener("click", () => {
    const next = currentTheme() === "dark" ? "light" : "dark";
    applyTheme(next);
    storageSet("theme", next);
    document.dispatchEvent(new CustomEvent("themechange"));
  });
}

async function loadMetrics() {
  const res = await fetch("data/run_metrics.json", { cache: "no-store" });
  if (!res.ok) throw new Error(`run_metrics.json: HTTP ${res.status}`);
  return res.json();
}

const fmt = {
  int: (n) => (n ?? 0).toLocaleString("en-GB"),
  pct: (x) => (x == null ? "–" : `${Math.round(x * 100)}%`),
  secs: (s) => (s == null ? "–" : s < 60 ? `${s.toFixed(1)} s` : `${(s / 60).toFixed(1)} min`),
  esc: (s) => String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]),
};

renderChrome();

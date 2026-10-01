// Every technical term on the site, defined once. Used by the inline popovers and the
// Glossary page. Mark a term in HTML with <span data-term="key">visible text</span>.
const GLOSSARY = {
  // ---- CDC and data movement -------------------------------------------------------------
  cdc: { term: "Change data capture (CDC)", group: "Change data capture",
    short: "Detecting every insert, update and delete in a source system and sending just those changes on, instead of copying the whole table again.",
    long: "A bank's accounts table might hold millions of rows while only a few thousand change each hour. CDC picks up those few thousand changes as they happen, so downstream systems stay current without re-reading everything." },
  "log-cdc": { term: "Log-based CDC", group: "Change data capture",
    short: "CDC that reads the database's own transaction log, the record every database keeps of each change it commits.",
    long: "It catches inserts, updates and deletes, puts almost no load on the source, and keeps the exact order of changes. It needs the database to expose its log, which SQL Server does through its built-in CDC feature." },
  "query-cdc": { term: "Query-based CDC", group: "Change data capture",
    short: "CDC that repeatedly asks the table \"what changed since last time?\", using a timestamp column.",
    long: "Used when you can't read the log. It is simpler but misses hard deletes (a deleted row is simply gone), so it is paired with a periodic snapshot comparison." },
  "file-cdc": { term: "File-arrival CDC", group: "Change data capture",
    short: "Treating each new or changed file in a folder as a set of changes.",
    long: "Partners often send data as files. The pipeline lists the folder, compares each file with what it has already processed, and turns the rows of new or changed files into change events." },
  "transaction-log": { term: "Transaction log", group: "Change data capture",
    short: "The internal journal where a database writes every change before applying it, so it can recover after a crash." },
  lsn: { term: "LSN (log sequence number)", group: "Change data capture",
    short: "SQL Server's position number for each entry in its transaction log. Later changes always have higher LSNs.",
    long: "Storing the last LSN processed tells the pipeline exactly where to resume. On the Traceability page, the LSN is the proof of which database transaction produced a change." },
  watermark: { term: "Watermark", group: "Change data capture",
    short: "A bookmark: the last position the pipeline has fully processed (an LSN, a timestamp, or a list of files).",
    long: "Each run reads from the watermark onwards, then moves the watermark forward. It is saved only after the changes are safely delivered, so a crash never skips data." },
  "capture-job": { term: "Capture job (SQL Server Agent)", group: "Change data capture",
    short: "A background job inside SQL Server that copies changes from the transaction log into readable change tables every few seconds." },
  retention: { term: "CDC retention", group: "Change data capture",
    short: "How long SQL Server keeps changes in its change tables before deleting them (3 days by default).",
    long: "If the pipeline stops for longer than that, changes are lost. The adapter detects this and stops with an error instead of silently skipping them." },
  heartbeat: { term: "Heartbeat", group: "Change data capture",
    short: "A tiny row the pipeline updates on purpose. Seeing it come back through CDC proves the capture is working and shows how far behind it is." },
  snapshot: { term: "Snapshot / hash diff", group: "Change data capture",
    short: "Taking a full copy of a table and comparing a fingerprint (hash) of each row with the previous copy, to find rows that changed or disappeared." },
  "row-change-ts": { term: "Row change timestamp", group: "Change data capture",
    short: "A column DB2 fills in by itself with the time a row was inserted or last updated. Query-based CDC uses it to find recent changes." },
  "safety-lag": { term: "Safety lag", group: "Change data capture",
    short: "Leaving the most recent few seconds of changes for the next run, because a transaction that stamped a row may not have committed yet." },
  etag: { term: "ETag", group: "Change data capture",
    short: "A version tag that cloud storage assigns to a file. If the file changes, the ETag changes." },
  adapter: { term: "Source adapter", group: "Change data capture",
    short: "The piece of code that knows how to read changes from one kind of source. Every adapter outputs the same event format." },

  // ---- Events and Kafka ----------------------------------------------------------------
  event: { term: "Change event", group: "Events and Kafka",
    short: "One message describing one change: which row, what kind of change (insert, update, delete), and the row before and after." },
  envelope: { term: "Event envelope", group: "Events and Kafka",
    short: "The fixed outer format every change event uses, whatever its source. Consumers only learn one format." },
  "event-id": { term: "Deterministic event ID", group: "Events and Kafka",
    short: "An ID computed from the change itself (source, table, row key, log position). The same change always gets the same ID.",
    long: "If a change is accidentally sent twice, both copies share an ID, so the second one is recognised and dropped." },
  hash: { term: "Hash (SHA-256)", group: "Events and Kafka",
    short: "A fixed-length fingerprint of some data. Change one character and the fingerprint changes completely, so it proves content hasn't been altered." },
  kafka: { term: "Apache Kafka", group: "Events and Kafka",
    short: "A system that stores streams of messages durably and lets many applications read them independently, at their own pace.",
    long: "Think of it as a shared, append-only log for the whole company. Producers write to it once; any number of consumers read from it without touching the source systems." },
  confluent: { term: "Confluent", group: "Events and Kafka",
    short: "The company behind Kafka's commercial platform. Confluent Cloud is Kafka run as a managed service." },
  topic: { term: "Topic", group: "Events and Kafka",
    short: "A named stream of messages in Kafka, like a table of events. Here there is one topic per source table." },
  partition: { term: "Partition", group: "Events and Kafka",
    short: "A topic is split into partitions so it can scale. Messages with the same key always go to the same partition, which keeps their order." },
  offset: { term: "Offset", group: "Events and Kafka",
    short: "A message's position number inside a partition. Topic + partition + offset points to exactly one message." },
  "message-key": { term: "Message key", group: "Events and Kafka",
    short: "A value attached to each message that decides its partition. Using the row's ID as the key keeps all changes to one row in order." },
  producer: { term: "Producer / consumer", group: "Events and Kafka",
    short: "A producer writes messages to Kafka; a consumer reads them. This pipeline is a producer (capture) and a consumer (Bronze)." },
  idempotent: { term: "Idempotent", group: "Events and Kafka",
    short: "Safe to repeat: doing it twice has the same result as doing it once.",
    long: "Kafka's idempotent producer won't create duplicates when it retries a send. The Delta steps are idempotent too, so a re-run after a failure is harmless." },
  acks: { term: "acks=all", group: "Events and Kafka",
    short: "A producer setting: a message only counts as sent once every copy of it in Kafka has been written." },
  dlq: { term: "Dead-letter queue (DLQ)", group: "Events and Kafka",
    short: "A separate topic for records that couldn't be processed, kept with the reason, so bad data is visible and fixable instead of silently dropped." },
  "schema-registry": { term: "Schema Registry", group: "Events and Kafka",
    short: "A Confluent service that stores the agreed format of each topic's messages and rejects changes that would break consumers." },
  "json-schema": { term: "JSON Schema", group: "Events and Kafka",
    short: "A standard way to write down the required shape of a JSON message (fields, types, allowed values), so it can be checked automatically." },
  "at-least-once": { term: "At-least-once / exactly-once", group: "Events and Kafka",
    short: "At-least-once: nothing is lost, but a message may arrive twice. Exactly-once: each change takes effect once.",
    long: "This project accepts at-least-once delivery over Kafka and achieves exactly-once results in Delta by dropping duplicates using the deterministic event ID." },
  outbox: { term: "Outbox pattern", group: "Events and Kafka",
    short: "Mark a record as \"to be sent\", send it, and clear the mark only after the message system confirms. A crash can resend, but never lose, a message." },
  "commit-offset": { term: "Committing an offset", group: "Events and Kafka",
    short: "A consumer telling Kafka how far it has read. After a restart it continues from the last committed position." },
  "consumer-group": { term: "Consumer group", group: "Events and Kafka",
    short: "Consumers that share the work of reading a topic. Kafka remembers the group's position, so a restarted member resumes where the group left off." },
  sasl: { term: "SASL_SSL", group: "Events and Kafka",
    short: "Encrypted, authenticated connection to Kafka. Used with an API key and secret when connecting to Confluent Cloud." },

  // ---- Databricks, Spark and Delta -------------------------------------------------------
  databricks: { term: "Databricks", group: "Databricks, Spark and Delta",
    short: "A cloud data platform built around Apache Spark. Teams write notebooks and schedule them as jobs." },
  notebook: { term: "Notebook", group: "Databricks, Spark and Delta",
    short: "A document mixing code and notes, run cell by cell. In this project each pipeline step is one small notebook." },
  workflows: { term: "Databricks Workflows (job)", group: "Databricks, Spark and Delta",
    short: "Databricks' scheduler. A job is a set of tasks with dependencies, run on a schedule." },
  "for-each": { term: "For-each task", group: "Databricks, Spark and Delta",
    short: "A job task that runs the same notebook once per item in a list. Here: once per source system." },
  bundle: { term: "Asset Bundle", group: "Databricks, Spark and Delta",
    short: "A YAML file (databricks.yml) that describes jobs and code as configuration, so a whole deployment is one command and lives in Git." },
  "secret-scope": { term: "Secret scope", group: "Databricks, Spark and Delta",
    short: "Databricks' vault for passwords and API keys. Code asks for a secret by name; the value never appears in code or logs." },
  "unity-catalog": { term: "Unity Catalog", group: "Databricks, Spark and Delta",
    short: "Databricks' central catalogue of tables, with access control and automatic lineage tracking." },
  spark: { term: "Apache Spark", group: "Databricks, Spark and Delta",
    short: "An engine that processes large datasets by splitting work across many machines. Databricks runs on it." },
  streaming: { term: "Structured Streaming", group: "Databricks, Spark and Delta",
    short: "Spark's way of processing data that keeps arriving, picking up only what is new since the last run." },
  "available-now": { term: "availableNow trigger", group: "Databricks, Spark and Delta",
    short: "Run a streaming job as a batch: process everything that has arrived, then stop. Gives streaming's bookkeeping at batch cost." },
  checkpoint: { term: "Checkpoint", group: "Databricks, Spark and Delta",
    short: "Where a streaming job records how far it has read, so the next run continues from there." },
  delta: { term: "Delta Lake", group: "Databricks, Spark and Delta",
    short: "A table format for files in cloud storage that adds database features: safe concurrent writes, updates, deletes and a full change history." },
  merge: { term: "MERGE (upsert)", group: "Databricks, Spark and Delta",
    short: "One statement that updates rows that already exist and inserts the ones that don't. Used to apply changes to a table." },
  medallion: { term: "Medallion layers (Bronze / Silver / Gold)", group: "Databricks, Spark and Delta",
    short: "Bronze: raw events as received. Silver: cleaned, current state of each record. Gold: business results, here fraud alerts." },
  "soft-delete": { term: "Soft delete", group: "Databricks, Spark and Delta",
    short: "Marking a row as deleted with a flag instead of removing it, so its history and evidence stay available." },
  "ordering-guard": { term: "Ordering guard", group: "Databricks, Spark and Delta",
    short: "A rule that only lets a change overwrite a row if it is newer than what is already stored, so late or repeated changes can't undo newer ones." },
  scd2: { term: "SCD2 (slowly changing dimension, type 2)", group: "Databricks, Spark and Delta",
    short: "Keeping every past version of a record with valid-from / valid-to dates, so you can ask what it looked like on any date." },
  mlflow: { term: "MLflow", group: "Databricks, Spark and Delta",
    short: "A tool for tracking, versioning and deploying machine-learning models, built into Databricks." },

  // ---- Data quality and governance ---------------------------------------------------------
  reconciliation: { term: "Reconciliation", group: "Data quality and governance",
    short: "Comparing numbers between the source and the copy (row counts, sum of balances) to prove nothing was lost or invented." },
  lineage: { term: "Lineage / traceability", group: "Data quality and governance",
    short: "Being able to follow any result back through every step to the original source record that produced it." },
  audit: { term: "Audit trail", group: "Data quality and governance",
    short: "A permanent record of what was done, when, and where it went. Here: where each event landed in Kafka and which source position it came from." },
  "control-table": { term: "Control table", group: "Data quality and governance",
    short: "A small table the pipeline uses to run itself, for example storing each source's watermark and last run status." },
  "data-contract": { term: "Data contract", group: "Data quality and governance",
    short: "The agreed format and meaning of the data a producer publishes, so consumers can rely on it." },
  bcbs239: { term: "BCBS 239", group: "Data quality and governance",
    short: "Basel Committee principles telling banks their risk data must be accurate, complete, timely and traceable to its source." },
  "synthetic-data": { term: "Synthetic data", group: "Data quality and governance",
    short: "Realistic but invented data. No real customer appears anywhere in this project." },
  "ground-truth": { term: "Ground truth", group: "Data quality and governance",
    short: "The known right answers. The simulator records which transactions it made fraudulent, so detection can be scored." },

  // ---- Banking and fraud -----------------------------------------------------------------
  "t-plus-1": { term: "T+1", group: "Banking and fraud",
    short: "\"Trade date plus one day\": results available the next day. Here, fraud reports that run overnight." },
  aml: { term: "AML (anti-money-laundering)", group: "Banking and fraud",
    short: "The checks banks must run to detect money from crime being moved through accounts." },
  structuring: { term: "Structuring", group: "Banking and fraud",
    short: "Splitting a large amount into several transfers just under a reporting threshold to avoid triggering checks. A classic AML red flag." },
  velocity: { term: "Card velocity rule", group: "Banking and fraud",
    short: "Flags many card payments on one account in a short time, a typical sign of a stolen or cloned card." },
  "impossible-travel": { term: "Impossible travel rule", group: "Banking and fraud",
    short: "Flags two card payments in different countries closer together in time than anyone could travel between them." },
  iban: { term: "IBAN", group: "Banking and fraud",
    short: "International bank account number. Its two check digits catch typing errors; the pipeline validates them." },
  sepa: { term: "SEPA transfer", group: "Banking and fraud",
    short: "A euro bank transfer within the Single Euro Payments Area." },
  mainframe: { term: "Mainframe / DB2", group: "Banking and fraud",
    short: "The large central computers many banks still run their core systems on. DB2 is IBM's database on them." },
  mips: { term: "MIPS cost", group: "Banking and fraud",
    short: "Mainframe processing is billed by usage, so every extra query against the mainframe costs money." },

  // ---- Measuring results -----------------------------------------------------------------
  latency: { term: "Latency", group: "Measuring results",
    short: "Delay. Here: time from a change being saved in the source system to the fraud alert being written." },
  percentile: { term: "p50 / p95", group: "Measuring results",
    short: "p50 (median): half the cases were faster than this. p95: 95% were faster; it shows the slow tail." },
  precision: { term: "Precision", group: "Measuring results",
    short: "Of the alerts raised, the share that were real fraud. Low precision means analysts waste time on false alarms." },
  recall: { term: "Recall", group: "Measuring results",
    short: "Of the real fraud cases, the share that raised an alert. Low recall means fraud slips through." },
  "false-positive": { term: "False positive", group: "Measuring results",
    short: "An alert on normal, legitimate activity." },

  // ---- Infrastructure and engineering ------------------------------------------------------
  "sql-server": { term: "SQL Server", group: "Infrastructure and engineering",
    short: "Microsoft's relational database. Plays the bank's cards platform here; Azure SQL is the same engine as a cloud service." },
  "azure-files": { term: "Azure File Share", group: "Infrastructure and engineering",
    short: "A network folder in Microsoft's cloud, accessed like a shared drive (SMB). Partners drop files there." },
  "auto-loader": { term: "Auto Loader", group: "Infrastructure and engineering",
    short: "Databricks' tool for picking up new files automatically. It reads cloud object storage (ADLS, Blob), not Azure File Shares." },
  singlestore: { term: "SingleStore", group: "Infrastructure and engineering",
    short: "A fast distributed SQL database, used for real-time workloads such as instant payments." },
  docker: { term: "Docker / docker compose", group: "Infrastructure and engineering",
    short: "Runs software in isolated containers. docker compose starts several together; here, SQL Server and Kafka on a laptop." },
  kraft: { term: "KRaft", group: "Infrastructure and engineering",
    short: "Kafka's built-in coordination mode, which removes the old need for a separate ZooKeeper service." },
  ci: { term: "CI (continuous integration)", group: "Infrastructure and engineering",
    short: "Automatic checks (tests, linting) that run on every code change before it is merged. Here, GitHub Actions." },
  pytest: { term: "pytest / ruff", group: "Infrastructure and engineering",
    short: "pytest runs the automated tests; ruff checks code style and common mistakes." },
  adr: { term: "ADR (architecture decision record)", group: "Infrastructure and engineering",
    short: "A short note recording one design decision: the context, the choice, and its cost." },
  "rest-api": { term: "REST API", group: "Infrastructure and engineering",
    short: "A web interface where programs read and change data with plain HTTP requests (GET to read, PATCH to change)." },
  fastapi: { term: "FastAPI", group: "Infrastructure and engineering",
    short: "A Python framework for building web APIs. It checks request data automatically and publishes interactive documentation at /docs." },
  microservice: { term: "Microservice", group: "Infrastructure and engineering",
    short: "A small, independent application with one job, here a service that reads fraud events from Kafka and serves them through an API." },
  yaml: { term: "Config-driven (YAML)", group: "Infrastructure and engineering",
    short: "Behaviour set in a configuration file (conf/sources.yml) rather than in code, so adding a table means editing config, not writing code." },
};

// ---- Popovers ----------------------------------------------------------------------------
(function () {
  const pop = document.createElement("div");
  pop.className = "term-pop";
  pop.id = "term-pop";
  pop.setAttribute("role", "tooltip");
  pop.hidden = true;
  document.body.append(pop);

  let anchor = null;
  let pinned = false;
  let hideTimer = null;
  const slug = (k) => `term-${k}`;

  function place(el) {
    pop.style.left = "0px";
    pop.style.top = "0px";
    const r = el.getBoundingClientRect();
    const p = pop.getBoundingClientRect();
    const margin = 12;
    let left = Math.min(Math.max(margin, r.left + r.width / 2 - p.width / 2),
      document.documentElement.clientWidth - p.width - margin);
    let top = r.bottom + 8;
    if (top + p.height > window.innerHeight - margin && r.top - p.height - 8 > margin) {
      top = r.top - p.height - 8;
    }
    pop.style.left = `${left + window.scrollX}px`;
    pop.style.top = `${top + window.scrollY}px`;
  }

  function show(el) {
    clearTimeout(hideTimer);
    const g = GLOSSARY[el.dataset.term];
    if (!g) return;
    if (anchor && anchor !== el) anchor.setAttribute("aria-expanded", "false");
    anchor = el;
    el.setAttribute("aria-expanded", "true");
    pop.innerHTML = `<div class="term-pop-title">${g.term}</div><p>${g.short}</p>` +
      (document.body.dataset.page === "glossary" ? "" :
        `<a href="glossary.html#${slug(el.dataset.term)}">Glossary →</a>`);
    pop.hidden = false;
    place(el);
  }

  function hide() {
    pop.hidden = true;
    pinned = false;
    if (anchor) anchor.setAttribute("aria-expanded", "false");
    anchor = null;
  }

  function enhance(root = document) {
  root.querySelectorAll("span[data-term]").forEach((el) => {
    if (!GLOSSARY[el.dataset.term]) {
      console.warn("glossary: unknown term", el.dataset.term);
      return;
    }
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "term";
    btn.dataset.term = el.dataset.term;
    btn.innerHTML = el.innerHTML;
    btn.setAttribute("aria-describedby", "term-pop");
    btn.setAttribute("aria-expanded", "false");
    el.replaceWith(btn);

    btn.addEventListener("mouseenter", () => { if (!pinned) show(btn); });
    btn.addEventListener("mouseleave", () => {
      if (!pinned) hideTimer = setTimeout(hide, 180);
    });
    btn.addEventListener("focus", () => { if (!pinned) show(btn); });
    btn.addEventListener("blur", () => { if (!pinned) hideTimer = setTimeout(hide, 180); });
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      if (pinned && anchor === btn) { hide(); return; }
      show(btn);
      pinned = true;
    });
  });
  }
  window.enhanceTerms = enhance;
  enhance();

  pop.addEventListener("mouseenter", () => clearTimeout(hideTimer));
  pop.addEventListener("mouseleave", () => { if (!pinned) hideTimer = setTimeout(hide, 180); });
  document.addEventListener("click", (e) => { if (!pop.contains(e.target)) hide(); });
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") hide(); });
  window.addEventListener("resize", () => anchor && place(anchor));
})();

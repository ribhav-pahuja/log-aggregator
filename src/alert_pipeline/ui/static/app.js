(() => {
  const $ = (id) => document.getElementById(id);
  const state = { alerts: [], selectedId: null, timer: null, busy: false };

  function qs() {
    const params = new URLSearchParams();
    const status = $("status").value;
    const severity = $("severity").value;
    const service = $("service").value;
    const q = $("q").value.trim();
    if (status) params.set("status", status);
    if (severity) params.set("severity", severity);
    if (service) params.set("service", service);
    if (q) params.set("q", q);
    params.set("limit", "150");
    return params.toString();
  }

  function fmtTime(iso) {
    if (!iso) return "—";
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return iso;
    return d.toLocaleString(undefined, {
      month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit",
    });
  }

  function fmtDuration(sec) {
    if (sec === null || sec === undefined) return "—";
    sec = Number(sec);
    if (Number.isNaN(sec)) return "—";
    if (sec < 60) return `${sec}s`;
    if (sec < 3600) return `${Math.floor(sec / 60)}m ${sec % 60}s`;
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    return `${h}h ${m}m`;
  }

  function relative(iso) {
    if (!iso) return "";
    const t = new Date(iso).getTime();
    const s = Math.max(0, Math.round((Date.now() - t) / 1000));
    if (s < 60) return `${s}s ago`;
    if (s < 3600) return `${Math.floor(s / 60)}m ago`;
    if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
    return `${Math.floor(s / 86400)}d ago`;
  }

  function escapeHtml(s) {
    return String(s ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  async function fetchJSON(url, opts) {
    const res = await fetch(url, opts);
    if (!res.ok) throw new Error(`${res.status} ${await res.text()}`);
    return res.json();
  }

  async function loadStats() {
    const s = await fetchJSON("/api/stats");
    document.querySelectorAll("#stats [data-k]").forEach((el) => {
      const k = el.getAttribute("data-k");
      el.textContent = s[k] ?? "0";
    });
  }

  async function loadServices() {
    const services = await fetchJSON("/api/services");
    const sel = $("service");
    const cur = sel.value;
    sel.innerHTML = '<option value="">All services</option>';
    for (const svc of services) {
      const opt = document.createElement("option");
      opt.value = svc;
      opt.textContent = svc;
      sel.appendChild(opt);
    }
    if (cur) sel.value = cur;
  }

  /** Full operator action set — Acknowledge is always explicit, never hidden behind "only resolve". */
  function actionButtons(a, { size = "md" } = {}) {
    const st = a.status;
    const cls = size === "lg" ? "btn btn-lg" : "btn";
    const parts = [];

    // Acknowledge: available whenever not already acked or resolved
    if (st === "open" || st === "updated") {
      parts.push(
        `<button type="button" class="${cls} btn-ack" data-act="ack" data-id="${escapeHtml(a.id)}" title="Acknowledge this alert (you are working on it)">` +
          `Acknowledge</button>`
      );
    }

    // Resolve: available for open, updated, and acknowledged
    if (st === "open" || st === "updated" || st === "acknowledged") {
      parts.push(
        `<button type="button" class="${cls} btn-resolve" data-act="resolve" data-id="${escapeHtml(a.id)}" title="Resolve / close this alert">` +
          `Resolve</button>`
      );
    }

    // Reopen resolved (or un-ack by going back to open)
    if (st === "resolved") {
      parts.push(
        `<button type="button" class="${cls}" data-act="reopen" data-id="${escapeHtml(a.id)}">Reopen</button>`
      );
    }
    if (st === "acknowledged") {
      parts.push(
        `<button type="button" class="${cls} btn-ghost" data-act="reopen" data-id="${escapeHtml(a.id)}" title="Clear acknowledgement">Un-ack (reopen)</button>`
      );
    }

    return parts.join("");
  }

  function bindActions(root) {
    root.querySelectorAll("[data-act]").forEach((btn) => {
      btn.addEventListener("click", (ev) => {
        ev.preventDefault();
        ev.stopPropagation();
        const id = btn.getAttribute("data-id");
        const act = btn.getAttribute("data-act");
        if (id && act) runAction(id, act, btn);
      });
    });
  }

  async function runAction(id, act, btn) {
    if (state.busy) return;
    state.busy = true;
    if (btn) {
      btn.disabled = true;
      btn.classList.add("busy");
    }
    const paths = { ack: "ack", resolve: "resolve", reopen: "reopen" };
    const path = paths[act];
    const labels = { ack: "Acknowledged", resolve: "Resolved", reopen: "Reopened" };
    try {
      const updated = await fetchJSON(`/api/alerts/${id}/${path}`, { method: "POST" });
      showToast(`${labels[act] || act}: ${updated.title.slice(0, 60)}`);
      state.selectedId = id;
      await refresh({ keepSelection: true });
      await selectAlert(id);
    } catch (err) {
      showToast(`Failed: ${err.message}`, true);
    } finally {
      state.busy = false;
    }
  }

  function showToast(msg, isError) {
    let el = $("toast");
    if (!el) {
      el = document.createElement("div");
      el.id = "toast";
      el.className = "toast";
      document.body.appendChild(el);
    }
    el.textContent = msg;
    el.classList.toggle("error", !!isError);
    el.classList.add("show");
    clearTimeout(el._t);
    el._t = setTimeout(() => el.classList.remove("show"), 2800);
  }

  function renderList() {
    const root = $("alertList");
    const empty = $("emptyState");
    $("listCount").textContent = String(state.alerts.length);
    root.innerHTML = "";
    if (!state.alerts.length) {
      empty.classList.remove("hidden");
      empty.innerHTML = "No alerts yet — use <strong>Fire alert</strong> above to create one.";
      return;
    }
    empty.classList.add("hidden");
    for (const a of state.alerts) {
      const el = document.createElement("article");
      el.className = "alert-card" + (a.id === state.selectedId ? " active" : "");
      el.dataset.id = a.id;
      el.innerHTML = `
        <div class="sev-dot ${escapeHtml(a.severity)}" title="${escapeHtml(a.severity)}"></div>
        <div class="card-main">
          <p class="card-title">${escapeHtml(a.title)}</p>
          <div class="card-meta">
            <span class="pill ${escapeHtml(a.status)}">${escapeHtml(a.status)}</span>
            <span>${escapeHtml(a.service)}</span>
            <span>${escapeHtml(a.severity)}</span>
            ${a.error_code ? `<span>${escapeHtml(a.error_code)}</span>` : ""}
            ${a.tta_seconds != null ? `<span title="TTA">TTA ${fmtDuration(a.tta_seconds)}</span>` : ""}
            ${a.ttr_seconds != null ? `<span title="TTR">TTR ${fmtDuration(a.ttr_seconds)}</span>` : ""}
          </div>
          <div class="card-actions row-actions">${actionButtons(a)}</div>
        </div>
        <div class="card-side">
          <span class="count">×${a.occurrence_count}</span>
          <span>${relative(a.last_seen)}</span>
        </div>`;
      el.addEventListener("click", (ev) => {
        if (ev.target.closest("[data-act]")) return;
        selectAlert(a.id);
      });
      root.appendChild(el);
    }
    bindActions(root);
  }

  async function selectAlert(id) {
    state.selectedId = id;
    renderList();
    $("detailPlaceholder").classList.add("hidden");
    const detail = $("detail");
    detail.classList.remove("hidden");
    detail.innerHTML = `<p class="muted">Loading…</p>`;
    try {
      const [a, dispatches] = await Promise.all([
        fetchJSON(`/api/alerts/${id}`),
        fetchJSON(`/api/alerts/${id}/dispatches`),
      ]);
      const labels = Object.entries(a.labels || {})
        .map(([k, v]) => `<span class="pill">${escapeHtml(k)}=${escapeHtml(v)}</span>`)
        .join(" ") || "—";
      const rows = dispatches.length
        ? dispatches
            .map(
              (d) => `<tr>
              <td>${escapeHtml(d.channel)}</td>
              <td class="${d.success ? "ok-text" : "fail-text"}">${d.success ? "ok" : "fail"}</td>
              <td>${d.status_code ?? "—"}</td>
              <td>${fmtTime(d.created_at)}</td>
              <td>${escapeHtml(d.error_message || "")}</td>
            </tr>`
            )
            .join("")
        : `<tr><td colspan="5" class="muted">No dispatches recorded</td></tr>`;

      const canAck = a.status === "open" || a.status === "updated";
      const canResolve = a.status === "open" || a.status === "updated" || a.status === "acknowledged";

      detail.innerHTML = `
        <div class="ops-bar ${canAck || canResolve ? "" : "ops-bar-muted"}">
          <div class="ops-bar-label">Operator actions</div>
          <div class="ops-bar-buttons detail-actions">
            ${actionButtons(a, { size: "lg" })}
          </div>
          <p class="ops-help">
            <span><kbd>Acknowledge</kbd> — you are handling it; alert stays active and counts can still rise.</span>
            <span><kbd>Resolve</kbd> — close the incident; a new matching error creates a new alert.</span>
          </p>
        </div>
        <h3>${escapeHtml(a.title)}</h3>
        <div class="card-meta" style="margin-bottom:0.75rem">
          <span class="pill ${escapeHtml(a.status)}">${escapeHtml(a.status)}</span>
          <span class="pill">${escapeHtml(a.severity)}</span>
          <span>×${a.occurrence_count} occurrences</span>
        </div>
        <dl class="kv">
          <dt>Service</dt><dd>${escapeHtml(a.service)}</dd>
          <dt>Host</dt><dd>${escapeHtml(a.host)}</dd>
          <dt>Fingerprint</dt><dd>${escapeHtml(a.fingerprint)}</dd>
          <dt>Error code</dt><dd>${escapeHtml(a.error_code || "—")}</dd>
          <dt>Trace ID</dt><dd>${escapeHtml(a.trace_id || "—")}</dd>
          <dt>First seen</dt><dd>${fmtTime(a.first_seen)}</dd>
          <dt>Last seen</dt><dd>${fmtTime(a.last_seen)} <span class="muted">(${relative(a.last_seen)})</span></dd>
          <dt>TTA</dt><dd title="Time to acknowledge">${fmtDuration(a.tta_seconds)}${a.acknowledged_at ? ` <span class="muted">(${fmtTime(a.acknowledged_at)})</span>` : ""}</dd>
          <dt>TTR</dt><dd title="Time to resolve">${fmtDuration(a.ttr_seconds)}${a.resolved_at ? ` <span class="muted">(${fmtTime(a.resolved_at)})</span>` : ""}</dd>
          <dt>Dispatch</dt><dd><span class="ok-text">${a.dispatch_success} ok</span> · <span class="fail-text">${a.dispatch_failed} failed</span></dd>
          <dt>Labels</dt><dd style="font-family:inherit">${labels}</dd>
        </dl>
        <p class="section-title">Sample message</p>
        <div class="msg-box">${escapeHtml(a.sample_message || a.description || "—")}</div>
        <p class="section-title">Dispatch history</p>
        <table class="dispatch-table">
          <thead><tr><th>Channel</th><th>Result</th><th>HTTP</th><th>When</th><th>Error</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`;
      bindActions(detail);
    } catch (err) {
      detail.innerHTML = `<p class="fail-text">${escapeHtml(err.message)}</p>`;
    }
  }

  async function loadAlerts() {
    state.alerts = await fetchJSON(`/api/alerts?${qs()}`);
    renderList();
  }

  async function refresh({ keepSelection = false } = {}) {
    try {
      await Promise.all([loadStats(), loadAlerts()]);
      $("lastRefresh").textContent = `Updated ${new Date().toLocaleTimeString()}`;
      // Auto-select first alert so Acknowledge is always one click away in the detail pane
      if (state.alerts.length) {
        const stillThere =
          state.selectedId && state.alerts.some((a) => a.id === state.selectedId);
        if (!stillThere || !keepSelection) {
          if (!state.selectedId || !stillThere) {
            await selectAlert(state.alerts[0].id);
          }
        } else if (keepSelection && state.selectedId) {
          await selectAlert(state.selectedId);
        }
      }
    } catch (err) {
      $("lastRefresh").textContent = `Error: ${err.message}`;
      console.error(err);
    }
  }

  function schedule() {
    if (state.timer) clearInterval(state.timer);
    if ($("autoRefresh").checked) {
      state.timer = setInterval(() => refresh({ keepSelection: true }), 5000);
    }
  }

  ["status", "severity", "service"].forEach((id) =>
    $(id).addEventListener("change", () => {
      state.selectedId = null;
      refresh();
    })
  );
  let searchTimer;
  $("q").addEventListener("input", () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      state.selectedId = null;
      refresh();
    }, 250);
  });
  $("btnRefresh").addEventListener("click", () => refresh({ keepSelection: true }));
  $("autoRefresh").addEventListener("change", schedule);

  async function demoReset() {
    if (!confirm("Delete ALL alerts and dispatch history? This cannot be undone.")) return;
    const statusEl = $("demoStatus");
    if (statusEl) statusEl.textContent = "Clearing…";
    const r = await fetchJSON("/api/demo/reset", { method: "POST" });
    state.selectedId = null;
    $("detail").classList.add("hidden");
    $("detailPlaceholder").classList.remove("hidden");
    if (statusEl) {
      statusEl.textContent = `Cleared ${r.alerts_deleted} alerts, ${r.dispatch_log_deleted} dispatch rows.`;
    }
    await refresh();
  }

  function readDemoForm(overrides = {}) {
    const service = overrides.service || ($("demo_service") && $("demo_service").value) || "payments-api";
    const message = overrides.message || ($("demo_message") && $("demo_message").value) || "demo failure";
    const severity = overrides.severity || ($("demo_severity") && $("demo_severity").value) || "ERROR";
    const errorCodeRaw = overrides.error_code !== undefined
      ? overrides.error_code
      : ($("demo_error_code") && $("demo_error_code").value);
    const count = overrides.count || Number(($("demo_count") && $("demo_count").value) || 1);
    const alsoKafka = !!( $("demo_kafka") && $("demo_kafka").checked );
    return {
      service: String(service).trim(),
      message: String(message).trim(),
      severity: String(severity).trim(),
      error_code: errorCodeRaw ? String(errorCodeRaw).trim() : null,
      count: Math.min(20, Math.max(1, count)),
      host: "ui-demo",
      also_publish_kafka: alsoKafka,
    };
  }

  async function demoFire(overrides = {}) {
    const statusEl = $("demoStatus");
    const body = readDemoForm(overrides);
    if (statusEl) statusEl.textContent = "Firing…";
    const r = await fetchJSON("/api/demo/fire", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (statusEl) {
      statusEl.textContent = `OK — ${r.events_sent} event(s), alert ${r.alert_id || "?"}. ${r.note || ""}`;
    }
    // Show all statuses so the new row is never filtered away
    if ($("status")) $("status").value = "";
    state.selectedId = r.alert_id || (r.alerts && r.alerts[0] && r.alerts[0].id) || null;
    await refresh({ keepSelection: !!state.selectedId });
    if (state.selectedId) {
      try { await selectAlert(state.selectedId); } catch (_) { /* ignore */ }
    }
  }

  const form = $("demoForm");
  if (form) {
    form.addEventListener("submit", (ev) => {
      ev.preventDefault();
      ev.stopPropagation();
      demoFire().catch((e) => {
        const statusEl = $("demoStatus");
        if (statusEl) statusEl.textContent = `Error: ${e.message}`;
        console.error(e);
      });
    });
  }
  const burst = $("btnFireBurst");
  if (burst) {
    burst.addEventListener("click", (ev) => {
      ev.preventDefault();
      demoFire({ count: 5 }).catch((e) => {
        const statusEl = $("demoStatus");
        if (statusEl) statusEl.textContent = `Error: ${e.message}`;
        console.error(e);
      });
    });
  }
  const resetBtn = $("btnReset");
  if (resetBtn) {
    resetBtn.addEventListener("click", (ev) => {
      ev.preventDefault();
      demoReset().catch((e) => {
        const statusEl = $("demoStatus");
        if (statusEl) statusEl.textContent = `Error: ${e.message}`;
        console.error(e);
      });
    });
  }

  const boot = async () => {
    try {
      await loadServices();
    } catch (e) {
      console.error(e);
    }
    if (new URLSearchParams(location.search).has("fresh")) {
      try { await fetchJSON("/api/demo/reset", { method: "POST" }); } catch (_) { /* ignore */ }
    }
    await refresh();
  };
  boot();
  schedule();
})();

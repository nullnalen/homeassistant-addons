/* Roblox Foreldrekontroll — frontend */
(function () {
  "use strict";

  let state = null;
  let currentTab = "all";
  let refreshTimer = null;

  // Finn base-path for ingress-støtte (HA legger til prefix i URL)
  const BASE = (() => {
    const p = window.location.pathname;
    // Hvis vi er på /api/hassio_ingress/<token>/... fjern filnavn
    const m = p.match(/^(\/[^/]+\/[^/]+\/[^/]+)/);
    return m ? m[1] : "";
  })();

  function apiUrl(path) {
    return BASE + path;
  }

  async function fetchState() {
    try {
      const r = await fetch(apiUrl("/api/state"));
      if (!r.ok) throw new Error("HTTP " + r.status);
      return await r.json();
    } catch (e) {
      console.error("fetchState feilet:", e);
      return null;
    }
  }

  async function postAction(path, body) {
    const r = await fetch(apiUrl(path), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!r.ok) throw new Error("HTTP " + r.status);
    return r.json();
  }

  function formatMinutes(mins) {
    if (mins == null) return "—";
    if (mins < 60) return mins + " min";
    const h = Math.floor(mins / 60);
    const m = mins % 60;
    return m > 0 ? `${h}t ${m}m` : `${h}t`;
  }

  function timeAgo(ts) {
    if (!ts) return "";
    const secs = Math.floor(Date.now() / 1000 - ts);
    if (secs < 60) return "akkurat nå";
    if (secs < 3600) return `${Math.floor(secs / 60)} min siden`;
    return `${Math.floor(secs / 3600)}t siden`;
  }

  function showToast(msg, type = "ok") {
    const el = document.getElementById("toast");
    el.textContent = msg;
    el.className = `toast toast-${type}`;
    clearTimeout(el._timer);
    el._timer = setTimeout(() => { el.className = "toast hidden"; }, 3000);
  }

  function renderPresenceBadge(presence) {
    const el = document.getElementById("presence-badge");
    if (presence.in_game) {
      el.textContent = "Spiller nå";
      el.className = "badge badge-ingame";
    } else if (presence.online) {
      el.textContent = "Online";
      el.className = "badge badge-online";
    } else {
      el.textContent = "Offline";
      el.className = "badge badge-offline";
    }
  }

  function renderCurrentGame(currentGame) {
    const section = document.getElementById("current-game-section");
    if (!currentGame) {
      section.classList.add("hidden");
      return;
    }
    section.classList.remove("hidden");
    document.getElementById("current-game-name").textContent = currentGame.name;

    const statusEl = document.getElementById("current-game-status");
    if (currentGame.status === "approved") {
      statusEl.textContent = "✓ Godkjent";
      statusEl.style.color = "var(--green)";
    } else if (currentGame.status === "blocked") {
      statusEl.textContent = "✗ Blokkert";
      statusEl.style.color = "var(--red)";
    } else {
      statusEl.textContent = "? Ikke godkjent";
      statusEl.style.color = "var(--yellow)";
    }

    const actionsEl = document.getElementById("current-game-actions");
    actionsEl.innerHTML = "";
    if (currentGame.status !== "approved") {
      const btn = makeApproveBtn(currentGame.universe_id, currentGame.name);
      actionsEl.appendChild(btn);
    } else {
      const btn = makeUnapproveBtn(currentGame.universe_id);
      actionsEl.appendChild(btn);
    }
  }

  function renderScreentime(s) {
    const todayEl = document.getElementById("today-value");
    const weekEl = document.getElementById("week-value");
    const limitEl = document.getElementById("today-limit");

    todayEl.textContent = formatMinutes(s.screentime_today);
    weekEl.textContent = formatMinutes(s.screentime_week);

    const todayBox = todayEl.closest(".screentime-box");
    if (s.daily_limit && s.screentime_today >= s.daily_limit) {
      todayBox.classList.add("screentime-over");
      limitEl.textContent = `Grense: ${formatMinutes(s.daily_limit)} ⚠️`;
    } else {
      todayBox.classList.remove("screentime-over");
      limitEl.textContent = s.daily_limit ? `Grense: ${formatMinutes(s.daily_limit)}` : "";
    }

    renderWeekChart(s.daily_data, s.daily_limit);
  }

  function renderWeekChart(dailyData, limit) {
    const chart = document.getElementById("week-chart");
    chart.innerHTML = "";
    if (!dailyData || dailyData.length === 0) return;

    const sorted = [...dailyData].sort((a, b) => b.daysAgo - a.daysAgo);
    const maxMins = Math.max(...sorted.map(d => d.minutes), 1);

    const days = ["Man", "Tir", "Ons", "Tor", "Fre", "Lør", "Søn"];
    const today = new Date().getDay(); // 0=Sun

    sorted.forEach(d => {
      const pct = Math.max((d.minutes / maxMins) * 100, d.minutes > 0 ? 5 : 0);
      const isToday = d.daysAgo === 0;
      const overLimit = limit && d.minutes >= limit;

      const wrap = document.createElement("div");
      wrap.className = "week-bar-wrap";

      const bar = document.createElement("div");
      bar.className = "week-bar" + (isToday ? " today" : "") + (overLimit ? " over-limit" : "");
      bar.style.height = pct + "%";
      bar.title = formatMinutes(d.minutes);

      // Label: dagsnavn
      const jsDay = (today - d.daysAgo + 7) % 7;
      const label = document.createElement("div");
      label.className = "week-label";
      label.textContent = isToday ? "I dag" : days[jsDay === 0 ? 6 : jsDay - 1];

      wrap.appendChild(bar);
      wrap.appendChild(label);
      chart.appendChild(wrap);
    });
  }

  function makeApproveBtn(universeId, name) {
    const btn = document.createElement("button");
    btn.className = "btn btn-approve";
    btn.textContent = "Godkjenn";
    btn.onclick = async () => {
      btn.disabled = true;
      try {
        await postAction("/api/games/approve", { universe_id: universeId });
        showToast(`'${name}' godkjent`, "ok");
        await refresh();
      } catch (e) {
        showToast("Feil ved godkjenning", "err");
        btn.disabled = false;
      }
    };
    return btn;
  }

  function makeUnapproveBtn(universeId) {
    const btn = document.createElement("button");
    btn.className = "btn btn-unapprove";
    btn.textContent = "Fjern godkjenning";
    btn.onclick = async () => {
      btn.disabled = true;
      try {
        await postAction("/api/games/unapprove", { universe_id: universeId });
        showToast("Godkjenning fjernet", "ok");
        await refresh();
      } catch (e) {
        showToast("Feil", "err");
        btn.disabled = false;
      }
    };
    return btn;
  }

  function renderGamesList(universes) {
    const list = document.getElementById("games-list");
    const empty = document.getElementById("games-empty");
    list.innerHTML = "";

    const filtered = universes.filter(g => {
      if (currentTab === "all") return true;
      return g.status === currentTab;
    });

    if (filtered.length === 0) {
      empty.classList.remove("hidden");
      return;
    }
    empty.classList.add("hidden");

    filtered.forEach(g => {
      const row = document.createElement("div");
      row.className = "game-row";

      const dot = document.createElement("div");
      dot.className = `game-dot dot-${g.status}`;

      const info = document.createElement("div");
      info.className = "game-row-info";

      const name = document.createElement("div");
      name.className = "game-row-name";
      name.textContent = g.name;

      const meta = document.createElement("div");
      meta.className = "game-row-meta";
      const statusLabel = g.status === "approved" ? "Godkjent" : g.status === "blocked" ? "Blokkert" : "Ukjent";
      meta.textContent = `${formatMinutes(g.minutes)} denne uken  •  ${statusLabel}`;

      info.appendChild(name);
      info.appendChild(meta);

      const actions = document.createElement("div");
      actions.className = "game-row-actions";

      if (g.status !== "approved") {
        actions.appendChild(makeApproveBtn(g.universe_id, g.name));
      } else {
        actions.appendChild(makeUnapproveBtn(g.universe_id));
      }

      // Blokker-knapp er placeholder til skrive-endepunkt er fanget
      const blockBtn = document.createElement("button");
      blockBtn.className = "btn btn-block-placeholder";
      blockBtn.textContent = "Blokker";
      blockBtn.title = "Blokkering via API ikke tilgjengelig ennå — bruk Roblox-appen";
      blockBtn.disabled = true;
      actions.appendChild(blockBtn);

      row.appendChild(dot);
      row.appendChild(info);
      row.appendChild(actions);
      list.appendChild(row);
    });
  }

  function render(s) {
    if (!s) return;

    const authBanner = document.getElementById("auth-error-banner");
    if (s.auth_error) {
      authBanner.classList.remove("hidden");
    } else {
      authBanner.classList.add("hidden");
    }

    renderPresenceBadge(s.presence);
    renderCurrentGame(s.current_game);
    renderScreentime(s);
    renderGamesList(s.top_universes || []);

    const lastEl = document.getElementById("last-updated");
    lastEl.textContent = s.last_slow_update ? `Oppdatert ${timeAgo(s.last_slow_update)}` : "";
  }

  async function refresh() {
    const s = await fetchState();
    if (s) {
      state = s;
      render(state);
    }
  }

  function setupTabs() {
    document.querySelectorAll(".tab").forEach(btn => {
      btn.addEventListener("click", () => {
        document.querySelectorAll(".tab").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        currentTab = btn.dataset.tab;
        if (state) renderGamesList(state.top_universes || []);
      });
    });
  }

  async function init() {
    setupTabs();
    await refresh();
    // Oppdater hvert 30. sekund
    refreshTimer = setInterval(refresh, 30_000);
  }

  document.addEventListener("DOMContentLoaded", init);
})();

/* Roblox Foreldrekontroll — frontend */
(function () {
  "use strict";

  let state = null;
  let activeChildIndex = 0;
  let currentTab = "all";

  const BASE = (() => {
    const p = window.location.pathname;
    const m = p.match(/^(\/[^/]+\/[^/]+\/[^/]+)/);
    return m ? m[1] : "";
  })();

  function apiUrl(path) { return BASE + path; }

  async function fetchJson(path) {
    const r = await fetch(apiUrl(path));
    if (!r.ok) throw new Error("HTTP " + r.status);
    return r.json();
  }

  async function postJson(path, body) {
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
    const s = Math.floor(Date.now() / 1000 - ts);
    if (s < 60) return "akkurat nå";
    if (s < 3600) return `${Math.floor(s / 60)} min siden`;
    return `${Math.floor(s / 3600)}t siden`;
  }

  function showToast(msg, type = "ok") {
    const el = document.getElementById("toast");
    el.textContent = msg;
    el.className = `toast toast-${type}`;
    clearTimeout(el._t);
    el._t = setTimeout(() => { el.className = "toast hidden"; }, 3000);
  }

  // ─── SETUP ───────────────────────────────────────────────────────────────

  function showSetup() {
    document.getElementById("setup-view").classList.remove("hidden");
    document.getElementById("main-view").classList.add("hidden");
  }

  function showMain() {
    document.getElementById("setup-view").classList.add("hidden");
    document.getElementById("main-view").classList.remove("hidden");
  }

  async function initSetup() {
    const form = document.getElementById("setup-form");
    const fetchBtn = document.getElementById("setup-fetch-btn");
    const saveBtn = document.getElementById("setup-save-btn");
    const cookieInput = document.getElementById("setup-cookie");
    const childrenDiv = document.getElementById("setup-children");
    const errorDiv = document.getElementById("setup-error");

    fetchBtn.addEventListener("click", async () => {
      const cookie = cookieInput.value.trim();
      if (!cookie) { showSetupError("Lim inn cookie først"); return; }
      fetchBtn.disabled = true;
      fetchBtn.textContent = "Henter…";
      errorDiv.classList.add("hidden");
      try {
        const data = await postJson("/api/setup/fetch-children", { cookie });
        renderChildrenCheckboxes(data.children, childrenDiv);
        saveBtn.classList.remove("hidden");
      } catch (e) {
        showSetupError(e.message.includes("401") ? "Cookie er ugyldig eller utløpt" : "Tilkoblingsfeil — prøv igjen");
      } finally {
        fetchBtn.disabled = false;
        fetchBtn.textContent = "Hent barn";
      }
    });

    saveBtn.addEventListener("click", async () => {
      const cookie = cookieInput.value.trim();
      const selected = [...childrenDiv.querySelectorAll("input[type=checkbox]:checked")].map(cb => parseInt(cb.value));
      if (!selected.length) { showSetupError("Velg minst ett barn"); return; }
      saveBtn.disabled = true;
      saveBtn.textContent = "Lagrer…";
      try {
        await postJson("/api/setup/save", { cookie, child_ids: selected });
        showMain();
        await refresh();
      } catch (e) {
        showSetupError("Lagring feilet — prøv igjen");
        saveBtn.disabled = false;
        saveBtn.textContent = "Lagre og start";
      }
    });
  }

  function renderChildrenCheckboxes(children, container) {
    container.innerHTML = "<p class='setup-label'>Velg barn å overvåke:</p>";
    children.forEach(c => {
      const label = document.createElement("label");
      label.className = "child-checkbox";
      label.innerHTML = `<input type="checkbox" value="${c.id}" checked> ${c.name}`;
      container.appendChild(label);
    });
    container.classList.remove("hidden");
  }

  function showSetupError(msg) {
    const el = document.getElementById("setup-error");
    el.textContent = msg;
    el.classList.remove("hidden");
  }

  // ─── DASHBOARD ───────────────────────────────────────────────────────────

  function renderChildTabs(children) {
    const bar = document.getElementById("child-tabs");
    if (children.length <= 1) {
      bar.classList.add("hidden");
      return;
    }
    bar.classList.remove("hidden");
    bar.innerHTML = "";
    children.forEach((c, i) => {
      const btn = document.createElement("button");
      btn.className = "tab" + (i === activeChildIndex ? " active" : "");
      btn.textContent = c.display_name;
      btn.addEventListener("click", () => {
        activeChildIndex = i;
        renderDashboard(state);
      });
      bar.appendChild(btn);
    });
  }

  function renderDashboard(s) {
    const children = s.children || [];
    if (!children.length) return;

    if (activeChildIndex >= children.length) activeChildIndex = 0;
    const child = children[activeChildIndex];

    renderChildTabs(children);

    // Auth-banner
    const authBanner = document.getElementById("auth-error-banner");
    s.auth_error ? authBanner.classList.remove("hidden") : authBanner.classList.add("hidden");

    // Presence badge
    const badge = document.getElementById("presence-badge");
    const p = child.presence || {};
    if (p.in_game) {
      badge.textContent = "Spiller nå";
      badge.className = "badge badge-ingame";
    } else if (p.online) {
      badge.textContent = "Online";
      badge.className = "badge badge-online";
    } else {
      badge.textContent = "Offline";
      badge.className = "badge badge-offline";
    }

    // Aktiv økt
    const sec = document.getElementById("current-game-section");
    if (child.current_game) {
      sec.classList.remove("hidden");
      document.getElementById("current-game-name").textContent = child.current_game.name;
      const statusEl = document.getElementById("current-game-status");
      const actionsEl = document.getElementById("current-game-actions");
      actionsEl.innerHTML = "";
      if (child.current_game.status === "approved") {
        statusEl.textContent = "✓ Godkjent";
        statusEl.style.color = "var(--green)";
        actionsEl.appendChild(makeUnapproveBtn(child.current_game.universe_id));
      } else if (child.current_game.status === "blocked") {
        statusEl.textContent = "✗ Blokkert";
        statusEl.style.color = "var(--red)";
      } else {
        statusEl.textContent = "? Ikke godkjent";
        statusEl.style.color = "var(--yellow)";
        actionsEl.appendChild(makeApproveBtn(child.current_game.universe_id, child.current_game.name));
      }
    } else {
      sec.classList.add("hidden");
    }

    // Skjermtid
    const todayEl = document.getElementById("today-value");
    const weekEl = document.getElementById("week-value");
    const limitEl = document.getElementById("today-limit");
    todayEl.textContent = formatMinutes(child.screentime_today);
    weekEl.textContent = formatMinutes(child.screentime_week);
    const todayBox = todayEl.closest(".screentime-box");
    if (child.daily_limit && child.screentime_today >= child.daily_limit) {
      todayBox.classList.add("screentime-over");
      limitEl.textContent = `Grense: ${formatMinutes(child.daily_limit)} ⚠️`;
    } else {
      todayBox.classList.remove("screentime-over");
      limitEl.textContent = child.daily_limit ? `Grense: ${formatMinutes(child.daily_limit)}` : "";
    }
    renderWeekChart(child.daily_data, child.daily_limit);

    // Spilliste
    renderGamesList(child.top_universes || [], child.child_id);

    // Venner
    renderFriends(child.friends || []);

    // Sist oppdatert
    document.getElementById("last-updated").textContent =
      s.last_slow_update ? `Oppdatert ${timeAgo(s.last_slow_update)}` : "";
  }

  function renderWeekChart(dailyData, limit) {
    const chart = document.getElementById("week-chart");
    chart.innerHTML = "";
    if (!dailyData || !dailyData.length) return;

    const sorted = [...dailyData].sort((a, b) => b.daysAgo - a.daysAgo);
    const maxMins = Math.max(...sorted.map(d => d.minutes), 1);
    const days = ["Man", "Tir", "Ons", "Tor", "Fre", "Lør", "Søn"];
    const today = new Date().getDay();

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

      const jsDay = (today - d.daysAgo + 7) % 7;
      const label = document.createElement("div");
      label.className = "week-label";
      label.textContent = isToday ? "I dag" : days[jsDay === 0 ? 6 : jsDay - 1];

      wrap.appendChild(bar);
      wrap.appendChild(label);
      chart.appendChild(wrap);
    });
  }

  function makeApproveBtn(universeId, name, childId) {
    const btn = document.createElement("button");
    btn.className = "btn btn-approve";
    btn.textContent = "Godkjenn";
    btn.onclick = async () => {
      btn.disabled = true;
      try {
        // Avblokker i Roblox-API hvis spillet er blokkert
        const child = state.children?.[activeChildIndex];
        const game = child?.top_universes?.find(g => g.universe_id === universeId);
        if (game?.blocked && childId) {
          await postJson("/api/games/unblock", { universe_id: universeId, child_id: childId });
        }
        await postJson("/api/games/approve", { universe_id: universeId });
        showToast(`'${name || universeId}' godkjent`, "ok");
        await refresh();
      } catch (e) {
        showToast("Feil ved godkjenning: " + e.message, "err");
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
        await postJson("/api/games/unapprove", { universe_id: universeId });
        showToast("Godkjenning fjernet", "ok");
        await refresh();
      } catch {
        showToast("Feil", "err");
        btn.disabled = false;
      }
    };
    return btn;
  }

  function makeBlockBtn(universeId, name, childId) {
    const btn = document.createElement("button");
    btn.className = "btn btn-block";
    btn.textContent = "Blokker";
    btn.onclick = async () => {
      btn.disabled = true;
      btn.textContent = "Blokkerer…";
      try {
        await postJson("/api/games/block", { universe_id: universeId, child_id: childId });
        await postJson("/api/games/unapprove", { universe_id: universeId });
        showToast(`'${name || universeId}' blokkert`, "ok");
        await refresh();
      } catch (e) {
        showToast("Feil ved blokkering: " + e.message, "err");
        btn.disabled = false;
        btn.textContent = "Blokker";
      }
    };
    return btn;
  }

  function makeUnblockBtn(universeId, name, childId) {
    const btn = document.createElement("button");
    btn.className = "btn btn-unblock";
    btn.textContent = "Avblokker";
    btn.onclick = async () => {
      btn.disabled = true;
      btn.textContent = "Avblokkerer…";
      try {
        await postJson("/api/games/unblock", { universe_id: universeId, child_id: childId });
        showToast(`'${name || universeId}' avblokkert`, "ok");
        await refresh();
      } catch (e) {
        showToast("Feil ved avblokkering: " + e.message, "err");
        btn.disabled = false;
        btn.textContent = "Avblokker";
      }
    };
    return btn;
  }

  function renderGamesList(universes, childId) {
    const list = document.getElementById("games-list");
    const empty = document.getElementById("games-empty");
    list.innerHTML = "";

    const filtered = universes.filter(g =>
      currentTab === "all" || g.status === currentTab
    );

    if (!filtered.length) {
      empty.classList.remove("hidden");
      return;
    }
    empty.classList.add("hidden");

    const statusLabel = { approved: "Godkjent", blocked: "Blokkert", unknown: "Ukjent" };

    filtered.forEach(g => {
      const wrap = document.createElement("div");
      wrap.className = "game-wrap";

      // ── Sammendragsrad (alltid synlig) ──
      const row = document.createElement("div");
      row.className = "game-row game-row-clickable";
      row.setAttribute("aria-expanded", "false");

      const dot = document.createElement("div");
      dot.className = `game-dot dot-${g.status}`;

      const info = document.createElement("div");
      info.className = "game-row-info";
      info.innerHTML = `
        <div class="game-row-name">${g.name}</div>
        <div class="game-row-meta">${formatMinutes(g.minutes)} denne uken  •  ${statusLabel[g.status] || g.status}${g.playing ? `  •  ${g.playing.toLocaleString("no")} spiller nå` : ""}</div>
      `;

      const chevron = document.createElement("div");
      chevron.className = "game-row-chevron";
      chevron.textContent = "▸";

      row.appendChild(dot);
      row.appendChild(info);
      row.appendChild(chevron);

      // ── Detalj-panel (skjult til å begynne med) ──
      const detail = document.createElement("div");
      detail.className = "game-detail hidden";

      const thumbEl = document.createElement("div");
      thumbEl.className = "game-detail-thumb";
      if (g.thumbnail_url) {
        const img = document.createElement("img");
        img.src = g.thumbnail_url;
        img.alt = g.name;
        img.loading = "lazy";
        thumbEl.appendChild(img);
      } else {
        thumbEl.classList.add("game-detail-thumb-placeholder");
        thumbEl.textContent = "🎮";
      }

      const detailInfo = document.createElement("div");
      detailInfo.className = "game-detail-info";
      const meta = [
        g.genre ? `Sjanger: ${g.genre}` : "",
        g.playing ? `${g.playing.toLocaleString("no")} spiller nå` : "",
      ].filter(Boolean).join("  •  ");
      detailInfo.innerHTML = `
        ${meta ? `<div class="game-detail-meta">${meta}</div>` : ""}
        ${g.description ? `<div class="game-detail-desc">${g.description}</div>` : ""}
      `;

      const actions = document.createElement("div");
      actions.className = "game-detail-actions";

      if (g.status === "blocked") {
        actions.appendChild(makeUnblockBtn(g.universe_id, g.name, childId));
        actions.appendChild(makeApproveBtn(g.universe_id, g.name, childId));
      } else if (g.status === "approved") {
        actions.appendChild(makeUnapproveBtn(g.universe_id));
        actions.appendChild(makeBlockBtn(g.universe_id, g.name, childId));
      } else {
        actions.appendChild(makeApproveBtn(g.universe_id, g.name, childId));
        actions.appendChild(makeBlockBtn(g.universe_id, g.name, childId));
      }

      detail.appendChild(thumbEl);
      detail.appendChild(detailInfo);
      detail.appendChild(actions);

      row.addEventListener("click", () => {
        const expanded = row.getAttribute("aria-expanded") === "true";
        row.setAttribute("aria-expanded", String(!expanded));
        detail.classList.toggle("hidden", expanded);
        chevron.textContent = expanded ? "▸" : "▾";
      });

      wrap.appendChild(row);
      wrap.appendChild(detail);
      list.appendChild(wrap);
    });
  }

  function renderFriends(friends) {
    const section = document.getElementById("friends-section");
    const list = document.getElementById("friends-list");
    const empty = document.getElementById("friends-empty");
    const count = document.getElementById("friends-count");

    count.textContent = friends.length;
    list.innerHTML = "";

    if (!friends.length) {
      empty.classList.remove("hidden");
      return;
    }
    empty.classList.add("hidden");

    friends.forEach(f => {
      const row = document.createElement("div");
      row.className = "friend-row";
      row.innerHTML = `<span class="friend-avatar">${f.name.charAt(0).toUpperCase()}</span><span class="friend-name">${f.name}</span>`;
      list.appendChild(row);
    });
  }

  function setupTabs() {
    document.querySelectorAll(".tab[data-tab]").forEach(btn => {
      btn.addEventListener("click", () => {
        document.querySelectorAll(".tab[data-tab]").forEach(b => b.classList.remove("active"));
        btn.classList.add("active");
        currentTab = btn.dataset.tab;
        if (state) {
          const child = state.children?.[activeChildIndex];
          renderGamesList(child?.top_universes || [], child?.child_id);
        }
      });
    });
  }

  async function refresh() {
    try {
      const s = await fetchJson("/api/state");
      if (!s.configured) { showSetup(); return; }
      state = s;
      showMain();
      renderDashboard(state);
    } catch (e) {
      console.error("refresh feilet:", e);
    }
  }

  async function init() {
    setupTabs();
    initSetup();

    const status = await fetchJson("/api/setup/status");
    if (!status.configured) {
      showSetup();
    } else {
      await refresh();
    }

    setInterval(refresh, 30_000);
  }

  document.addEventListener("DOMContentLoaded", init);
})();

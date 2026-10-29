"use strict";

const $ = (id) => document.getElementById(id);
const state = { status: null, players: [null, null], surface: "Hard", offset: 0, total: 0,
  directorySequence: 0, predictionSequence: 0, profileSequence: 0, predicting: false,
  upcomingSequence: 0, upcomingBusy: false, upcomingBatch: null, upcomingConfigured: false };
const searches = [0, 1].map(() => ({ sequence: 0, selectionSequence: 0, timer: null, options: [], active: -1 }));
const escapeHTML = (value) => String(value ?? "").replace(/[&<>"']/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
const number = (value, decimals = 0) => value == null ? "—" : Number(value).toLocaleString(undefined,
  { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
const percent = (value, decimals = 0) => value == null ? "—" : `${number(value * 100, decimals)}%`;
const dateLabel = (value) => value ? new Date(`${value}T12:00:00`).toLocaleDateString(undefined,
  { day: "numeric", month: "short", year: "numeric" }) : "Unknown";
const initials = (name) => escapeHTML(name.split(/\s+/).filter(Boolean).map((s) => s[0]).slice(0, 2).join(""));
const modelLabel = (name) => ({ logistic_regression: "Logistic regression", random_forest: "Random forest",
  extra_trees: "Extra trees", hist_gradient_boosting: "Histogram boosting", xgboost: "XGBoost" })[name] || name;

async function api(path, options = {}) {
  let response;
  try { response = await fetch(path, options); }
  catch { throw new Error("Cannot reach the local server. Check that the application is running."); }
  if (!response.ok) {
    let message = `Request failed (${response.status}).`;
    try {
      const data = await response.json();
      message = typeof data.detail === "string" ? data.detail
        : Array.isArray(data.detail) ? data.detail.map((item) => item.msg).join(" · ") : message;
    } catch { /* Preserve the HTTP status when a response is not JSON. */ }
    throw new Error(message);
  }
  return response.json();
}

function showError(id, message) { $(id).textContent = message; $(id).hidden = !message; }
function updatePredictButton() {
  $("predict-button").disabled = state.predicting || !state.players[0] || !state.players[1]
    || state.players[0]?.id === state.players[1]?.id || !$("model-select").value;
}
function invalidatePrediction() {
  state.predictionSequence += 1;
  state.predicting = false;
  $("predict-button").innerHTML = 'Predict matchup <span aria-hidden="true">↗</span>';
  $("result").hidden = true;
  $("empty-result").hidden = false;
  showError("prediction-error", "");
  updatePredictButton();
}
function stat(label, value) {
  return `<div><span class="stat-label">${label}</span><span class="stat-value">${value}</span></div>`;
}
function formBadges(matches) {
  return matches.slice(0, 10).map((m) => `<span class="form-badge${m.won ? "" : " loss"}"
    title="${m.won ? "Win" : "Loss"} · ${escapeHTML(m.surface)} · ${escapeHTML(m.tourney_date)}">${m.won ? "W" : "L"}</span>`).join("");
}
function identity(player) {
  return `<div class="player-identity"><div class="avatar" aria-hidden="true">${initials(player.name)}</div>
    <div><h3 class="player-name">${escapeHTML(player.name)}</h3><p class="player-meta">Observed rank ${player.rank == null ? "unavailable" : `#${number(player.rank)}`}
    <span aria-hidden="true"> · </span> ${number(player.wins + player.losses)} matches</p></div></div>`;
}
function renderCard(slot) {
  const player = state.players[slot];
  if (!player) {
    $(`player-detail-${slot}`).innerHTML = '<div class="player-placeholder"><span>◎</span><p>Select a player to see their profile</p></div>';
    return;
  }
  $(`player-detail-${slot}`).innerHTML = `${identity(player)}<div class="stat-row">
    ${stat("Overall Elo", number(player.elo))}${stat(`${state.surface} Elo`, number(player.surface_ratings[state.surface]))}
    ${stat("Last 10 win rate", percent(player.recent_win_rate))}</div><div class="card-bottom"><div>
    <div class="form-badges" aria-label="Recent form, newest first">${formBadges(player.recent_matches)}</div>
    <div class="form-caption">LATEST RESULTS · NEWEST FIRST</div></div>
    <button type="button" class="text-button" data-profile="${escapeHTML(player.id)}">Full profile ↗</button></div>`;
}
function closeSuggestions(slot) {
  $(`suggestions-${slot}`).hidden = true;
  const input = $(`player-search-${slot}`);
  input.setAttribute("aria-expanded", "false");
  input.removeAttribute("aria-activedescendant");
  searches[slot].active = -1;
}
async function selectPlayer(slot, playerId) {
  searches[slot].sequence += 1;
  const sequence = ++searches[slot].selectionSequence;
  clearTimeout(searches[slot].timer);
  closeSuggestions(slot);
  state.players[slot] = null;
  renderCard(slot);
  invalidatePrediction();
  try {
    const player = await api(`/api/players/${encodeURIComponent(playerId)}`);
    if (sequence !== searches[slot].selectionSequence) return;
    state.players[slot] = player;
    $(`player-search-${slot}`).value = player.name;
    renderCard(slot);
    updatePredictButton();
    if (state.players[0]?.id === state.players[1]?.id) showError("prediction-error", "Choose two different players.");
  } catch (error) {
    if (sequence === searches[slot].selectionSequence) showError("prediction-error", error.message);
  }
}
async function suggest(slot) {
  const sequence = ++searches[slot].sequence;
  const input = $(`player-search-${slot}`);
  const list = $(`suggestions-${slot}`);
  const query = input.value.trim();
  searches[slot].options = [];
  searches[slot].active = -1;
  list.hidden = false;
  input.setAttribute("aria-expanded", "true");
  list.innerHTML = '<div class="no-results">Searching…</div>';
  try {
    const data = await api(`/api/players?q=${encodeURIComponent(query)}&limit=8`);
    if (sequence !== searches[slot].sequence) return;
    searches[slot].options = data.players;
    searches[slot].active = -1;
    list.innerHTML = data.players.length ? data.players.map((p, index) => `<button type="button"
      role="option" aria-selected="false" id="option-${slot}-${index}" data-slot="${slot}"
      data-pick="${escapeHTML(p.id)}"><span>${escapeHTML(p.name)}</span><small>${p.rank ? `#${number(p.rank)}` : "Unranked"}</small></button>`).join("")
      : '<div class="no-results">No players found in this history.</div>';
  } catch (error) {
    if (sequence === searches[slot].sequence) list.innerHTML = `<div class="no-results">${escapeHTML(error.message)}</div>`;
  }
}
function showView(view, updateLocation = true) {
  const players = view === "players";
  const upcoming = view === "upcoming";
  const performance = view === "performance";
  $("matchup-view").hidden = players || upcoming || performance;
  $("players-view").hidden = !players;
  $("upcoming-view").hidden = !upcoming;
  $("performance-view").hidden = !performance;
  $("breadcrumb").textContent = performance ? "Performance" : upcoming ? "Upcoming matches" : players ? "Player explorer" : "Match predictor";
  $("page-title").textContent = performance ? "The results behind the forecasts." : upcoming ? "The next match starts here." : players ? "Get to know the game makers." : "Every matchup tells a story.";
  $("page-description").textContent = performance ? "See how saved forecasts compare with results, and which matches are still pending." : upcoming ? "Upcoming ATP matches, with a forecast from your player history." : players ? "Career records, surface strength, and the form behind the numbers." : "Explore the players. Set the conditions. See how they compare.";
  const path = performance ? "/performance" : "/";
  if (updateLocation && location.pathname !== path) history.pushState({ view }, "", path);
  document.querySelectorAll("[data-view]").forEach((button) => {
    button.classList.toggle("active", button.dataset.view === view);
    if (button.dataset.view === view) button.setAttribute("aria-current", "page");
    else button.removeAttribute("aria-current");
  });
  if (players) loadDirectory();
  if (upcoming) loadUpcoming();
  if (performance) loadPerformance();
}

const performanceReasons = {
  result_not_collected: "Awaiting result collection", result_unavailable: "Result unavailable",
  match_id_conflict: "Match identity changed", participant_conflict: "Participants changed",
  cancelled: "Cancelled matches", not_completed: "Matches still to finish",
  excluded_outcome: "Retirements, walkovers or other excluded outcomes", unknown_winner: "Winner not confirmed",
  unproven_timing: "Match timing not confirmed", not_pre_match: "Forecast outside the pre-match window",
  incompatible_history_date: "History too recent for evaluation", invalid_probability: "Invalid forecast probabilities",
  superseded_forecast: "Earlier forecasts replaced by a later eligible forecast"
};

let performanceDecisions = [];
let performancePage = 0;
const performancePageSize = 25;

function forecastStatus(row) {
  if (row.prediction_correct === true) return "Correct";
  if (row.prediction_correct === false) return "Incorrect";
  return performanceReasons[row.reason] || row.reason || "Details unavailable";
}

function renderPerformanceForecasts() {
  const query = $("forecast-search").value.trim().toLowerCase();
  const filter = $("forecast-status").value;
  const pendingReasons = ["result_not_collected", "result_unavailable", "not_completed", "unknown_winner"];
  const matches = performanceDecisions.filter((row) => {
    const pending = pendingReasons.includes(row.reason);
    const selected = filter === "all" || (filter === "evaluated" && row.reason == null)
      || (filter === "correct" && row.prediction_correct === true)
      || (filter === "incorrect" && row.prediction_correct === false)
      || (filter === "pending" && pending)
      || (filter === "excluded" && row.reason != null && !pending);
    return selected && [row.player0_name, row.player1_name, row.match_id, row.model_name,
      row.model_version, row.forecast?.match?.tournament, row.batch_id].join(" ").toLowerCase().includes(query);
  });
  const pages = Math.max(1, Math.ceil(matches.length / performancePageSize));
  performancePage = Math.min(performancePage, pages - 1);
  $("forecast-count").textContent = `${number(matches.length)} of ${number(performanceDecisions.length)} saved forecasts · Page ${performancePage + 1} of ${pages}`;
  $("forecast-prev").disabled = performancePage === 0;
  $("forecast-next").disabled = performancePage + 1 >= pages;
  $("forecast-rows").innerHTML = matches.slice(performancePage * performancePageSize,
    (performancePage + 1) * performancePageSize).map((row) => {
    const forecast = row.forecast || {};
    const match = forecast.match || {};
    const result = row.result || {};
    const names = [row.player0_name || "Player 1", row.player1_name || "Player 2"];
    const winner = row.winner_player_index == null ? null : names[row.winner_player_index];
    const score = result.score == null ? "Score unavailable" : typeof result.score === "object" ? JSON.stringify(result.score) : result.score;
    const resultState = [result.status, result.outcome].filter(Boolean).join(" · ");
    const tone = row.prediction_correct === true ? "correct" : row.prediction_correct === false ? "incorrect" : "unscored";
    return `<tr><td><strong>${escapeHTML(names[0])} vs ${escapeHTML(names[1])}</strong>
      <small>${escapeHTML([match.tournament, match.round, row.surface].filter(Boolean).join(" · "))}</small>
      <small>Match ${escapeHTML(row.match_id)} · ${escapeHTML(match.scheduled_time ? kickoffLabel(match.scheduled_time) : "Schedule unavailable")}</small></td>
      <td>${escapeHTML(names[0])}: ${percent(forecast.player0_win_probability, 1)}<br>${escapeHTML(names[1])}: ${percent(forecast.player1_win_probability, 1)}
      <small>Forecast ${escapeHTML(row.created_at ? kickoffLabel(row.created_at) : "time unavailable")}</small></td>
      <td><strong>${escapeHTML(winner || (row.reason === "match_id_conflict" || row.reason === "participant_conflict" ? "Result identity unverified" : "Winner not confirmed"))}</strong>
      <small>${escapeHTML(score)}</small><small>${escapeHTML(resultState || "Result not collected")}</small></td>
      <td>${escapeHTML(modelLabel(row.model_name) || "Unknown model")}<small>Version ${escapeHTML(row.model_version?.slice(0, 12))}</small></td>
      <td><span class="forecast-status ${tone}">${escapeHTML(forecastStatus(row))}</span>
      <details class="forecast-record"><summary>Prediction & result</summary><p class="context-note">Result observed ${escapeHTML(row.result_observed_at ? kickoffLabel(row.result_observed_at) : "—")}</p>
      <pre>${escapeHTML(JSON.stringify({ batch_id: row.batch_id, forecast: row.forecast, result: row.result }, null, 2))}</pre></details></td></tr>`;
  }).join("") || '<tr><td colspan="5">No forecasts match these filters.</td></tr>';
}

function renderPerformance(report) {
  performanceDecisions = [...(report.decisions || [])].sort((a, b) => (b.created_at || "").localeCompare(a.created_at || ""));
  performancePage = 0;
  $("performance-updated").textContent = `Report updated ${kickoffLabel(report.generated_at)}`;
  const rows = report.models.map((model) => `<tr><td><strong>${escapeHTML(modelLabel(model.model_name))}</strong><small>Version ${escapeHTML(model.model_version.slice(0, 12))}</small></td>
    <td>${number(model.count)} <span class="muted">/ ${number(model.forecast_matches)}</span></td><td>${percent(model.accuracy, 1)}</td><td>${number(model.brier, 4)}</td><td>${number(model.log_loss, 4)}</td></tr>`).join("");
  const reasons = Object.entries(report.reason_counts).map(([reason, count]) => `<li><span>${escapeHTML(performanceReasons[reason] || "Other excluded forecasts")}</span><strong>${number(count)}</strong></li>`).join("");
  $("performance-content").innerHTML = `<div class="performance-stats">
    <article class="performance-stat">${stat("Matches forecast", number(report.forecast_match_count))}</article>
    <article class="performance-stat">${stat("Matches evaluated", number(report.evaluated_match_count))}</article>
    <article class="performance-stat">${stat("Forecast coverage", percent(report.prediction_coverage, 1))}</article>
    <article class="performance-stat">${stat("Saved batches", number(report.batch_count))}</article></div>
    <section class="performance-panel"><div class="section-heading"><h2>Results by model</h2><span>PROSPECTIVE EVALUATION</span></div>
      <div class="performance-table-wrap" role="region" aria-label="Model performance" tabindex="0"><table class="performance-table"><thead><tr><th>Model</th><th>Evaluated / forecast</th><th>Accuracy</th><th>Brier</th><th>Log loss</th></tr></thead><tbody>${rows}</tbody></table></div>
      ${!report.evaluated_count ? '<div class="performance-waiting"><span class="status-dot"></span>No eligible completed matches yet. Metrics will appear as results become available.</div>' : ""}
      <p class="context-note">Lower Brier and log loss indicate better probability estimates. A neutral 50% forecast has Brier 0.25 and log loss 0.6931. Models may cover different matches.</p></section>
    <section class="performance-panel"><div class="section-heading"><h2>Every prediction & result</h2></div>
      <p class="context-note">Each saved forecast appears separately, including earlier forecasts and excluded results. Only evaluated rows count toward the metrics. At 50% / 50%, accuracy selects the second player.</p>
      <div class="forecast-filters"><label>Search forecasts<input id="forecast-search" type="search" placeholder="Player, tournament, match or model"></label>
      <label>Evaluation<select id="forecast-status"><option value="all">All forecasts</option><option value="evaluated">Evaluated</option><option value="correct">Correct</option><option value="incorrect">Incorrect</option><option value="pending">Pending</option><option value="excluded">Excluded / superseded</option></select></label></div>
      <div class="performance-table-wrap" role="region" aria-label="Individual predictions and results" tabindex="0"><table class="performance-table forecast-table"><thead><tr><th>Match</th><th>Saved prediction</th><th>Observed result</th><th>Model</th><th>Evaluation</th></tr></thead><tbody id="forecast-rows"></tbody></table></div>
      <div class="forecast-pagination"><span id="forecast-count" role="status"></span><button id="forecast-prev" class="secondary-button" type="button">Previous</button><button id="forecast-next" class="secondary-button" type="button">Next</button></div></section>
    <div class="performance-details"><section class="performance-panel"><div class="section-heading"><h2>Excluded or pending forecasts</h2></div><ul class="performance-reasons">${reasons || '<li>No excluded forecasts.</li>'}</ul><p class="context-note">Counts refer to individual saved forecasts. A match can appear in more than one batch.</p></section>
    <section class="performance-panel"><div class="section-heading"><h2>What counts in the report</h2></div><p class="performance-policy">The last eligible forecast for each match and model version. Only normal completions are scored; retirements, walkovers and cancellations are excluded.</p><p class="context-note">Forecasts must precede both saved schedules and the provider’s recorded live time. That observation can change and is not an official start time. Pending matches remain in coverage totals. Small samples do not establish calibration.</p></section></div>
    <div class="performance-download"><a class="secondary-button" href="/api/performance/download">Download report ↓</a><a class="text-button" href="/api/performance">View full data ↗</a></div>`;
  for (const id of ["forecast-search", "forecast-status"]) {
    $(id).addEventListener(id === "forecast-search" ? "input" : "change", () => { performancePage = 0; renderPerformanceForecasts(); });
  }
  $("forecast-prev").addEventListener("click", () => { performancePage -= 1; renderPerformanceForecasts(); });
  $("forecast-next").addEventListener("click", () => { performancePage += 1; renderPerformanceForecasts(); });
  renderPerformanceForecasts();
}

async function loadPerformance() {
  $("reload-performance").disabled = true;
  showError("performance-error", "");
  try {
    const response = await fetch("/api/performance");
    if (response.status === 404) {
      $("performance-updated").textContent = "Your forecast record starts here.";
      $("performance-content").innerHTML = '<div class="upcoming-empty"><span aria-hidden="true">↗</span><h3>No performance report yet.</h3><p>Once saved forecasts have been checked against results, your report will appear here.</p></div>';
      return;
    }
    if (!response.ok) throw new Error("Cannot load the performance report. Try again later.");
    renderPerformance(await response.json());
  } catch (error) { showError("performance-error", error.message); }
  finally { $("reload-performance").disabled = false; }
}

window.addEventListener("popstate", (event) => showView(event.state?.view || (location.pathname === "/performance" ? "performance" : "upcoming"), false));

const kickoffLabel = (value) => new Date(value).toLocaleString(undefined,
  { day: "numeric", month: "short", hour: "2-digit", minute: "2-digit", timeZoneName: "short" });

function renderUpcoming(data) {
  const batch = data.batch;
  state.upcomingBatch = batch;
  state.upcomingConfigured = data.configured;
  $("refresh-upcoming").disabled = state.upcomingBusy || !data.configured || !$("model-select").value;
  $("upcoming-model").textContent = `Model: ${$("model-select").selectedOptions[0]?.textContent || "No model selected"}`;
  $("download-batch").hidden = !batch;
  if (!batch) {
    $("upcoming-summary").textContent = "";
    $("upcoming-grid").innerHTML = `<div class="upcoming-empty"><span aria-hidden="true">▦</span><h3>Your next matches, in focus.</h3><p>${!data.configured ? "Configure the match provider on the server to load upcoming games." : !$("model-select").value ? "Choose a compatible model to generate forecasts." : "Refresh the schedule to generate forecasts for the available matches."}</p></div>`;
    return;
  }
  const matches = batch.predictions.filter((row) => new Date(row.match.scheduled_time) > new Date());
  const skipped = batch.skipped.length;
  $("upcoming-summary").textContent = `${matches.length} upcoming forecasts · ${skipped} skipped · Schedule checked ${kickoffLabel(batch.source_fetched_at)} · Predictions ${kickoffLabel(batch.created_at)}`;
  $("download-batch").href = `/api/upcoming/batches/${encodeURIComponent(batch.id)}`;
  $("download-batch").setAttribute("download", `predictions-${batch.id.slice(0, 12)}.json`);
  $("upcoming-grid").innerHTML = matches.length ? matches.map((row) => {
    const p0 = row.player0_win_probability, p1 = row.player1_win_probability;
    return `<article class="upcoming-card">
      <div class="upcoming-context"><span>${escapeHTML(row.match.tournament)} · ${escapeHTML(row.match.round)}</span><span class="upcoming-surface">${escapeHTML(row.match.surface)}</span></div>
      <time datetime="${escapeHTML(row.match.scheduled_time)}">${escapeHTML(kickoffLabel(row.match.scheduled_time))}</time>
      <div class="upcoming-player"><button type="button" data-profile="${escapeHTML(row.player0.id)}">${escapeHTML(row.player0.name)}</button><strong>${percent(p0, 1)}</strong></div>
      <div class="upcoming-player"><button type="button" data-profile="${escapeHTML(row.player1.id)}">${escapeHTML(row.player1.name)}</button><strong>${percent(p1, 1)}</strong></div>
      <div class="probability-bar" role="img" aria-label="${escapeHTML(row.player0.name)} ${percent(p0, 1)}, ${escapeHTML(row.player1.name)} ${percent(p1, 1)}"><span style="width:${p0 * 100}%"></span></div>
      <p class="upcoming-history">${escapeHTML(modelLabel(row.model_name))} · History ${escapeHTML(dateLabel(row.history_source_date))}</p>
      <details><summary>Prediction details</summary><p class="context-note">Player one: ${escapeHTML(row.player0.name)} · Player two: ${escapeHTML(row.player1.name)}</p>
        <table class="feature-table"><tbody>${Object.entries(row.features).map(([name, value]) => `<tr><td>${escapeHTML(name.replaceAll("_", " "))}</td><td>${number(value, 4)}</td></tr>`).join("")}</tbody></table>
        <p class="context-note">Fair decimal odds: ${number(row.player0_decimal_odds, 2)} / ${number(row.player1_decimal_odds, 2)}. Probabilities are estimates; calibration has not been established.</p></details>
    </article>`;
  }).join("") : '<div class="upcoming-empty"><h3>No eligible upcoming matches.</h3><p>Unavailable players, postponements and matches that have started are excluded. Refresh the schedule later.</p></div>';
}

async function loadUpcoming(refresh = false) {
  if (refresh && state.upcomingBusy) return;
  const sequence = ++state.upcomingSequence;
  state.upcomingBusy = refresh;
  $("refresh-upcoming").disabled = true;
  $("refresh-upcoming").textContent = refresh ? "Updating forecasts…" : "Refresh & predict ↗";
  showError("upcoming-error", "");
  if (!refresh) {
    state.upcomingBatch = null;
    $("upcoming-summary").textContent = "";
    $("download-batch").hidden = true;
    $("upcoming-grid").innerHTML = '<div class="loading">Loading upcoming forecasts…</div>';
  }
  try {
    const modelId = $("model-select").value;
    const data = refresh ? await api("/api/upcoming/refresh", {
      method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ model_id: modelId })
    }) : await api(`/api/upcoming?model_id=${encodeURIComponent(modelId)}`);
    if (sequence !== state.upcomingSequence) return;
    state.upcomingBusy = false;
    renderUpcoming(data);
  } catch (error) {
    if (sequence !== state.upcomingSequence) return;
    showError("upcoming-error", error.message);
    if (!refresh) { $("upcoming-grid").innerHTML = ""; $("upcoming-summary").textContent = ""; $("download-batch").hidden = true; }
    $("refresh-upcoming").disabled = !$("model-select").value;
  } finally {
    if (sequence === state.upcomingSequence) {
      state.upcomingBusy = false;
      $("refresh-upcoming").textContent = "Refresh & predict ↗";
    }
  }
}

async function loadDirectory() {
  const sequence = ++state.directorySequence;
  $("directory-grid").innerHTML = '<div class="loading">Loading player profiles…</div>';
  $("previous-page").disabled = true;
  $("next-page").disabled = true;
  showError("directory-error", "");
  try {
    const data = await api(`/api/players?q=${encodeURIComponent($("directory-search").value)}&limit=24&offset=${state.offset}`);
    if (sequence !== state.directorySequence) return;
    state.total = data.total;
    $("directory-total").textContent = `${number(data.total)} PLAYERS`;
    $("directory-grid").innerHTML = data.players.length ? data.players.map((player) => `<article class="directory-card">${identity(player)}
      <div class="stat-row">${stat("Overall Elo", number(player.elo))}${stat("Career wins", number(player.wins))}${stat("Win rate", percent(player.wins / (player.wins + player.losses)))}</div>
      <div class="card-bottom"><span class="player-meta">Last result ${escapeHTML(dateLabel(player.last_source_date))}</span>
      <button type="button" class="text-button" data-profile="${escapeHTML(player.id)}" aria-label="View ${escapeHTML(player.name)} profile">View profile ↗</button></div></article>`).join("")
      : '<div class="loading">No players match your search.</div>';
    $("page-info").textContent = data.total ? `${state.offset + 1}–${Math.min(state.offset + 24, data.total)} of ${number(data.total)}` : "0 players";
    $("previous-page").disabled = state.offset === 0;
    $("next-page").disabled = state.offset + 24 >= data.total;
  } catch (error) {
    if (sequence === state.directorySequence) {
      $("directory-grid").innerHTML = "";
      showError("directory-error", error.message);
    }
  }
}

async function openProfile(playerId) {
  const sequence = ++state.profileSequence;
  $("profile-dialog-content").innerHTML = '<h2 id="profile-dialog-title">Player profile</h2><div class="loading">Loading profile…</div>';
  if (!$("profile-dialog").open) $("profile-dialog").showModal();
  try {
    const player = await api(`/api/players/${encodeURIComponent(playerId)}`);
    if (sequence !== state.profileSequence) return;
    const surfaces = Object.entries(player.surface_ratings).map(([surface, elo]) => {
      const form = player.surface_form[surface];
      return `<tr><td>${escapeHTML(surface)}</td><td>${number(elo)}</td><td>${percent(form?.win_rate)} <span class="muted">(${form?.matches ?? 0})</span></td></tr>`;
    }).join("");
    $("profile-dialog-content").innerHTML = `<h2 id="profile-dialog-title">${escapeHTML(player.name)}</h2><div class="profile-stats">
      ${stat("Observed rank", player.rank ? `#${number(player.rank)}` : "—")}${stat("Overall Elo", number(player.elo))}
      ${stat("Career record", `${number(player.wins)}<small>–${number(player.losses)}</small>`)}${stat("Career win rate", percent(player.win_rate))}</div>
      <p class="context-note">Last player result ${escapeHTML(dateLabel(player.last_source_date))}. Age ${number(player.age, 1)} and ranking points ${number(player.rank_points)} are last observed values.</p>
      <div class="profile-section"><div><h3>Surface strength</h3><table class="surface-table"><thead><tr><th>Surface</th><th>Elo</th><th>Recent form</th></tr></thead><tbody>${surfaces}</tbody></table>
      <p class="context-note">Form uses up to 10 results per surface.</p></div><div><h3>Serving, over observed history</h3><table class="surface-table"><tbody>
      <tr><td>Aces / serve points</td><td>${percent(player.serve.ace_rate, 1)}</td></tr><tr><td>Double faults / serve points</td><td>${percent(player.serve.double_fault_rate, 1)}</td></tr>
      <tr><td>Service points won</td><td>${percent(player.serve.service_points_won_rate, 1)}</td></tr></tbody></table><p class="context-note">${number(player.serve.observed_points)} observed serve points. Rates use valid observations; missing values display as —.</p></div></div>
      <h3>Recent results <span class="muted">· newest first</span></h3><div class="form-badges">${formBadges(player.recent_matches)}</div>
      <p class="context-note">Dates are source dates, which may represent a tournament week. Opponents and scores are not stored in this player snapshot.</p>
      <table class="recent-table"><thead><tr><th>Source date</th><th>Surface</th><th>Result</th></tr></thead><tbody>${player.recent_matches.slice(0, 15).map((m) => `<tr><td>${escapeHTML(dateLabel(m.tourney_date))}</td><td>${escapeHTML(m.surface)}</td><td><span class="form-badge${m.won ? "" : " loss"}">${m.won ? "W" : "L"}</span></td></tr>`).join("")}</tbody></table>
      <div class="profile-actions"><button type="button" class="primary-button" data-use-player="${escapeHTML(player.id)}" data-slot="0">Use as player one ↗</button><button type="button" class="secondary-button" data-use-player="${escapeHTML(player.id)}" data-slot="1">Use as player two ↗</button></div>`;
  } catch (error) {
    if (sequence === state.profileSequence) $("profile-dialog-content").innerHTML = `<h2 id="profile-dialog-title">Player profile</h2><div class="notice error" role="alert">${escapeHTML(error.message)}</div>`;
  }
}

function renderResult(result) {
  const p0 = result.player0_win_probability;
  const p1 = result.player1_win_probability;
  $("result").innerHTML = `<div class="result-heading"><div><div class="eyebrow">THE MATCHUP FORECAST</div><h2>A look at the odds.</h2></div><small>${escapeHTML(result.request.surface)} · ${escapeHTML(dateLabel(result.request.match_date))}</small></div>
    <div class="forecast"><div class="forecast-side"><h3>${escapeHTML(result.player0.name)}</h3><div class="probability">${percent(p0, 1)}</div><div class="fair-odds">Fair decimal odds <strong>${result.player0_decimal_odds == null ? "∞" : number(result.player0_decimal_odds, 2)}</strong></div></div>
    <div class="forecast-side"><h3>${escapeHTML(result.player1.name)}</h3><div class="probability">${percent(p1, 1)}</div><div class="fair-odds">Fair decimal odds <strong>${result.player1_decimal_odds == null ? "∞" : number(result.player1_decimal_odds, 2)}</strong></div></div></div>
    <div class="probability-bar" role="img" aria-label="${escapeHTML(result.player0.name)} ${percent(p0, 1)}, ${escapeHTML(result.player1.name)} ${percent(p1, 1)}"><span style="width:${p0 * 100}%"></span></div>
    <div class="result-bottom"><span>Recorded head-to-head <strong>${result.head_to_head.player0_wins} – ${result.head_to_head.player1_wins}</strong></span><span>${escapeHTML(modelLabel(result.model_name))} · History ${escapeHTML(dateLabel(result.history_source_date))}</span></div>
    <p class="context-note">These are model probabilities and fair odds, with no bookmaker margin. Probability calibration has not been established.</p>
    <details><summary>Inspect features & prediction provenance</summary><table class="feature-table"><tbody>${Object.entries(result.features).map(([feature, value]) => `<tr><td>${escapeHTML(feature.replaceAll("_", " "))}</td><td>${number(value, 4)}</td></tr>`).join("")}</tbody></table>
    <p class="context-note">Signed comparisons favor player two when positive, except age and serving rates, whose interpretation depends on the feature.</p>
    <p class="mono">Model SHA-256: ${escapeHTML(result.model_version)}<br>History SHA-256: ${escapeHTML(result.history_version)}<br>MLflow run: ${escapeHTML(result.run_id || "Untracked")}</p>
    <button type="button" id="download-prediction" class="secondary-button">Download prediction JSON ↓</button></details>`;
  $("result").hidden = false;
  $("empty-result").hidden = true;
  $("download-prediction").addEventListener("click", () => {
    const url = URL.createObjectURL(new Blob([JSON.stringify(result, null, 2)], { type: "application/json" }));
    const link = document.createElement("a");
    link.href = url; link.download = `court-report-${result.request.match_date}.json`; link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  });
}

function updateModelInformation() {
  const model = state.status?.models.find((m) => m.id === $("model-select").value);
  $("model-information").innerHTML = model ? `<div class="profile-stats">${stat("Features", model.feature_count)}${stat("Validation log loss", number(model.validation_log_loss, 4))}</div>
    <p class="context-note">Training observations through ${escapeHTML(dateLabel(model.trained_through))}. This model is compatible with player-history predictions.</p><p class="mono">MLflow run: ${escapeHTML(model.run_id || "Untracked")}<br>History SHA-256: ${escapeHTML(state.status.history_version)}</p>` : '<p class="context-note">No compatible trained model was found. Train a model with the default comparison features, then restart the application.</p>';
}

async function initialize() {
  try {
    state.status = await api("/api/status");
    const now = new Date();
    const today = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-${String(now.getDate()).padStart(2, "0")}`;
    const cutoff = new Date(`${state.status.history_source_date}T12:00:00`);
    const localToday = new Date(`${today}T12:00:00`);
    const lag = Math.max(0, Math.round((localToday - cutoff) / 86400000));
    $("snapshot-label").textContent = `History through ${dateLabel(state.status.history_source_date)}${lag ? ` · ${lag} days before today` : ""}`;
    $("player-count").textContent = `${number(state.status.player_count)} player profiles`;
    $("match-date").min = state.status.minimum_prediction_date;
    $("match-date").value = today > state.status.minimum_prediction_date ? today : state.status.minimum_prediction_date;
    $("model-select").innerHTML = state.status.models.map((m) => `<option value="${escapeHTML(m.id)}">${escapeHTML(modelLabel(m.name))} · ${m.run_id ? escapeHTML(m.run_id.slice(0, 8)) : m.id}</option>`).join("");
    $("model-select").value = state.status.default_model_id || "";
    updateModelInformation();
    if (!state.status.models.length) showError("startup-error", "Player profiles are ready. Train a compatible model and restart the application to enable predictions.");
    const defaults = await Promise.all(["Carlos Alcaraz", "Jannik Sinner"].map((name) => api(`/api/players?q=${encodeURIComponent(name)}&limit=8`)));
    await Promise.all(defaults.map((data, slot) => data.players[0] ? selectPlayer(slot, data.players[0].id) : Promise.resolve()));
    updatePredictButton();
  } catch (error) { showError("startup-error", error.message); $("snapshot-label").textContent = "History unavailable"; }
  if (location.pathname === "/performance") showView("performance", false);
  else if (state.status) showView("upcoming", false);
}

for (const slot of [0, 1]) {
  const input = $(`player-search-${slot}`);
  input.addEventListener("input", () => {
    searches[slot].sequence += 1;
    searches[slot].selectionSequence += 1;
    state.players[slot] = null;
    renderCard(slot); invalidatePrediction();
    clearTimeout(searches[slot].timer);
    closeSuggestions(slot);
    searches[slot].timer = setTimeout(() => suggest(slot), 180);
  });
  input.addEventListener("focus", () => { if (!state.players[slot]) suggest(slot); });
  input.addEventListener("keydown", (event) => {
    const search = searches[slot];
    const list = $(`suggestions-${slot}`);
    if (event.key === "Escape") { search.sequence += 1; closeSuggestions(slot); }
    if (!list.hidden && search.options.length && ["ArrowDown", "ArrowUp"].includes(event.key)) {
      event.preventDefault();
      search.active = (search.active + (event.key === "ArrowDown" ? 1 : -1) + search.options.length) % search.options.length;
      list.querySelectorAll("button").forEach((button, i) => {
        button.classList.toggle("focused", i === search.active);
        button.setAttribute("aria-selected", i === search.active ? "true" : "false");
      });
      input.setAttribute("aria-activedescendant", `option-${slot}-${search.active}`);
      $(`option-${slot}-${search.active}`).scrollIntoView({ block: "nearest" });
    }
    if (!list.hidden && event.key === "Enter") {
      event.preventDefault();
      if (search.options[search.active]) selectPlayer(slot, search.options[search.active].id);
    }
  });
}
document.addEventListener("click", (event) => {
  if (!event.target.closest(".search-wrap")) [0, 1].forEach((slot) => {
    searches[slot].sequence += 1; clearTimeout(searches[slot].timer); closeSuggestions(slot);
  });
  const target = event.target.closest("button");
  if (target?.dataset.pick) selectPlayer(Number(target.dataset.slot), target.dataset.pick);
  if (target?.dataset.profile) openProfile(target.dataset.profile);
  if (target?.dataset.view) showView(target.dataset.view);
  if (target?.dataset.closeDialog) $(target.dataset.closeDialog).close();
  if (target?.dataset.usePlayer) {
    selectPlayer(Number(target.dataset.slot), target.dataset.usePlayer);
    $("profile-dialog").close(); showView("matchup");
  }
});
document.querySelectorAll("[data-surface]").forEach((button) => button.addEventListener("click", () => {
  state.surface = button.dataset.surface;
  document.querySelectorAll("[data-surface]").forEach((b) => {
    b.classList.toggle("active", b === button); b.setAttribute("aria-pressed", b === button ? "true" : "false");
  });
  [0, 1].forEach(renderCard); invalidatePrediction();
}));
$("swap-players").addEventListener("click", () => {
  for (const slot of [0, 1]) {
    searches[slot].sequence += 1; searches[slot].selectionSequence += 1;
    clearTimeout(searches[slot].timer); closeSuggestions(slot);
  }
  state.players.reverse();
  [0, 1].forEach((slot) => { $(`player-search-${slot}`).value = state.players[slot]?.name || ""; renderCard(slot); });
  invalidatePrediction();
});
$("match-date").addEventListener("input", invalidatePrediction);
$("model-select").addEventListener("change", () => { updateModelInformation(); invalidatePrediction(); loadUpcoming(); });
$("refresh-upcoming").addEventListener("click", () => loadUpcoming(true));
$("reload-performance").addEventListener("click", loadPerformance);
setInterval(() => {
  if (state.upcomingBatch && !$("upcoming-view").hidden && !state.upcomingBusy) {
    renderUpcoming({ configured: state.upcomingConfigured, batch: state.upcomingBatch });
  }
}, 30000);
$("model-details-button").addEventListener("click", () => $("settings-dialog").showModal());
let directoryTimer;
$("directory-search").addEventListener("input", () => {
  clearTimeout(directoryTimer); state.offset = 0; state.directorySequence += 1;
  directoryTimer = setTimeout(loadDirectory, 200);
});
$("previous-page").addEventListener("click", () => { state.offset = Math.max(0, state.offset - 24); loadDirectory(); });
$("next-page").addEventListener("click", () => { state.offset += 24; loadDirectory(); });
$("prediction-form").addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!state.players[0] || !state.players[1] || state.predicting) return;
  const sequence = ++state.predictionSequence;
  state.predicting = true; updatePredictButton();
  $("predict-button").textContent = "Reading the matchup…";
  showError("prediction-error", "");
  try {
    const result = await api("/api/predict", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ player0_id: state.players[0].id, player1_id: state.players[1].id,
        surface: state.surface, match_date: $("match-date").value, model_id: $("model-select").value }) });
    if (sequence === state.predictionSequence) renderResult(result);
  } catch (error) { if (sequence === state.predictionSequence) showError("prediction-error", error.message); }
  finally {
    if (sequence === state.predictionSequence) {
      state.predicting = false;
      $("predict-button").innerHTML = 'Predict matchup <span aria-hidden="true">↗</span>';
      updatePredictButton();
    }
  }
});
initialize();

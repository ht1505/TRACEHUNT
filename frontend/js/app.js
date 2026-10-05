/**
 * TraceHunt - Frontend Application Controller
 * Handles tab navigation, API calls to Flask backend, and dynamic UI rendering.
 */

const API_BASE = "";

document.addEventListener("DOMContentLoaded", () => {
  initTabs();
  loadOverview();
  loadIncidentsDropdown();
  loadAnomalies();

  // Search Button Handlers
  document.getElementById("btnSearchJourney").addEventListener("click", searchJourney);
  document.getElementById("btnLoadIncident").addEventListener("click", loadSelectedIncident);

  // Initial load for demo candidate
  searchJourney();
});

// ==============================================================================
// Tab Navigation Controller
// ==============================================================================
function initTabs() {
  const tabs = document.querySelectorAll(".nav-tab");
  tabs.forEach(tab => {
    tab.addEventListener("click", () => {
      tabs.forEach(t => t.classList.remove("active"));
      tab.classList.add("active");

      const target = tab.getAttribute("data-tab");
      document.querySelectorAll(".view-panel").forEach(panel => {
        panel.classList.remove("active");
      });
      const activePanel = document.getElementById(`view-${target}`);
      if (activePanel) {
        activePanel.classList.add("active");
      }
    });
  });
}

// ==============================================================================
// Overview Statistics
// ==============================================================================
async function loadOverview() {
  try {
    const res = await fetch(`${API_BASE}/api/overview`);
    if (res.ok) {
      const data = await res.json();
      document.getElementById("valPeople").textContent = Number(data.total_people || 1000).toLocaleString();
      document.getElementById("valLocations").textContent = data.total_locations || 50;
      document.getElementById("valIncidents").textContent = data.total_incidents || 50;
      document.getElementById("valAnomalies").textContent = data.total_anomalies || "--";
      if (data.database_status) {
        document.getElementById("dbStatusBadge").textContent = data.database_status;
      }
    }
  } catch (err) {
    console.warn("Using offline overview defaults:", err);
  }
}

// ==============================================================================
// Person Journey Reconstruction
// ==============================================================================
window.setPerson = function(personId) {
  document.getElementById("personIdInput").value = personId;
  searchJourney();
};

async function searchJourney() {
  const personId = document.getElementById("personIdInput").value.trim() || "P00023";
  const summaryBox = document.getElementById("journeySummaryCard");
  const timelineList = document.getElementById("timelineList");

  try {
    const res = await fetch(`${API_BASE}/api/journey/${personId}`);
    const data = await res.json();

    if (res.ok && data && !data.error) {
      // The API returns the journey document itself, not an array or an envelope.
      const j = data.journey || data;
      summaryBox.style.display = "block";
      document.getElementById("jPersonName").textContent = `Citizen ${j.person_id} Journey Record`;
      document.getElementById("jPersonMeta").textContent = `Person ID: ${j.person_id} | Date: ${j.date || '2026-09-29'}`;
      document.getElementById("jEventCount").textContent = j.total_events || (j.timeline ? j.timeline.length : 0);
      document.getElementById("jDistance").textContent = j.total_distance_km || "0.0";

      // Populate Timeline Cards
      timelineList.innerHTML = "";
      if (j.timeline && j.timeline.length > 0) {
        j.timeline.forEach(step => {
          const item = document.createElement("div");
          item.className = "timeline-item";
          item.innerHTML = `
            <div>
              <div class="tl-time">${step.timestamp}</div>
              <div class="tl-loc">${step.location_name || step.location_id}</div>
            </div>
            <div class="tl-action">${step.event_type} (${step.transport_mode || 'Transit'})</div>
          `;
          timelineList.appendChild(item);
        });
      } else {
        timelineList.innerHTML = `<p class='text-muted'>No individual timeline stops recorded.</p>`;
      }
    } else if (res.status === 404) {
      summaryBox.style.display = "block";
      document.getElementById("jPersonName").textContent = `Citizen ${personId}`;
      document.getElementById("jPersonMeta").textContent = `Person ID: ${personId}`;
      document.getElementById("jEventCount").textContent = "0";
      document.getElementById("jDistance").textContent = "0.0";
      timelineList.innerHTML = `<p class='text-muted'>No journey data found for ${personId}. Run the available journey analytics pipeline and try again.</p>`;
    } else {
      summaryBox.style.display = "block";
      document.getElementById("jPersonName").textContent = `Citizen ${personId}`;
      document.getElementById("jPersonMeta").textContent = `Person ID: ${personId}`;
      timelineList.innerHTML = `<p class='text-muted'>Journey service error${data && data.error ? `: ${data.error}` : ` (HTTP ${res.status})`}. Please try again.</p>`;
    }
  } catch (err) {
    console.error("Journey search error:", err);
    summaryBox.style.display = "block";
    document.getElementById("jPersonName").textContent = `Citizen ${personId}`;
    document.getElementById("jPersonMeta").textContent = `Person ID: ${personId}`;
    timelineList.innerHTML = `<p class='text-muted'>Unable to load journey data right now. Check the API connection and try again.</p>`;
  }
}

// ==============================================================================
// Incident Investigation
// ==============================================================================
async function loadIncidentsDropdown() {
  const select = document.getElementById("incidentSelect");
  try {
    const res = await fetch(`${API_BASE}/api/incidents`);
    if (res.ok) {
      const list = await res.json();
      select.innerHTML = "";
      list.forEach(inc => {
        const opt = document.createElement("option");
        opt.value = inc.incident_id;
        opt.textContent = `${inc.incident_id} - ${inc.location_name || inc.location_id} (${inc.incident_type}, ${inc.severity})`;
        select.appendChild(opt);
      });
      loadSelectedIncident();
    }
  } catch (err) {
    console.warn("Using default incident select:", err);
  }
}

async function loadSelectedIncident() {
  const incId = document.getElementById("incidentSelect").value || "INC_0001";
  const tableBody = document.getElementById("incidentNearbyTable");

  try {
    const res = await fetch(`${API_BASE}/api/incidents/${incId}`);
    if (res.ok) {
      const inc = await res.json();
      document.getElementById("incCaseTitle").textContent = `${inc.incident_id}: ${inc.description || 'Reported Incident'}`;
      document.getElementById("incCaseMeta").textContent = `Location: ${inc.location_name} | Time: ${inc.timestamp}`;
      document.getElementById("incSeverity").textContent = `${inc.severity} SEVERITY`;

      tableBody.innerHTML = "";
      if (inc.nearby_people && inc.nearby_people.length > 0) {
        inc.nearby_people.forEach(p => {
          const tr = document.createElement("tr");
          tr.innerHTML = `
            <td><code>${p.person_id}</code></td>
            <td><strong>${p.full_name || 'Citizen'}</strong></td>
            <td>${p.occupation || 'N/A'}</td>
            <td><code>${p.timestamp}</code></td>
            <td>${p.location_name || inc.location_name}</td>
            <td><span class="tag-pill">${p.distance_meters} m</span></td>
            <td><button class="chip" onclick="setPerson('${p.person_id}')">Track Journey</button></td>
          `;
          tableBody.appendChild(tr);
        });
      } else {
        tableBody.innerHTML = `<tr><td colspan="7" class="text-center">No nearby people detected within search threshold.</td></tr>`;
      }
    }
  } catch (err) {
    console.error("Error loading incident:", err);
  }
}

// ==============================================================================
// Anomaly Intelligence
// ==============================================================================
let allAnomalies = [];

async function loadAnomalies() {
  try {
    const res = await fetch(`${API_BASE}/api/anomalies`);
    if (res.ok) {
      allAnomalies = await res.json();
      renderAnomalies(allAnomalies);
      // Update badge in overview
      document.getElementById("valAnomalies").textContent = allAnomalies.length;
    }
  } catch (err) {
    console.warn("Could not load anomalies:", err);
  }
}

function renderAnomalies(list) {
  const container = document.getElementById("anomaliesList");
  container.innerHTML = "";

  if (!list || list.length === 0) {
    container.innerHTML = `<div class="card-box text-center"><p class="text-muted">No anomalies detected yet. Run Spark anomaly analytics.</p></div>`;
    return;
  }

  list.forEach(a => {
    const isSpeed = a.type === "IMPOSSIBLE_MOVEMENT";
    const card = document.createElement("div");
    card.className = `anomaly-card ${a.severity === "CRITICAL" ? "critical" : ""}`;

    if (isSpeed) {
      card.innerHTML = `
        <div class="anomaly-header">
          <span class="anomaly-title">🚨 ${a.type}: Person ${a.person_id}</span>
          <span class="severity-badge high">${a.severity}</span>
        </div>
        <p class="anomaly-desc">${a.description}</p>
        <div class="anomaly-metrics-grid">
          <div class="anomaly-metric-item">Origin: <span>${a.origin_location} (${a.origin_time.slice(11)})</span></div>
          <div class="anomaly-metric-item">Destination: <span>${a.destination_location} (${a.destination_time.slice(11)})</span></div>
          <div class="anomaly-metric-item">Distance: <span>${a.distance_km} km</span></div>
          <div class="anomaly-metric-item">Velocity: <span>${a.calculated_speed_kmh} km/h</span></div>
        </div>
      `;
    } else {
      card.innerHTML = `
        <div class="anomaly-header">
          <span class="anomaly-title">⚡ ${a.type}: ${a.location_name}</span>
          <span class="severity-badge">${a.severity}</span>
        </div>
        <p class="anomaly-desc">${a.description}</p>
        <div class="anomaly-metrics-grid">
          <div class="anomaly-metric-item">Time Window: <span>${a.time_window}</span></div>
          <div class="anomaly-metric-item">Observed Volume: <span>${a.observed_events_per_hour} events/hr</span></div>
          <div class="anomaly-metric-item">Baseline Avg: <span>${a.baseline_avg_events} events/hr</span></div>
          <div class="anomaly-metric-item">Surge Factor: <span>${a.surge_ratio}</span></div>
        </div>
      `;
    }
    container.appendChild(card);
  });
}

window.filterAnomalies = function(type) {
  document.querySelectorAll(".btn-filter").forEach(b => b.classList.remove("active"));
  event.target.classList.add("active");

  if (!type) {
    renderAnomalies(allAnomalies);
  } else {
    renderAnomalies(allAnomalies.filter(a => a.type === type));
  }
};

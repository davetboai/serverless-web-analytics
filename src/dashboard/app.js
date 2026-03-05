(function () {
  "use strict";

  var BASE = window.location.origin;
  var PASSWORD = localStorage.getItem("analytics_pw") || "";
  var chart = null;

  var siteSelect = document.getElementById("site-select");
  var rangeSelect = document.getElementById("range-select");

  function headers() {
    var h = { "Content-Type": "application/json" };
    if (PASSWORD) h["Authorization"] = "Bearer " + PASSWORD;
    return h;
  }

  function api(path, params) {
    var qs = new URLSearchParams(params).toString();
    var url = BASE + path + (qs ? "?" + qs : "");
    return fetch(url, { headers: headers() }).then(function (r) {
      if (r.status === 401) {
        var pw = prompt("Dashboard password:");
        if (pw) {
          PASSWORD = pw;
          localStorage.setItem("analytics_pw", pw);
          return api(path, params);
        }
      }
      return r.json();
    });
  }

  function loadSites() {
    api("/api/sites", {}).then(function (data) {
      siteSelect.innerHTML = "";
      if (!data.sites || data.sites.length === 0) {
        siteSelect.innerHTML =
          '<option value="">No sites yet — add tracking script</option>';
        return;
      }
      data.sites.forEach(function (s) {
        var opt = document.createElement("option");
        opt.value = s.id;
        opt.textContent = s.domain || s.id;
        siteSelect.appendChild(opt);
      });
      loadStats();
    });
  }

  function loadStats() {
    var siteId = siteSelect.value;
    if (!siteId) return;
    var days = rangeSelect.value;

    api("/api/query", { site_id: siteId, days: days }).then(function (d) {
      // Summary
      document.getElementById("total-pageviews").textContent = fmt(
        d.totalPageviews
      );
      document.getElementById("total-visitors").textContent = fmt(
        d.totalVisitors
      );
      document.getElementById("pages-per-visitor").textContent =
        d.totalVisitors > 0
          ? (d.totalPageviews / d.totalVisitors).toFixed(1)
          : "—";

      // Chart
      renderChart(d.dates);

      // Tables
      renderTable(
        "top-pages",
        d.topPages.map(function (p) {
          return [p.path, p.count];
        })
      );
      renderTable(
        "top-referrers",
        d.topReferrers.map(function (r) {
          return [r.domain, r.count];
        })
      );
      renderTable(
        "countries",
        d.countries.map(function (c) {
          return [c.code, c.count];
        })
      );
      var devEntries = Object.entries(d.devices || {});
      renderTable("devices", devEntries);
    });
  }

  function fmt(n) {
    return (n || 0).toLocaleString();
  }

  function renderChart(dates) {
    var labels = dates.map(function (d) {
      return d.date.slice(5);
    }); // MM-DD
    var pvData = dates.map(function (d) {
      return d.pageviews;
    });
    var uvData = dates.map(function (d) {
      return d.visitors;
    });

    if (chart) chart.destroy();
    chart = new Chart(document.getElementById("traffic-chart"), {
      type: "line",
      data: {
        labels: labels,
        datasets: [
          {
            label: "Pageviews",
            data: pvData,
            borderColor: "#0ea5e9",
            backgroundColor: "rgba(14,165,233,0.1)",
            fill: true,
            tension: 0.3,
          },
          {
            label: "Visitors",
            data: uvData,
            borderColor: "#6366f1",
            backgroundColor: "rgba(99,102,241,0.1)",
            fill: true,
            tension: 0.3,
          },
        ],
      },
      options: {
        responsive: true,
        interaction: { intersect: false, mode: "index" },
        scales: {
          x: { ticks: { color: "#64748b" }, grid: { color: "#1e293b" } },
          y: {
            beginAtZero: true,
            ticks: { color: "#64748b" },
            grid: { color: "#1e293b" },
          },
        },
        plugins: { legend: { labels: { color: "#94a3b8" } } },
      },
    });
  }

  function renderTable(id, rows) {
    var tbody = document.querySelector("#" + id + " tbody");
    if (!rows.length) {
      tbody.innerHTML = '<tr><td class="empty" colspan="2">No data</td></tr>';
      return;
    }
    tbody.innerHTML = rows
      .map(function (r) {
        return "<tr><td>" + esc(String(r[0])) + "</td><td>" + fmt(r[1]) + "</td></tr>";
      })
      .join("");
  }

  function esc(s) {
    var d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  siteSelect.addEventListener("change", loadStats);
  rangeSelect.addEventListener("change", loadStats);

  loadSites();
})();

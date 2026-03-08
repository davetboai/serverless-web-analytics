import { useState, useEffect } from "react";
import { fetchAuthSession } from "aws-amplify/auth";
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend,
} from "chart.js";
import { Line } from "react-chartjs-2";

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
  Legend
);

interface SiteInfo {
  id: string;
  domain: string;
}
interface DateEntry {
  date: string;
  pageviews: number;
  visitors: number;
}
interface StatsData {
  totalPageviews: number;
  totalVisitors: number;
  totalSessions: number;
  bounceRate: number;
  avgDuration: number;
  dates: DateEntry[];
  topPages: { path: string; count: number }[];
  topReferrers: { domain: string; count: number }[];
  countries: { code: string; count: number }[];
  devices: Record<string, number>;
}

async function apiFetch(
  path: string,
  params: Record<string, string> = {},
  options?: { method?: string; body?: unknown }
) {
  const session = await fetchAuthSession();
  const token = session.tokens?.idToken?.toString() ?? "";
  const qs = new URLSearchParams(params).toString();
  const url = `${window.location.origin}${path}${qs ? "?" + qs : ""}`;
  const fetchOpts: RequestInit = {
    method: options?.method || "GET",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
  };
  if (options?.body) fetchOpts.body = JSON.stringify(options.body);
  const res = await fetch(url, fetchOpts);
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

function fmt(n: number) {
  return (n || 0).toLocaleString();
}

function fmtDuration(seconds: number) {
  if (!seconds) return "0s";
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

function countryFlag(code: string) {
  if (!code || code === "XX" || code.length !== 2) return "";
  const offset = 0x1f1e6;
  const a = code.toUpperCase().charCodeAt(0) - 65 + offset;
  const b = code.toUpperCase().charCodeAt(1) - 65 + offset;
  return String.fromCodePoint(a, b) + " ";
}

function exportCsv(stats: StatsData) {
  const rows = [
    ["Date", "Pageviews", "Visitors"],
    ...stats.dates.map((d) => [d.date, d.pageviews, d.visitors]),
  ];
  const csv = rows.map((r) => r.join(",")).join("\n");
  const blob = new Blob([csv], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "analytics.csv";
  a.click();
  URL.revokeObjectURL(url);
}

export default function Dashboard() {
  const [sites, setSites] = useState<SiteInfo[]>([]);
  const [siteId, setSiteId] = useState("");
  const [days, setDays] = useState("30");
  const [stats, setStats] = useState<StatsData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [liveCount, setLiveCount] = useState<number | null>(null);
  const [showManage, setShowManage] = useState(false);
  const [newSiteId, setNewSiteId] = useState("");
  const [newSiteDomain, setNewSiteDomain] = useState("");

  const refreshSites = () =>
    apiFetch("/api/sites")
      .then((data) => {
        setSites(data.sites || []);
        if (data.sites?.length && !data.sites.find((s: SiteInfo) => s.id === siteId)) {
          setSiteId(data.sites[0].id);
        }
      })
      .catch((e) => setError(e.message));

  useEffect(() => {
    refreshSites();
  }, []);

  useEffect(() => {
    if (!siteId) return;
    setLoading(true);
    setError("");
    apiFetch("/api/query", { site_id: siteId, days })
      .then((data) => {
        setStats(data);
        setLoading(false);
      })
      .catch((e) => {
        setError(e.message);
        setLoading(false);
      });
  }, [siteId, days]);

  useEffect(() => {
    if (!siteId) return;
    const fetchLive = () =>
      apiFetch("/api/live", { site_id: siteId })
        .then((data) => setLiveCount(data.liveVisitors ?? null))
        .catch(() => {});
    fetchLive();
    const interval = setInterval(fetchLive, 30000);
    return () => clearInterval(interval);
  }, [siteId]);

  const chartData = stats
    ? {
        labels: stats.dates.map((d) => d.date.slice(5)),
        datasets: [
          {
            label: "Pageviews",
            data: stats.dates.map((d) => d.pageviews),
            borderColor: "#0ea5e9",
            backgroundColor: "rgba(14,165,233,0.1)",
            fill: true,
            tension: 0.3,
          },
          {
            label: "Visitors",
            data: stats.dates.map((d) => d.visitors),
            borderColor: "#6366f1",
            backgroundColor: "rgba(99,102,241,0.1)",
            fill: true,
            tension: 0.3,
          },
        ],
      }
    : null;

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: true,
    interaction: { intersect: false as const, mode: "index" as const },
    scales: {
      x: { ticks: { color: "#64748b" }, grid: { color: "#1e293b" } },
      y: {
        beginAtZero: true,
        ticks: { color: "#64748b" },
        grid: { color: "#1e293b" },
      },
    },
    plugins: { legend: { labels: { color: "#94a3b8" } } },
  };

  return (
    <>
      <div className="controls">
        <select value={siteId} onChange={(e) => setSiteId(e.target.value)}>
          {sites.length === 0 && (
            <option value="">No sites yet — add tracking script</option>
          )}
          {sites.map((s) => (
            <option key={s.id} value={s.id}>
              {s.domain || s.id}
            </option>
          ))}
        </select>
        <select value={days} onChange={(e) => setDays(e.target.value)}>
          <option value="7">Last 7 days</option>
          <option value="30">Last 30 days</option>
          <option value="90">Last 90 days</option>
        </select>
        {stats && (
          <button className="btn-export" onClick={() => exportCsv(stats)}>
            Export CSV
          </button>
        )}
        <button
          className="btn-export"
          onClick={() => setShowManage(!showManage)}
        >
          {showManage ? "Close" : "Manage Sites"}
        </button>
      </div>

      {showManage && (
        <div className="manage-sites">
          <div className="manage-add">
            <input
              placeholder="Site ID"
              value={newSiteId}
              onChange={(e) => setNewSiteId(e.target.value)}
            />
            <input
              placeholder="Domain (optional)"
              value={newSiteDomain}
              onChange={(e) => setNewSiteDomain(e.target.value)}
            />
            <button
              className="btn-export"
              onClick={() => {
                if (!newSiteId.trim()) return;
                apiFetch("/api/sites", {}, {
                  method: "POST",
                  body: { id: newSiteId.trim(), domain: newSiteDomain.trim() || newSiteId.trim() },
                })
                  .then(() => {
                    setNewSiteId("");
                    setNewSiteDomain("");
                    refreshSites();
                  })
                  .catch((e) => setError(e.message));
              }}
            >
              Add Site
            </button>
          </div>
          <div className="manage-list">
            {sites.map((s) => (
              <div key={s.id} className="manage-row">
                <span className="manage-id">{s.id}</span>
                <span className="manage-domain">{s.domain}</span>
                <button
                  className="btn-danger"
                  onClick={() => {
                    if (!confirm(`Delete site "${s.id}"? This removes the site entry but not its analytics data.`))
                      return;
                    apiFetch("/api/sites", {}, {
                      method: "DELETE",
                      body: { id: s.id },
                    })
                      .then(() => refreshSites())
                      .catch((e) => setError(e.message));
                  }}
                >
                  Delete
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {error && <div className="error">{error}</div>}
      {loading && <div className="loading">Loading...</div>}

      {stats && (
        <>
          {liveCount !== null && (
            <div className="live-indicator">
              <span className="live-dot" /> {fmt(liveCount)} currently online
            </div>
          )}

          <div className="stats-grid">
            <div className="stat-card">
              <div className="stat-label">Pageviews</div>
              <div className="stat-value">{fmt(stats.totalPageviews)}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Unique Visitors</div>
              <div className="stat-value">{fmt(stats.totalVisitors)}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Bounce Rate</div>
              <div className="stat-value">{stats.bounceRate}%</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Avg. Duration</div>
              <div className="stat-value">{fmtDuration(stats.avgDuration)}</div>
            </div>
            <div className="stat-card">
              <div className="stat-label">Pages / Session</div>
              <div className="stat-value">
                {stats.totalSessions > 0
                  ? (stats.totalPageviews / stats.totalSessions).toFixed(1)
                  : "\u2014"}
              </div>
            </div>
          </div>

          <div className="chart-container">
            {chartData && <Line data={chartData} options={chartOptions} />}
          </div>

          <div className="tables-grid">
            <BarTableCard
              title="Top Pages"
              rows={stats.topPages.map((p) => [p.path, p.count])}
            />
            <BarTableCard
              title="Referrers"
              rows={stats.topReferrers.map((r) => [r.domain, r.count])}
            />
            <BarTableCard
              title="Countries"
              rows={stats.countries.map((c) => [
                countryFlag(c.code) + c.code,
                c.count,
              ])}
            />
            <BarTableCard
              title="Devices"
              rows={Object.entries(stats.devices || {}).sort(
                (a, b) => (b[1] as number) - (a[1] as number)
              )}
            />
          </div>
        </>
      )}
    </>
  );
}

function BarTableCard({
  title,
  rows,
}: {
  title: string;
  rows: [string, number][];
}) {
  const max = rows.length > 0 ? Math.max(...rows.map(([, c]) => c)) : 1;
  return (
    <div className="table-card">
      <h3>{title}</h3>
      {rows.length === 0 ? (
        <div className="empty">No data</div>
      ) : (
        <table>
          <tbody>
            {rows.map(([label, count], i) => (
              <tr key={i}>
                <td className="bar-cell">
                  <div
                    className="bar-bg"
                    style={{ width: `${(count / max) * 100}%` }}
                  />
                  <span className="bar-label">{label}</span>
                </td>
                <td>{fmt(count)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

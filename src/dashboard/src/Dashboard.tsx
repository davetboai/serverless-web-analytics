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
  ttl_days?: number;
  api_key?: string;
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
  browsers: { name: string; count: number }[];
  oses: { name: string; count: number }[];
  languages: { code: string; count: number }[];
  utmSources: { name: string; count: number }[];
  utmMediums: { name: string; count: number }[];
  utmCampaigns: { name: string; count: number }[];
  entryPages: { path: string; count: number }[];
  exitPages: { path: string; count: number }[];
  channels: { name: string; count: number }[];
}
interface EventsData {
  totalEvents: number;
  uniqueVisitors: number;
  events: { name: string; count: number }[];
}
interface RecentEntry {
  time: string;
  path: string;
  country: string;
  device: string;
  browser: string;
  referrer: string;
}
interface GoalData {
  id: string;
  name: string;
  type: string;
  value: string;
  completions: number;
  conversionRate: number;
}
interface GoalsResponse {
  goals: GoalData[];
  totalVisitors: number;
}
interface FunnelStep {
  label: string;
  type: string;
  value: string;
  visitors: number;
  dropOff: number;
}
interface FunnelData {
  id: string;
  name: string;
  steps: FunnelStep[];
  conversionRate: number;
}
interface FunnelsResponse {
  funnels: FunnelData[];
}
interface PerfData {
  sampleCount: number;
  avgLoad: number;
  avgTtfb: number;
  avgDom: number;
  p75Load: number;
  p75Ttfb: number;
  byPage: { path: string; count: number; avgLoad: number }[];
}
interface CompareData {
  current: { pageviews: number; visitors: number };
  previous: { pageviews: number; visitors: number };
  change: { pageviews: number; visitors: number };
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

function fmtMs(ms: number) {
  if (!ms) return "0ms";
  if (ms >= 1000) return `${(ms / 1000).toFixed(1)}s`;
  return `${ms}ms`;
}

function ChangeIndicator({ value }: { value: number }) {
  if (value === 0) return null;
  const color = value > 0 ? "#22c55e" : "#ef4444";
  const arrow = value > 0 ? "\u2191" : "\u2193";
  return <div className="change-indicator" style={{ color }}>{arrow} {Math.abs(value)}%</div>;
}

function countryFlag(code: string) {
  if (!code || code === "XX" || code.length !== 2) return "";
  const offset = 0x1f1e6;
  const a = code.toUpperCase().charCodeAt(0) - 65 + offset;
  const b = code.toUpperCase().charCodeAt(1) - 65 + offset;
  return String.fromCodePoint(a, b) + " ";
}

function downloadCsv(rows: (string | number)[][], filename: string) {
  const csv = rows.map((r) => r.join(",")).join("\n");
  const blob = new Blob([csv], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

function exportCsv(stats: StatsData) {
  downloadCsv(
    [["Date", "Pageviews", "Visitors"], ...stats.dates.map((d) => [d.date, d.pageviews, d.visitors])],
    "analytics.csv"
  );
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
  const [eventsData, setEventsData] = useState<EventsData | null>(null);
  const [recentData, setRecentData] = useState<RecentEntry[]>([]);
  const [goalsData, setGoalsData] = useState<GoalsResponse | null>(null);
  const [showGoalForm, setShowGoalForm] = useState(false);
  const [newGoalName, setNewGoalName] = useState("");
  const [newGoalType, setNewGoalType] = useState<"page" | "event">("page");
  const [newGoalValue, setNewGoalValue] = useState("");
  const [activeTab, setActiveTab] = useState<"overview" | "events" | "realtime" | "performance" | "funnels">("overview");
  const [perfData, setPerfData] = useState<PerfData | null>(null);
  const [compareData, setCompareData] = useState<CompareData | null>(null);
  const [funnelsData, setFunnelsData] = useState<FunnelsResponse | null>(null);
  const [showFunnelForm, setShowFunnelForm] = useState(false);
  const [newFunnelName, setNewFunnelName] = useState("");
  const [newFunnelSteps, setNewFunnelSteps] = useState<{type: string; value: string; label: string}[]>([
    {type: "page", value: "", label: ""}, {type: "page", value: "", label: ""},
  ]);

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

  const refreshGoals = () => {
    if (!siteId) return;
    apiFetch("/api/goals", { site_id: siteId, days })
      .then((data) => setGoalsData(data))
      .catch(() => {});
  };

  useEffect(() => {
    if (!siteId || activeTab !== "events") return;
    apiFetch("/api/events", { site_id: siteId, days })
      .then((data) => setEventsData(data))
      .catch(() => {});
    refreshGoals();
  }, [siteId, days, activeTab]);

  useEffect(() => {
    if (!siteId) return;
    apiFetch("/api/compare", { site_id: siteId, days })
      .then((data) => setCompareData(data))
      .catch(() => setCompareData(null));
  }, [siteId, days]);

  const refreshFunnels = () => {
    if (!siteId) return;
    apiFetch("/api/funnels", { site_id: siteId, days })
      .then((data) => setFunnelsData(data))
      .catch(() => {});
  };

  useEffect(() => {
    if (!siteId || activeTab !== "funnels") return;
    refreshFunnels();
  }, [siteId, days, activeTab]);

  useEffect(() => {
    if (!siteId || activeTab !== "performance") return;
    apiFetch("/api/perf", { site_id: siteId, days })
      .then((data) => setPerfData(data))
      .catch(() => {});
  }, [siteId, days, activeTab]);

  useEffect(() => {
    if (!siteId || activeTab !== "realtime") return;
    const fetchRecent = () =>
      apiFetch("/api/recent", { site_id: siteId })
        .then((data) => setRecentData(data.recent || []))
        .catch(() => {});
    fetchRecent();
    const interval = setInterval(fetchRecent, 10000);
    return () => clearInterval(interval);
  }, [siteId, activeTab]);

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
                {s.api_key && (
                  <span className="manage-api-key" title="API key for server-side events">
                    <code>{s.api_key.slice(0, 8)}...</code>
                    <button className="btn-table-export" onClick={() => { navigator.clipboard.writeText(s.api_key || ""); }}>Copy</button>
                  </span>
                )}
                <label className="manage-ttl" title="Data retention in days (30-730)">
                  TTL
                  <input
                    type="number"
                    min={30}
                    max={730}
                    defaultValue={s.ttl_days || 395}
                    className="ttl-input"
                    onBlur={(e) => {
                      const val = parseInt(e.target.value, 10);
                      if (isNaN(val)) return;
                      apiFetch("/api/sites", {}, {
                        method: "PUT",
                        body: { id: s.id, domain: s.domain, ttl_days: val },
                      }).then(() => refreshSites()).catch((err) => setError(err.message));
                    }}
                  />d
                </label>
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

      {liveCount !== null && (
        <div className="live-indicator">
          <span className="live-dot" /> {fmt(liveCount)} currently online
        </div>
      )}

      <div className="tabs">
        {(["overview", "events", "funnels", "performance", "realtime"] as const).map((tab) => (
          <button
            key={tab}
            className={`tab ${activeTab === tab ? "tab-active" : ""}`}
            onClick={() => setActiveTab(tab)}
          >
            {{overview: "Overview", events: "Events", funnels: "Funnels", performance: "Performance", realtime: "Real-time"}[tab]}
          </button>
        ))}
      </div>

      {activeTab === "overview" && stats && (
        <>
          <div className="stats-grid">
            <div className="stat-card">
              <div className="stat-label">Pageviews</div>
              <div className="stat-value">{fmt(stats.totalPageviews)}</div>
              {compareData && <ChangeIndicator value={compareData.change.pageviews} />}
            </div>
            <div className="stat-card">
              <div className="stat-label">Unique Visitors</div>
              <div className="stat-value">{fmt(stats.totalVisitors)}</div>
              {compareData && <ChangeIndicator value={compareData.change.visitors} />}
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
              onExport={() => downloadCsv(
                [["Path", "Count"], ...stats.topPages.map((p) => [p.path, p.count])],
                "top-pages.csv"
              )}
            />
            <BarTableCard
              title="Referrers"
              rows={stats.topReferrers.map((r) => [r.domain, r.count])}
              onExport={() => downloadCsv(
                [["Referrer", "Count"], ...stats.topReferrers.map((r) => [r.domain, r.count])],
                "referrers.csv"
              )}
            />
            <BarTableCard
              title="Channels"
              rows={(stats.channels || []).map((c) => [c.name, c.count])}
              onExport={() => downloadCsv(
                [["Channel", "Count"], ...(stats.channels || []).map((c) => [c.name, c.count])],
                "channels.csv"
              )}
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
            <BarTableCard
              title="Browsers"
              rows={(stats.browsers || []).map((b) => [b.name, b.count])}
            />
            <BarTableCard
              title="Operating Systems"
              rows={(stats.oses || []).map((o) => [o.name, o.count])}
            />
            <BarTableCard
              title="Languages"
              rows={(stats.languages || []).map((l) => [l.code, l.count])}
            />
            <BarTableCard
              title="Entry Pages"
              rows={(stats.entryPages || []).map((p) => [p.path, p.count])}
            />
            <BarTableCard
              title="Exit Pages"
              rows={(stats.exitPages || []).map((p) => [p.path, p.count])}
            />
          </div>

          {((stats.utmSources || []).length > 0 ||
            (stats.utmMediums || []).length > 0 ||
            (stats.utmCampaigns || []).length > 0) && (
            <>
              <h2 className="section-title">Campaigns</h2>
              <div className="tables-grid">
                <BarTableCard
                  title="UTM Sources"
                  rows={(stats.utmSources || []).map((s) => [s.name, s.count])}
                />
                <BarTableCard
                  title="UTM Mediums"
                  rows={(stats.utmMediums || []).map((m) => [m.name, m.count])}
                />
                <BarTableCard
                  title="UTM Campaigns"
                  rows={(stats.utmCampaigns || []).map((c) => [c.name, c.count])}
                />
              </div>
            </>
          )}
        </>
      )}

      {activeTab === "events" && (
        <div className="events-panel">
          {eventsData ? (
            <>
              <div className="stats-grid">
                <div className="stat-card">
                  <div className="stat-label">Total Events</div>
                  <div className="stat-value">{fmt(eventsData.totalEvents)}</div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">Unique Visitors</div>
                  <div className="stat-value">{fmt(eventsData.uniqueVisitors)}</div>
                </div>
              </div>
              <div className="tables-grid">
                <BarTableCard
                  title="Custom Events"
                  rows={eventsData.events.map((e) => [e.name, e.count])}
                  onExport={() => downloadCsv(
                    [["Event", "Count"], ...eventsData.events.map((e) => [e.name, e.count])],
                    "events.csv"
                  )}
                />
              </div>
            </>
          ) : (
            <div className="empty">No event data yet. Track events with: window.sa.event("name", &#123;props&#125;)</div>
          )}

          <h2 className="section-title">Goals & Conversions</h2>
          <button className="btn-export" onClick={() => setShowGoalForm(!showGoalForm)} style={{marginBottom: 12}}>
            {showGoalForm ? "Cancel" : "Add Goal"}
          </button>
          {showGoalForm && (
            <div className="manage-sites" style={{marginBottom: 16}}>
              <div className="manage-add">
                <input placeholder="Goal name" value={newGoalName} onChange={(e) => setNewGoalName(e.target.value)} />
                <select value={newGoalType} onChange={(e) => setNewGoalType(e.target.value as "page" | "event")}
                  style={{flex: "none", width: "auto"}}>
                  <option value="page">Page visit</option>
                  <option value="event">Custom event</option>
                </select>
                <input placeholder={newGoalType === "page" ? "/signup" : "signup"} value={newGoalValue}
                  onChange={(e) => setNewGoalValue(e.target.value)} />
                <button className="btn-export" onClick={() => {
                  if (!newGoalName.trim() || !newGoalValue.trim()) return;
                  apiFetch("/api/goals", {}, {
                    method: "POST",
                    body: { site_id: siteId, name: newGoalName.trim(), type: newGoalType, value: newGoalValue.trim() },
                  }).then(() => {
                    setNewGoalName(""); setNewGoalValue(""); setShowGoalForm(false);
                    refreshGoals();
                  }).catch((e) => setError(e.message));
                }}>Create</button>
              </div>
            </div>
          )}
          {goalsData && goalsData.goals.length > 0 ? (
            <div className="table-card">
              <table>
                <thead>
                  <tr>
                    <th>Goal</th>
                    <th>Type</th>
                    <th>Completions</th>
                    <th>Conv. Rate</th>
                    <th></th>
                  </tr>
                </thead>
                <tbody>
                  {goalsData.goals.map((g) => (
                    <tr key={g.id}>
                      <td>{g.name}</td>
                      <td>{g.type === "page" ? "Page" : "Event"}: {g.value}</td>
                      <td>{fmt(g.completions)}</td>
                      <td style={{fontWeight: 600, color: "#22c55e"}}>{g.conversionRate}%</td>
                      <td>
                        <button className="btn-danger" onClick={() => {
                          apiFetch("/api/goals", {}, {
                            method: "DELETE",
                            body: { site_id: siteId, id: g.id },
                          }).then(() => refreshGoals()).catch((e) => setError(e.message));
                        }}>Delete</button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="empty">No goals defined yet</div>
          )}
        </div>
      )}

      {activeTab === "funnels" && (
        <div className="funnels-panel">
          <button className="btn-export" onClick={() => setShowFunnelForm(!showFunnelForm)} style={{marginBottom: 12}}>
            {showFunnelForm ? "Cancel" : "Create Funnel"}
          </button>
          {showFunnelForm && (
            <div className="manage-sites" style={{marginBottom: 16}}>
              <div className="manage-add" style={{flexDirection: "column", gap: 8}}>
                <input placeholder="Funnel name" value={newFunnelName} onChange={(e) => setNewFunnelName(e.target.value)} />
                {newFunnelSteps.map((step, i) => (
                  <div key={i} style={{display: "flex", gap: 8, alignItems: "center"}}>
                    <span style={{color: "#64748b", fontSize: 12, minWidth: 50}}>Step {i + 1}</span>
                    <select value={step.type} onChange={(e) => {
                      const s = [...newFunnelSteps]; s[i] = {...s[i], type: e.target.value}; setNewFunnelSteps(s);
                    }} style={{flex: "none", width: "auto", background: "#0f172a", color: "#e2e8f0", border: "1px solid #334155", borderRadius: 6, padding: "6px 8px", fontSize: 13}}>
                      <option value="page">Page</option>
                      <option value="event">Event</option>
                    </select>
                    <input placeholder={step.type === "page" ? "/signup" : "purchase"} value={step.value}
                      onChange={(e) => { const s = [...newFunnelSteps]; s[i] = {...s[i], value: e.target.value}; setNewFunnelSteps(s); }} />
                    <input placeholder="Label (optional)" value={step.label}
                      onChange={(e) => { const s = [...newFunnelSteps]; s[i] = {...s[i], label: e.target.value}; setNewFunnelSteps(s); }} />
                    {newFunnelSteps.length > 2 && (
                      <button className="btn-danger" onClick={() => setNewFunnelSteps(newFunnelSteps.filter((_, j) => j !== i))}>X</button>
                    )}
                  </div>
                ))}
                <div style={{display: "flex", gap: 8}}>
                  <button className="btn-export" onClick={() => setNewFunnelSteps([...newFunnelSteps, {type: "page", value: "", label: ""}])}>+ Step</button>
                  <button className="btn-export" onClick={() => {
                    if (!newFunnelName.trim() || newFunnelSteps.some(s => !s.value.trim())) return;
                    apiFetch("/api/funnels", {}, {
                      method: "POST",
                      body: { site_id: siteId, name: newFunnelName.trim(), steps: newFunnelSteps.map(s => ({...s, label: s.label || s.value})) },
                    }).then(() => {
                      setNewFunnelName(""); setNewFunnelSteps([{type: "page", value: "", label: ""}, {type: "page", value: "", label: ""}]);
                      setShowFunnelForm(false); refreshFunnels();
                    }).catch((e) => setError(e.message));
                  }}>Create</button>
                </div>
              </div>
            </div>
          )}
          {funnelsData && funnelsData.funnels.length > 0 ? (
            funnelsData.funnels.map((f) => (
              <div key={f.id} className="table-card" style={{marginBottom: 16}}>
                <div className="table-card-header">
                  <h3>{f.name} <span style={{fontWeight: 400, color: "#64748b"}}>({f.conversionRate}% conversion)</span></h3>
                  <button className="btn-danger" onClick={() => {
                    apiFetch("/api/funnels", {}, { method: "DELETE", body: { site_id: siteId, id: f.id } })
                      .then(() => refreshFunnels()).catch((e) => setError(e.message));
                  }}>Delete</button>
                </div>
                <div className="funnel-steps">
                  {f.steps.map((step, i) => (
                    <div key={i} className="funnel-step">
                      <div className="funnel-bar" style={{width: `${f.steps[0].visitors > 0 ? (step.visitors / f.steps[0].visitors) * 100 : 0}%`}} />
                      <div className="funnel-step-content">
                        <span className="funnel-step-label">{step.label}</span>
                        <span className="funnel-step-count">{fmt(step.visitors)}</span>
                        {i > 0 && step.dropOff > 0 && <span className="funnel-drop">-{step.dropOff}%</span>}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ))
          ) : (
            <div className="empty">No funnels defined yet. Create one to track multi-step conversion paths.</div>
          )}
        </div>
      )}

      {activeTab === "performance" && (
        <div className="perf-panel">
          {perfData && perfData.sampleCount > 0 ? (
            <>
              <div className="stats-grid">
                <div className="stat-card">
                  <div className="stat-label">Avg Load Time</div>
                  <div className="stat-value">{fmtMs(perfData.avgLoad)}</div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">Avg TTFB</div>
                  <div className="stat-value">{fmtMs(perfData.avgTtfb)}</div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">Avg DOM Ready</div>
                  <div className="stat-value">{fmtMs(perfData.avgDom)}</div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">P75 Load</div>
                  <div className="stat-value">{fmtMs(perfData.p75Load)}</div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">P75 TTFB</div>
                  <div className="stat-value">{fmtMs(perfData.p75Ttfb)}</div>
                </div>
                <div className="stat-card">
                  <div className="stat-label">Samples</div>
                  <div className="stat-value">{fmt(perfData.sampleCount)}</div>
                </div>
              </div>
              {perfData.byPage.length > 0 && (
                <div className="tables-grid">
                  <div className="table-card">
                    <h3>Load Time by Page</h3>
                    <table>
                      <thead>
                        <tr>
                          <th>Page</th>
                          <th>Samples</th>
                          <th>Avg Load</th>
                        </tr>
                      </thead>
                      <tbody>
                        {perfData.byPage.map((p, i) => (
                          <tr key={i}>
                            <td>{p.path}</td>
                            <td>{p.count}</td>
                            <td>{fmtMs(p.avgLoad)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
          ) : (
            <div className="empty">No performance data yet. The tracker automatically collects page load timing.</div>
          )}
        </div>
      )}

      {activeTab === "realtime" && (
        <div className="realtime-panel">
          <h2 className="section-title">Last 30 Minutes</h2>
          {recentData.length === 0 ? (
            <div className="empty">No recent pageviews</div>
          ) : (
            <div className="table-card">
              <table>
                <thead>
                  <tr>
                    <th>Time</th>
                    <th>Page</th>
                    <th>Country</th>
                    <th>Browser</th>
                    <th>Referrer</th>
                  </tr>
                </thead>
                <tbody>
                  {recentData.map((r, i) => (
                    <tr key={i}>
                      <td className="nowrap">{r.time.split("T")[1]?.split(".")[0] || r.time}</td>
                      <td>{r.path}</td>
                      <td>{countryFlag(r.country)}{r.country}</td>
                      <td>{r.browser}</td>
                      <td>{r.referrer || "Direct"}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      )}
    </>
  );
}

function BarTableCard({
  title,
  rows,
  onExport,
}: {
  title: string;
  rows: [string, number][];
  onExport?: () => void;
}) {
  const max = rows.length > 0 ? Math.max(...rows.map(([, c]) => c)) : 1;
  return (
    <div className="table-card">
      <div className="table-card-header">
        <h3>{title}</h3>
        {onExport && rows.length > 0 && (
          <button className="btn-table-export" onClick={onExport} title="Export CSV">CSV</button>
        )}
      </div>
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

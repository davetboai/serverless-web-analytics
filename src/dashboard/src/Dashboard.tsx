import { useState, useEffect, useRef } from "react";
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
  dates: DateEntry[];
  topPages: { path: string; count: number }[];
  topReferrers: { domain: string; count: number }[];
  countries: { code: string; count: number }[];
  devices: Record<string, number>;
}

async function apiFetch(path: string, params: Record<string, string> = {}) {
  const session = await fetchAuthSession();
  const token = session.tokens?.idToken?.toString() ?? "";
  const qs = new URLSearchParams(params).toString();
  const url = `${window.location.origin}${path}${qs ? "?" + qs : ""}`;
  const res = await fetch(url, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.json();
}

function fmt(n: number) {
  return (n || 0).toLocaleString();
}

export default function Dashboard() {
  const [sites, setSites] = useState<SiteInfo[]>([]);
  const [siteId, setSiteId] = useState("");
  const [days, setDays] = useState("30");
  const [stats, setStats] = useState<StatsData | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    apiFetch("/api/sites").then((data) => {
      setSites(data.sites || []);
      if (data.sites?.length) setSiteId(data.sites[0].id);
    });
  }, []);

  useEffect(() => {
    if (!siteId) return;
    setLoading(true);
    apiFetch("/api/query", { site_id: siteId, days }).then((data) => {
      setStats(data);
      setLoading(false);
    });
  }, [siteId, days]);

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
      </div>

      {loading && <div className="loading">Loading...</div>}

      {stats && (
        <>
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
              <div className="stat-label">Pages / Visitor</div>
              <div className="stat-value">
                {stats.totalVisitors > 0
                  ? (stats.totalPageviews / stats.totalVisitors).toFixed(1)
                  : "—"}
              </div>
            </div>
          </div>

          <div className="chart-container">
            {chartData && <Line data={chartData} options={chartOptions} />}
          </div>

          <div className="tables-grid">
            <TableCard
              title="Top Pages"
              rows={stats.topPages.map((p) => [p.path, p.count])}
            />
            <TableCard
              title="Referrers"
              rows={stats.topReferrers.map((r) => [r.domain, r.count])}
            />
            <TableCard
              title="Countries"
              rows={stats.countries.map((c) => [c.code, c.count])}
            />
            <TableCard
              title="Devices"
              rows={Object.entries(stats.devices || {})}
            />
          </div>
        </>
      )}
    </>
  );
}

function TableCard({ title, rows }: { title: string; rows: [string, number][] }) {
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
                <td>{label}</td>
                <td>{fmt(count)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}

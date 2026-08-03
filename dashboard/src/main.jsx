import React, { useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import {
  Activity,
  AlertTriangle,
  ArrowUpRight,
  CheckCircle2,
  Clock3,
  Database,
  Gauge,
  Layers3,
  Radio,
  RefreshCw,
  Server,
  ShieldCheck,
  Wifi,
  Zap,
} from 'lucide-react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Filler,
  Tooltip,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import './styles.css';

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Filler, Tooltip);

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

function formatNumber(value, digits = 0) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '0';
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: digits });
}

function formatTime(value) {
  if (!value) return 'Never';
  return new Intl.DateTimeFormat(undefined, {
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
  }).format(new Date(value));
}

async function fetchJson(path) {
  const response = await fetch(`${API_URL}${path}`);
  if (!response.ok) throw new Error(`${path} returned ${response.status}`);
  return response.json();
}

function MetricCard({ icon: Icon, label, value, detail, tone = 'blue' }) {
  return (
    <article className="metric-card">
      <div className={`metric-icon ${tone}`}><Icon size={18} /></div>
      <div className="metric-copy">
        <span>{label}</span>
        <strong>{value}</strong>
        <small>{detail}</small>
      </div>
    </article>
  );
}

function EmptyState({ children }) {
  return <div className="empty-state">{children}</div>;
}

function App() {
  const [devices, setDevices] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [latest, setLatest] = useState([]);
  const [summaries, setSummaries] = useState([]);
  const [pipeline, setPipeline] = useState({});
  const [health, setHealth] = useState({ status: 'loading', dependencies: {} });
  const [error, setError] = useState('');
  const [updatedAt, setUpdatedAt] = useState(null);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshToken, setRefreshToken] = useState(0);

  useEffect(() => {
    let active = true;

    async function load() {
      setRefreshing(true);
      try {
        const [deviceRows, alertRows, pipelineRows, healthRows] = await Promise.all([
          fetchJson('/devices'),
          fetchJson('/alerts?limit=25'),
          fetchJson('/pipeline/stats'),
          fetchJson('/health'),
        ]);
        const regions = [...new Set(deviceRows.map((device) => device.region))].slice(0, 6);
        const [summaryRows, latestRows] = await Promise.all([
          Promise.all(regions.map((region) => fetchJson(`/regions/${region}/summary`))),
          Promise.all(
            deviceRows.slice(0, 12).map((device) =>
              fetchJson(`/devices/${device.device_id}/latest`).catch(() => null),
            ),
          ),
        ]);
        if (active) {
          setDevices(deviceRows);
          setAlerts(alertRows);
          setPipeline(pipelineRows);
          setHealth(healthRows);
          setSummaries(summaryRows);
          setLatest(latestRows.filter(Boolean));
          setUpdatedAt(new Date());
          setError('');
        }
      } catch (err) {
        if (active) setError(err.message);
      } finally {
        if (active) setRefreshing(false);
      }
    }

    load();
    const id = setInterval(load, 10000);
    return () => {
      active = false;
      clearInterval(id);
    };
  }, [refreshToken]);

  const totals = useMemo(() => {
    const eventCount = summaries.reduce((sum, row) => sum + Number(row.event_count || 0), 0);
    const lagRows = summaries.filter((row) => row.avg_event_lag_ms !== null);
    const avgLag = lagRows.reduce((sum, row) => sum + Number(row.avg_event_lag_ms || 0), 0) /
      Math.max(lagRows.length, 1);
    const unhealthy = devices.filter((device) => device.status !== 'OK').length;
    const activeRegions = new Set(devices.map((device) => device.region)).size;
    return { eventCount, avgLag, unhealthy, activeRegions };
  }, [devices, summaries]);

  const chartData = useMemo(() => ({
    labels: latest.map((row) => row.device_id.replace('sensor-', '#')),
    datasets: [
      {
        label: 'Temperature',
        data: latest.map((row) => row.temperature),
        borderColor: '#3b82f6',
        backgroundColor: 'rgba(59, 130, 246, 0.13)',
        pointBackgroundColor: '#60a5fa',
        pointBorderColor: '#0b1220',
        pointRadius: 3,
        pointHoverRadius: 5,
        borderWidth: 2,
        fill: true,
        tension: 0.32,
      },
      {
        label: '5m average',
        data: latest.map((row) => row.avg_temperature_5m),
        borderColor: '#2dd4bf',
        pointRadius: 0,
        borderWidth: 1.5,
        borderDash: [5, 5],
        fill: false,
        tension: 0.32,
      },
    ],
  }), [latest]);

  const chartOptions = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    resizeDelay: 250,
    animation: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: '#111c2f',
        borderColor: '#253450',
        borderWidth: 1,
        titleColor: '#f8fafc',
        bodyColor: '#cbd5e1',
        padding: 10,
      },
    },
    scales: {
      x: {
        grid: { display: false },
        ticks: { color: '#7f8da5', maxRotation: 0, autoSkip: true, maxTicksLimit: 8 },
        border: { display: false },
      },
      y: {
        grid: { color: 'rgba(148, 163, 184, 0.10)' },
        ticks: { color: '#7f8da5', callback: (value) => `${value}°` },
        border: { display: false },
      },
    },
  }), []);

  const healthyDependencies = Object.values(health.dependencies || {}).filter(Boolean).length;
  const totalDependencies = Object.keys(health.dependencies || {}).length;

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand"><span className="brand-mark"><Zap size={18} /></span><b>Metringest</b></div>
        <nav aria-label="Dashboard sections">
          <a className="active" href="#overview"><Gauge size={17} />Overview</a>
          <a href="#telemetry"><Activity size={17} />Telemetry</a>
          <a href="#incidents"><AlertTriangle size={17} />Incidents</a>
          <a href="http://localhost:9090" target="_blank" rel="noreferrer"><Radio size={17} />Prometheus</a>
        </nav>
        <div className="sidebar-foot">
          <ShieldCheck size={17} />
          <div><b>SLO protected</b><span>p99 persistence &lt; 5s</span></div>
        </div>
      </aside>

      <main id="overview">
        <header className="topbar">
          <div>
            <p className="eyebrow">Telemetry pipeline</p>
            <h1>Operations overview</h1>
            <p className="subtitle">Ingestion, processing, and delivery health in one view.</p>
          </div>
          <div className="header-actions">
            <div className={`health-chip ${health.status === 'ready' && !error ? 'healthy' : 'degraded'}`}>
              <span />{error ? 'Connection error' : health.status === 'ready' ? 'All systems operational' : 'Degraded'}
            </div>
            <button className="refresh-button" onClick={() => setRefreshToken((value) => value + 1)} disabled={refreshing}>
              <RefreshCw className={refreshing ? 'spinning' : ''} size={16} />
              <span>{updatedAt ? formatTime(updatedAt) : 'Refresh'}</span>
            </button>
          </div>
        </header>

        {error && <div className="error-banner"><AlertTriangle size={16} />Unable to refresh: {error}</div>}

        <section className="metric-grid" aria-label="Pipeline summary">
          <MetricCard icon={Server} label="Fleet" value={formatNumber(devices.length)} detail={`${totals.activeRegions} active regions`} />
          <MetricCard icon={Activity} label="Events · 1 hour" value={formatNumber(totals.eventCount)} detail={`${formatNumber(pipeline.event_count)} stored total`} tone="teal" />
          <MetricCard icon={Clock3} label="Average lag" value={`${formatNumber(totals.avgLag, 0)} ms`} detail="Event timestamp to processing" tone="violet" />
          <MetricCard icon={AlertTriangle} label="Needs attention" value={formatNumber(totals.unhealthy)} detail={`${formatNumber(alerts.length)} recent alert events`} tone="amber" />
        </section>

        <section className="overview-grid">
          <article className="panel chart-card">
            <div className="panel-heading">
              <div><p className="section-label">Fleet signal</p><h2>Temperature snapshot</h2></div>
              <div className="chart-key"><span className="solid" />Live <span className="dashed" />5m average</div>
            </div>
            <div className="chart-viewport">
              {latest.length ? <Line data={chartData} options={chartOptions} /> : <EmptyState>No telemetry samples yet.</EmptyState>}
            </div>
          </article>

          <article className="panel health-card">
            <div className="panel-heading"><div><p className="section-label">Runtime</p><h2>Pipeline health</h2></div></div>
            <div className="health-score">
              <div className="score-ring"><strong>{totalDependencies ? `${healthyDependencies}/${totalDependencies}` : '—'}</strong><span>online</span></div>
              <div><b>{health.status === 'ready' ? 'Ready for traffic' : 'Action required'}</b><p>All required data-plane services.</p></div>
            </div>
            <div className="dependency-list">
              {Object.entries(health.dependencies || {}).map(([name, ok]) => (
                <div key={name}><span className={`service-dot ${ok ? 'up' : 'down'}`} /><span>{name.replace('_', ' ')}</span><b>{ok ? 'Healthy' : 'Down'}</b></div>
              ))}
            </div>
            <div className="delivery-strip"><Database size={16} /><span>Outbox pending</span><b>{formatNumber(pipeline.outbox_pending_count)}</b></div>
          </article>
        </section>

        <section className="lower-grid" id="telemetry">
          <article className="panel telemetry-card">
            <div className="panel-heading">
              <div><p className="section-label">Recent activity</p><h2>Latest telemetry</h2></div>
              <span className="live-label"><Wifi size={14} />Live</span>
            </div>
            <div className="table-wrap">
              <table>
                <thead><tr><th>Device</th><th>Region</th><th>Status</th><th>Temperature</th><th>Voltage</th><th>Lag</th></tr></thead>
                <tbody>
                  {latest.slice(0, 8).map((row) => (
                    <tr key={row.event_id}>
                      <td><b>{row.device_id}</b><small>{formatTime(row.processed_at)}</small></td>
                      <td>{row.region}</td>
                      <td><span className={`status-pill ${row.status.toLowerCase()}`}>{row.status}</span></td>
                      <td>{formatNumber(row.temperature, 1)}° F</td>
                      <td>{formatNumber(row.voltage, 2)} V</td>
                      <td>{formatNumber(row.event_lag_ms)} ms</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {!latest.length && <EmptyState>No devices have reported telemetry.</EmptyState>}
            </div>
          </article>

          <article className="panel incident-card" id="incidents">
            <div className="panel-heading"><div><p className="section-label">Triage queue</p><h2>Recent incidents</h2></div><span className="count-badge">{alerts.length}</span></div>
            <div className="incident-list">
              {alerts.slice(0, 6).map((alert) => (
                <div className="incident" key={alert.event_id}>
                  <span className={`incident-icon ${alert.status.toLowerCase()}`}><AlertTriangle size={15} /></span>
                  <div><b>{alert.device_id}</b><span>{alert.region} · {formatTime(alert.processed_at)}</span></div>
                  <ArrowUpRight size={15} />
                </div>
              ))}
              {!alerts.length && <EmptyState><CheckCircle2 size={20} />No active incidents.</EmptyState>}
            </div>
          </article>
        </section>
      </main>
    </div>
  );
}

createRoot(document.getElementById('root')).render(<App />);

import React, { useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { AlertTriangle, RefreshCw } from 'lucide-react';
import {
  Chart as ChartJS, CategoryScale, LinearScale, PointElement, LineElement, Filler, Tooltip,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import './styles.css';

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Filler, Tooltip);

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';
const EMPTY_OVERVIEW = { device_status: {}, regions: [], trend: [], latest_devices: [], reliability: {} };

function formatNumber(value, digits = 0) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '0';
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: digits });
}

function formatTime(value, includeDate = false) {
  if (!value) return 'NEVER';
  return new Intl.DateTimeFormat(undefined, {
    month: includeDate ? '2-digit' : undefined, day: includeDate ? '2-digit' : undefined,
    hour: '2-digit', minute: '2-digit', second: includeDate ? undefined : '2-digit', hour12: false,
  }).format(new Date(value));
}

async function fetchJson(path) {
  const response = await fetch(`${API_URL}${path}`);
  if (!response.ok) throw new Error(`${path} returned ${response.status}`);
  return response.json();
}

function Metric({ label, value, detail, href }) {
  const content = <><span>{label}</span><strong>{value}</strong><small>{detail}{href ? ' · INVESTIGATE ↗' : ''}</small></>;
  return <article className="metric">{href ? <a href={href} target="_blank" rel="noreferrer">{content}</a> : content}</article>;
}

function EmptyState({ children }) { return <div className="empty-state">{children}</div>; }

const baseChartOptions = {
  responsive: true, maintainAspectRatio: false, resizeDelay: 250, animation: false,
  interaction: { mode: 'index', intersect: false },
  plugins: {
    legend: { display: false },
    tooltip: { backgroundColor: '#111', titleColor: '#fff', bodyColor: '#fff', padding: 9 },
  },
};

function App() {
  const [overview, setOverview] = useState(EMPTY_OVERVIEW);
  const [alerts, setAlerts] = useState([]);
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
        const [overviewRows, alertRows, healthRows] = await Promise.all([
          fetchJson('/dashboard/overview'), fetchJson('/alerts?limit=25'), fetchJson('/health'),
        ]);
        if (active) {
          setOverview(overviewRows); setAlerts(alertRows); setHealth(healthRows);
          setUpdatedAt(new Date()); setError('');
        }
      } catch (err) { if (active) setError(err.message); }
      finally { if (active) setRefreshing(false); }
    }
    load();
    const id = setInterval(load, 10000);
    return () => { active = false; clearInterval(id); };
  }, [refreshToken]);

  const statusEntries = Object.entries(overview.device_status || {});
  const deviceCount = statusEntries.reduce((sum, [, count]) => sum + Number(count), 0);
  const unhealthy = statusEntries.reduce((sum, [status, count]) => sum + (status === 'OK' ? 0 : Number(count)), 0);
  const latestWindow = overview.trend.at(-1) || {};
  const reliability = overview.reliability || {};
  const dependencies = Object.entries(health.dependencies || {});

  const volumeData = useMemo(() => ({
    labels: overview.trend.map((row) => formatTime(row.bucket, true)),
    datasets: [
      { label: 'EVENTS', data: overview.trend.map((row) => row.events), borderColor: '#111', backgroundColor: 'rgba(0,0,0,.04)', pointRadius: 2, borderWidth: 1.5, fill: true, tension: 0.15 },
      { label: 'NON-OK', data: overview.trend.map((row) => row.failures), borderColor: '#a51e1e', pointRadius: 2, borderWidth: 1.5, borderDash: [4, 3], fill: false, tension: 0.15 },
    ],
  }), [overview.trend]);

  const signalData = useMemo(() => ({
    labels: overview.trend.map((row) => formatTime(row.bucket, true)),
    datasets: [
      { label: 'TEMPERATURE °F', data: overview.trend.map((row) => row.avg_temperature), borderColor: '#111', pointRadius: 2, borderWidth: 1.5, yAxisID: 'temperature', tension: 0.15 },
      { label: 'VOLTAGE V', data: overview.trend.map((row) => row.avg_voltage), borderColor: '#16803c', pointRadius: 2, borderWidth: 1.5, borderDash: [4, 3], yAxisID: 'voltage', tension: 0.15 },
    ],
  }), [overview.trend]);

  const volumeOptions = useMemo(() => ({ ...baseChartOptions, scales: {
    x: { grid: { display: false }, ticks: { color: '#666', maxRotation: 0, maxTicksLimit: 6 }, border: { color: '#bbb' } },
    y: { beginAtZero: true, grid: { color: '#e3e3e3' }, ticks: { color: '#666', precision: 0 }, border: { display: false } },
  } }), []);

  const signalOptions = useMemo(() => ({ ...baseChartOptions, scales: {
    x: { grid: { display: false }, ticks: { color: '#666', maxRotation: 0, maxTicksLimit: 6 }, border: { color: '#bbb' } },
    temperature: { position: 'left', grid: { color: '#e3e3e3' }, ticks: { color: '#555', callback: (v) => `${v}°` }, border: { display: false } },
    voltage: { position: 'right', grid: { drawOnChartArea: false }, ticks: { color: '#16803c', callback: (v) => `${v}V` }, border: { display: false } },
  } }), []);

  return (
    <div className="app-shell" id="overview">
      <header className="site-header">
        <div className="wordmark">METRINGEST <span>/ TELEMETRY OPERATIONS</span></div>
        <nav><a href="#overview">OVERVIEW</a><a href="#signals">SIGNALS</a><a href="#devices">DEVICES</a><a href="http://localhost:9090" target="_blank" rel="noreferrer">PROMETHEUS ↗</a></nav>
      </header>
      <main>
        <header className="topbar">
          <div><p className="eyebrow">SYSTEM / LIVE</p><h1>Telemetry Pipeline</h1><p className="subtitle">Fleet signals, processing throughput and delivery state.</p></div>
          <div className="header-actions">
            <div className={`health-chip ${health.status === 'ready' && !error ? 'healthy' : 'degraded'}`}><span />{error ? 'CONNECTION ERROR' : health.status === 'ready' ? 'OPERATIONAL' : 'DEGRADED'}</div>
            <button className="refresh-button" onClick={() => setRefreshToken((v) => v + 1)} disabled={refreshing}><RefreshCw className={refreshing ? 'spinning' : ''} size={13} />{updatedAt ? formatTime(updatedAt) : 'REFRESH'}</button>
          </div>
        </header>
        {error && <div className="error-banner"><AlertTriangle size={14} />REFRESH FAILED — {error}</div>}

        <section className="metric-grid">
          <Metric label="DEVICES" value={formatNumber(deviceCount)} detail={`${overview.regions.length} regions`} />
          <Metric label="EVENTS STORED" value={formatNumber(reliability.event_count)} detail={`${formatNumber(latestWindow.events)} latest minute`} />
          <Metric label="MEAN LAG" value={`${formatNumber(latestWindow.avg_lag_ms)} ms`} detail="latest processing window" />
          <Metric label="UNHEALTHY" value={formatNumber(unhealthy)} detail={`${formatNumber(reliability.low_voltage_devices)} low voltage`} />
          <Metric label="DLQ" value={formatNumber(reliability.dlq_count)} detail={`${formatNumber(reliability.replay_count)} replay attempts`} href="http://localhost:9090/graph?g0.expr=dead_letter_count_total" />
          <Metric label="OUTBOX PENDING" value={formatNumber(reliability.outbox_pending)} detail={`oldest ${formatNumber(reliability.oldest_outbox_seconds)}s`} href="http://localhost:9090/graph?g0.expr=outbox_oldest_pending_seconds" />
        </section>

        <section className="overview-grid">
          <article className="panel chart-panel">
            <div className="panel-heading"><div><p className="section-label">01 / PROCESSING</p><h2>Event volume and non-OK results</h2></div><div className="chart-key"><span />EVENTS <i className="red" />NON-OK</div></div>
            <div className="chart-viewport">{overview.trend.length ? <Line data={volumeData} options={volumeOptions} /> : <EmptyState>NO PROCESSING WINDOWS</EmptyState>}</div>
          </article>
          <article className="panel fleet-panel">
            <div className="panel-heading"><div><p className="section-label">02 / FLEET</p><h2>Device state</h2></div><strong className="fraction">{formatNumber(deviceCount)}</strong></div>
            <div className="status-list">{statusEntries.map(([status, count]) => <div key={status}><span>{status}</span><div><i className={status.toLowerCase()} style={{ width: `${deviceCount ? (Number(count) / deviceCount) * 100 : 0}%` }} /></div><b>{formatNumber(count)}</b></div>)}</div>
            <a className="health-summary investigation-link" href="http://localhost:9090/graph?g0.expr=dependency_ready" target="_blank" rel="noreferrer">DEPENDENCIES {dependencies.filter(([, ok]) => ok).length}/{dependencies.length || '—'} <span>INVESTIGATE ↗</span></a>
            <div className="dependency-list">{dependencies.map(([name, ok]) => <div key={name}><span className={ok ? 'up' : 'down'} />{name.replace('_', ' ')}<b>{ok ? 'UP' : 'DOWN'}</b></div>)}</div>
          </article>
        </section>

        <section className="signal-grid" id="signals">
          <article className="panel signal-panel">
            <div className="panel-heading"><div><p className="section-label">03 / SENSOR SIGNALS</p><h2>Average temperature and voltage</h2></div><div className="chart-key"><span />TEMP <i />VOLTAGE</div></div>
            <div className="chart-viewport compact">{overview.trend.length ? <Line data={signalData} options={signalOptions} /> : <EmptyState>NO SIGNAL DATA</EmptyState>}</div>
          </article>
          <article className="panel region-panel">
            <div className="panel-heading"><div><p className="section-label">04 / DISTRIBUTION</p><h2>Regional health</h2></div></div>
            <div className="region-table"><div className="region-head"><span>REGION</span><span>DEVICES</span><span>NON-OK</span><span>AVG V</span></div>{overview.regions.map((row) => <div key={row.region}><b>{row.region}</b><span>{formatNumber(row.devices)}</span><span>{formatNumber(row.unhealthy)}</span><span>{formatNumber(row.avg_voltage, 2)}</span></div>)}</div>
          </article>
        </section>

        <section className="lower-grid" id="devices">
          <article className="panel telemetry-panel">
            <div className="panel-heading"><div><p className="section-label">05 / DEVICE STATE</p><h2>Most recently seen devices</h2></div><span className="live-label">● LIVE</span></div>
            <div className="table-wrap"><table><thead><tr><th>DEVICE</th><th>REGION</th><th>STATE</th><th>TEMP</th><th>VOLTAGE</th><th>LAST SEEN</th></tr></thead><tbody>{overview.latest_devices.map((row) => <tr key={row.device_id}><td><b>{row.device_id}</b></td><td>{row.region}</td><td><span className={`state ${row.status.toLowerCase()}`}>{row.status}</span></td><td>{formatNumber(row.temperature, 1)}° F</td><td>{formatNumber(row.voltage, 2)} V</td><td>{formatTime(row.last_seen)}</td></tr>)}</tbody></table>{!overview.latest_devices.length && <EmptyState>NO DEVICE DATA</EmptyState>}</div>
          </article>
          <article className="panel incident-panel" id="incidents">
            <div className="panel-heading"><div><p className="section-label">06 / TRIAGE</p><h2>Recent incidents</h2></div><strong className="incident-count">{alerts.length}</strong></div>
            <div className="incident-list">{alerts.slice(0, 6).map((alert) => <div className="incident" key={alert.event_id}><span className={`incident-code ${alert.status.toLowerCase()}`}>{alert.status === 'FAILED' ? 'ERR' : 'WRN'}</span><div><b>{alert.device_id}</b><span>{alert.region} / {formatTime(alert.processed_at)}</span></div></div>)}{!alerts.length && <EmptyState>NO ACTIVE INCIDENTS</EmptyState>}</div>
          </article>
        </section>
      </main>
      <footer>METRINGEST TELEMETRY CONTROL · SLO P99 &lt; 5S · AUTO REFRESH 10S</footer>
    </div>
  );
}

createRoot(document.getElementById('root')).render(<App />);

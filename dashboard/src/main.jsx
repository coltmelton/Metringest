import React, { useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { AlertTriangle, RefreshCw } from 'lucide-react';
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
  if (!value) return 'NEVER';
  return new Intl.DateTimeFormat(undefined, {
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
  }).format(new Date(value));
}

async function fetchJson(path) {
  const response = await fetch(`${API_URL}${path}`);
  if (!response.ok) throw new Error(`${path} returned ${response.status}`);
  return response.json();
}

function Metric({ label, value, detail }) {
  return <article className="metric"><span>{label}</span><strong>{value}</strong><small>{detail}</small></article>;
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
          fetchJson('/devices'), fetchJson('/alerts?limit=25'),
          fetchJson('/pipeline/stats'), fetchJson('/health'),
        ]);
        const regions = [...new Set(deviceRows.map((device) => device.region))].slice(0, 6);
        const [summaryRows, latestRows] = await Promise.all([
          Promise.all(regions.map((region) => fetchJson(`/regions/${region}/summary`))),
          Promise.all(deviceRows.slice(0, 12).map((device) =>
            fetchJson(`/devices/${device.device_id}/latest`).catch(() => null))),
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
    return () => { active = false; clearInterval(id); };
  }, [refreshToken]);

  const totals = useMemo(() => {
    const eventCount = summaries.reduce((sum, row) => sum + Number(row.event_count || 0), 0);
    const lagRows = summaries.filter((row) => row.avg_event_lag_ms !== null);
    const avgLag = lagRows.reduce((sum, row) => sum + Number(row.avg_event_lag_ms || 0), 0) /
      Math.max(lagRows.length, 1);
    return {
      eventCount,
      avgLag,
      unhealthy: devices.filter((device) => device.status !== 'OK').length,
      regions: new Set(devices.map((device) => device.region)).size,
    };
  }, [devices, summaries]);

  const chartData = useMemo(() => ({
    labels: latest.map((row) => row.device_id.replace('sensor-', '#')),
    datasets: [
      {
        label: 'CURRENT', data: latest.map((row) => row.temperature),
        borderColor: '#111111', backgroundColor: 'rgba(0,0,0,.035)',
        pointBackgroundColor: '#111111', pointBorderColor: '#ffffff',
        pointRadius: 3, pointHoverRadius: 4, borderWidth: 1.5, fill: true, tension: 0.15,
      },
      {
        label: '5M AVG', data: latest.map((row) => row.avg_temperature_5m),
        borderColor: '#16803c', pointRadius: 0, borderWidth: 1.5,
        borderDash: [4, 4], fill: false, tension: 0.15,
      },
    ],
  }), [latest]);

  const chartOptions = useMemo(() => ({
    responsive: true, maintainAspectRatio: false, resizeDelay: 250, animation: false,
    interaction: { mode: 'index', intersect: false },
    plugins: {
      legend: { display: false },
      tooltip: {
        backgroundColor: '#111111', borderColor: '#111111', borderWidth: 1,
        titleColor: '#ffffff', bodyColor: '#ffffff', padding: 9,
      },
    },
    scales: {
      x: { grid: { display: false }, ticks: { color: '#666666', maxRotation: 0, maxTicksLimit: 8 }, border: { color: '#bcbcbc' } },
      y: { grid: { color: '#e3e3e3' }, ticks: { color: '#666666', callback: (value) => `${value}°` }, border: { display: false } },
    },
  }), []);

  const dependencies = Object.entries(health.dependencies || {});
  const healthyDependencies = dependencies.filter(([, ok]) => ok).length;

  return (
    <div className="app-shell" id="overview">
      <header className="site-header">
        <div className="wordmark">METRINGEST <span>/ TELEMETRY OPERATIONS</span></div>
        <nav><a href="#overview">OVERVIEW</a><a href="#telemetry">TELEMETRY</a><a href="#incidents">INCIDENTS</a><a href="http://localhost:9090" target="_blank" rel="noreferrer">PROMETHEUS ↗</a></nav>
      </header>

      <main>
        <header className="topbar">
          <div><p className="eyebrow">SYSTEM / LIVE</p><h1>Telemetry Pipeline</h1><p className="subtitle">Ingestion, processing and delivery status.</p></div>
          <div className="header-actions">
            <div className={`health-chip ${health.status === 'ready' && !error ? 'healthy' : 'degraded'}`}><span />{error ? 'CONNECTION ERROR' : health.status === 'ready' ? 'OPERATIONAL' : 'DEGRADED'}</div>
            <button className="refresh-button" onClick={() => setRefreshToken((value) => value + 1)} disabled={refreshing}><RefreshCw className={refreshing ? 'spinning' : ''} size={13} />{updatedAt ? formatTime(updatedAt) : 'REFRESH'}</button>
          </div>
        </header>

        {error && <div className="error-banner"><AlertTriangle size={14} />REFRESH FAILED — {error}</div>}

        <section className="metric-grid">
          <Metric label="DEVICES" value={formatNumber(devices.length)} detail={`${totals.regions} regions`} />
          <Metric label="EVENTS / HOUR" value={formatNumber(totals.eventCount)} detail={`${formatNumber(pipeline.event_count)} stored`} />
          <Metric label="MEAN LAG" value={`${formatNumber(totals.avgLag)} ms`} detail="timestamp → process" />
          <Metric label="UNHEALTHY" value={formatNumber(totals.unhealthy)} detail={`${alerts.length} recent alerts`} />
        </section>

        <section className="overview-grid">
          <article className="panel chart-panel">
            <div className="panel-heading"><div><p className="section-label">01 / FLEET SIGNAL</p><h2>Temperature snapshot</h2></div><div className="chart-key"><span />CURRENT <i />5M AVG</div></div>
            <div className="chart-viewport">{latest.length ? <Line data={chartData} options={chartOptions} /> : <EmptyState>NO TELEMETRY SAMPLES</EmptyState>}</div>
          </article>

          <article className="panel health-panel">
            <div className="panel-heading"><div><p className="section-label">02 / RUNTIME</p><h2>Pipeline health</h2></div><strong className="fraction">{dependencies.length ? `${healthyDependencies}/${dependencies.length}` : '—'}</strong></div>
            <div className="health-summary">{health.status === 'ready' ? 'READY FOR TRAFFIC' : 'ACTION REQUIRED'}</div>
            <div className="dependency-list">{dependencies.map(([name, ok]) => <div key={name}><span className={ok ? 'up' : 'down'} />{name.replace('_', ' ')}<b>{ok ? 'UP' : 'DOWN'}</b></div>)}</div>
            <div className="delivery-row"><span>OUTBOX PENDING</span><b>{formatNumber(pipeline.outbox_pending_count)}</b></div>
            <div className="delivery-row"><span>DLQ RECORDS</span><b>{formatNumber(pipeline.dlq_count)}</b></div>
          </article>
        </section>

        <section className="lower-grid" id="telemetry">
          <article className="panel telemetry-panel">
            <div className="panel-heading"><div><p className="section-label">03 / RECENT ACTIVITY</p><h2>Latest telemetry</h2></div><span className="live-label">● LIVE</span></div>
            <div className="table-wrap"><table><thead><tr><th>DEVICE</th><th>REGION</th><th>STATE</th><th>TEMP</th><th>VOLTAGE</th><th>LAG</th></tr></thead><tbody>{latest.slice(0, 8).map((row) => <tr key={row.event_id}><td><b>{row.device_id}</b><small>{formatTime(row.processed_at)}</small></td><td>{row.region}</td><td><span className={`state ${row.status.toLowerCase()}`}>{row.status}</span></td><td>{formatNumber(row.temperature, 1)}° F</td><td>{formatNumber(row.voltage, 2)} V</td><td>{formatNumber(row.event_lag_ms)} ms</td></tr>)}</tbody></table>{!latest.length && <EmptyState>NO DEVICE DATA</EmptyState>}</div>
          </article>

          <article className="panel incident-panel" id="incidents">
            <div className="panel-heading"><div><p className="section-label">04 / TRIAGE</p><h2>Recent incidents</h2></div><strong className="incident-count">{alerts.length}</strong></div>
            <div className="incident-list">{alerts.slice(0, 6).map((alert) => <div className="incident" key={alert.event_id}><span className={`incident-code ${alert.status.toLowerCase()}`}>{alert.status === 'FAILED' ? 'ERR' : 'WRN'}</span><div><b>{alert.device_id}</b><span>{alert.region} / {formatTime(alert.processed_at)}</span></div></div>)}{!alerts.length && <EmptyState>NO ACTIVE INCIDENTS</EmptyState>}</div>
          </article>
        </section>
      </main>
      <footer>METRINGEST TELEMETRY CONTROL · SLO P99 &lt; 5S · AUTO REFRESH 10S</footer>
    </div>
  );
}

createRoot(document.getElementById('root')).render(<App />);

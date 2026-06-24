import React, { useEffect, useMemo, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { Activity, AlertTriangle, Clock, Cpu, Gauge, Server } from 'lucide-react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import './styles.css';

ChartJS.register(CategoryScale, LinearScale, PointElement, LineElement, Tooltip, Legend);

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

function formatNumber(value, digits = 0) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '0';
  return Number(value).toLocaleString(undefined, { maximumFractionDigits: digits });
}

async function fetchJson(path) {
  const response = await fetch(`${API_URL}${path}`);
  if (!response.ok) throw new Error(`${path} returned ${response.status}`);
  return response.json();
}

function Stat({ icon: Icon, label, value, tone }) {
  return (
    <section className={`stat ${tone || ''}`}>
      <Icon size={20} aria-hidden="true" />
      <div>
        <span>{label}</span>
        <strong>{value}</strong>
      </div>
    </section>
  );
}

function App() {
  const [devices, setDevices] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [latest, setLatest] = useState([]);
  const [summaries, setSummaries] = useState([]);
  const [error, setError] = useState('');

  useEffect(() => {
    let active = true;

    async function load() {
      try {
        const deviceRows = await fetchJson('/devices');
        const regions = [...new Set(deviceRows.map((device) => device.region))].slice(0, 6);
        const [alertRows, summaryRows] = await Promise.all([
          fetchJson('/alerts?limit=25'),
          Promise.all(regions.map((region) => fetchJson(`/regions/${region}/summary`))),
        ]);
        const latestRows = await Promise.all(
          deviceRows.slice(0, 10).map((device) =>
            fetchJson(`/devices/${device.device_id}/latest`).catch(() => null),
          ),
        );
        if (active) {
          setDevices(deviceRows);
          setAlerts(alertRows);
          setSummaries(summaryRows);
          setLatest(latestRows.filter(Boolean));
          setError('');
        }
      } catch (err) {
        if (active) setError(err.message);
      }
    }

    load();
    const id = setInterval(load, 5000);
    return () => {
      active = false;
      clearInterval(id);
    };
  }, []);

  const totals = useMemo(() => {
    const eventCount = summaries.reduce((sum, row) => sum + Number(row.event_count || 0), 0);
    const avgLag =
      summaries.reduce((sum, row) => sum + Number(row.avg_event_lag_ms || 0), 0) /
      Math.max(summaries.filter((row) => row.avg_event_lag_ms).length, 1);
    const failedDevices = devices.filter((device) => device.status === 'FAILED').length;
    return { eventCount, avgLag, failedDevices };
  }, [devices, summaries]);

  const chartData = {
    labels: latest.map((row) => row.device_id),
    datasets: [
      {
        label: 'Temperature',
        data: latest.map((row) => row.temperature),
        borderColor: '#2563eb',
        backgroundColor: '#2563eb',
        tension: 0.35,
      },
      {
        label: '5m Avg',
        data: latest.map((row) => row.avg_temperature_5m),
        borderColor: '#16a34a',
        backgroundColor: '#16a34a',
        tension: 0.35,
      },
    ],
  };

  return (
    <main>
      <header>
        <div>
          <p>Distributed Telemetry Pipeline</p>
          <h1>Live Operations</h1>
        </div>
        <span className={error ? 'status bad' : 'status good'}>
          {error ? `API error: ${error}` : 'Streaming'}
        </span>
      </header>

      <div className="stats">
        <Stat icon={Server} label="Devices" value={formatNumber(devices.length)} />
        <Stat icon={Activity} label="Events / Hour" value={formatNumber(totals.eventCount)} />
        <Stat icon={Clock} label="Avg Lag" value={`${formatNumber(totals.avgLag, 1)} ms`} />
        <Stat icon={AlertTriangle} label="Alerts" value={formatNumber(alerts.length)} tone="warn" />
        <Stat icon={Cpu} label="Failed Devices" value={formatNumber(totals.failedDevices)} tone="danger" />
      </div>

      <section className="workspace">
        <div className="panel chart-panel">
          <div className="panel-title">
            <Gauge size={18} />
            <h2>Latest Temperature</h2>
          </div>
          <Line data={chartData} options={{ responsive: true, maintainAspectRatio: false }} />
        </div>

        <div className="panel">
          <div className="panel-title">
            <AlertTriangle size={18} />
            <h2>Alerts</h2>
          </div>
          <div className="alert-list">
            {alerts.slice(0, 8).map((alert) => (
              <article key={alert.event_id}>
                <strong>{alert.device_id}</strong>
                <span>{alert.region}</span>
                <span>{alert.status}</span>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="panel table-panel">
        <div className="panel-title">
          <Activity size={18} />
          <h2>Latest Telemetry</h2>
        </div>
        <table>
          <thead>
            <tr>
              <th>Device</th>
              <th>Region</th>
              <th>Status</th>
              <th>Temp</th>
              <th>Voltage</th>
              <th>Lag</th>
            </tr>
          </thead>
          <tbody>
            {latest.map((row) => (
              <tr key={row.event_id}>
                <td>{row.device_id}</td>
                <td>{row.region}</td>
                <td><span className={`pill ${row.status.toLowerCase()}`}>{row.status}</span></td>
                <td>{formatNumber(row.temperature, 1)}</td>
                <td>{formatNumber(row.voltage, 2)}</td>
                <td>{formatNumber(row.event_lag_ms)} ms</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </main>
  );
}

createRoot(document.getElementById('root')).render(<App />);

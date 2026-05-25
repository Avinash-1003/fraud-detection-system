/**
 * FraudShield — Real-Time Fraud Detection Dashboard
 * ==================================================
 * Main application component with all dashboard panels.
 */

import { useState, useEffect, useMemo } from 'react';
import {
  LineChart, Line, AreaChart, Area, PieChart, Pie, Cell,
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, Legend
} from 'recharts';
import { useWebSocket } from './hooks/useWebSocket';
import { api } from './services/api';

/* ===== ICON COMPONENTS ===== */
const ShieldIcon = () => (
  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
    <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/>
  </svg>
);

const ActivityIcon = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
);

const AlertIcon = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
);

const ClockIcon = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>
);

const TrendUpIcon = () => (
  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/></svg>
);

/* ===== NAVBAR ===== */
function Navbar({ isConnected }) {
  return (
    <nav className="navbar">
      <div className="navbar-brand">
        <div className="navbar-logo">
          <ShieldIcon />
        </div>
        <span className="navbar-title">FraudShield</span>
      </div>
      <div className="navbar-status">
        <div className={`status-dot`} style={{ background: isConnected ? '#10b981' : '#ef4444' }} />
        <span>{isConnected ? 'Live' : 'Connecting...'}</span>
        <span style={{ marginLeft: '1rem', opacity: 0.5 }}>
          {new Date().toLocaleTimeString()}
        </span>
      </div>
    </nav>
  );
}

/* ===== METRIC CARD ===== */
function MetricCard({ icon, label, value, change, color = 'blue' }) {
  return (
    <div className="metric-card fade-in">
      <div className={`metric-icon ${color}`}>{icon}</div>
      <div className="metric-content">
        <div className="metric-label">{label}</div>
        <div className="metric-value">{value}</div>
        {change && (
          <div className={`metric-change ${change >= 0 ? 'positive' : 'negative'}`}>
            {change >= 0 ? '↑' : '↓'} {Math.abs(change)}%
          </div>
        )}
      </div>
    </div>
  );
}

/* ===== TRANSACTION TABLE ===== */
function TransactionFeed({ transactions }) {
  const getBadgeClass = (classification) => {
    switch (classification) {
      case 'FRAUDULENT': return 'badge badge-fraudulent';
      case 'SUSPICIOUS': return 'badge badge-suspicious';
      default: return 'badge badge-legitimate';
    }
  };

  const formatAmount = (amount) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency', currency: 'USD'
    }).format(amount || 0);
  };

  const formatTime = (timestamp) => {
    if (!timestamp) return '--';
    const d = new Date(typeof timestamp === 'number' ? timestamp : timestamp);
    return d.toLocaleTimeString();
  };

  return (
    <div className="card">
      <div className="card-header">
        <span className="card-title">Live Transaction Feed</span>
        <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)' }}>
          {transactions.length} events
        </span>
      </div>
      <div className="scroll-container">
        <table className="txn-table">
          <thead>
            <tr>
              <th>Time</th>
              <th>Transaction ID</th>
              <th>Amount</th>
              <th>Merchant</th>
              <th>Channel</th>
              <th>Score</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {transactions.length === 0 ? (
              <tr>
                <td colSpan="7" style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-muted)' }}>
                  Waiting for transactions...
                </td>
              </tr>
            ) : (
              transactions.slice(0, 30).map((txn, i) => (
                <tr key={txn.transaction_id || i} className="slide-in">
                  <td style={{ color: 'var(--text-muted)', fontSize: '0.8rem' }}>
                    {formatTime(txn.timestamp)}
                  </td>
                  <td style={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>
                    {(txn.transaction_id || '').slice(0, 16)}
                  </td>
                  <td className="amount" style={{
                    color: (txn.amount || 0) > 2000 ? 'var(--amber)' : 'var(--text-primary)'
                  }}>
                    {formatAmount(txn.amount)}
                  </td>
                  <td>{txn.merchant_name || '--'}</td>
                  <td>
                    <span style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>
                      {txn.channel || '--'}
                    </span>
                  </td>
                  <td>
                    <span style={{
                      fontWeight: 600,
                      color: (txn.fraud_score || 0) > 0.7 ? 'var(--red)'
                        : (txn.fraud_score || 0) > 0.4 ? 'var(--amber)' : 'var(--green)'
                    }}>
                      {(txn.fraud_score || 0).toFixed(3)}
                    </span>
                  </td>
                  <td>
                    <span className={getBadgeClass(txn.classification)}>
                      {txn.classification || 'PENDING'}
                    </span>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

/* ===== FRAUD ALERTS PANEL ===== */
function FraudAlerts({ transactions }) {
  const alerts = useMemo(() => {
    return transactions
      .filter(t => t.classification === 'FRAUDULENT' || t.classification === 'SUSPICIOUS')
      .slice(0, 15);
  }, [transactions]);

  return (
    <div className="card">
      <div className="card-header">
        <span className="card-title">⚠️ Fraud Alerts</span>
        <span className="badge badge-critical">{alerts.length} active</span>
      </div>
      <div className="scroll-container">
        {alerts.length === 0 ? (
          <div style={{ textAlign: 'center', padding: '2rem', color: 'var(--text-muted)' }}>
            No alerts yet
          </div>
        ) : (
          alerts.map((alert, i) => (
            <div className="alert-item slide-in" key={alert.transaction_id || i}>
              <div className={`alert-icon ${alert.classification === 'FRAUDULENT' ? 'critical' : 'high'}`}>
                <AlertIcon />
              </div>
              <div className="alert-details">
                <div className="alert-title">
                  {alert.classification === 'FRAUDULENT' ? '🔴' : '🟡'}{' '}
                  ${(alert.amount || 0).toLocaleString(undefined, { minimumFractionDigits: 2 })} — {alert.merchant_name || 'Unknown'}
                </div>
                <div className="alert-meta">
                  {alert.cardholder_id} • Score: {(alert.fraud_score || 0).toFixed(3)} • {alert.channel}
                </div>
              </div>
              <span className={`badge ${alert.classification === 'FRAUDULENT' ? 'badge-critical' : 'badge-medium'}`}>
                {alert.classification}
              </span>
            </div>
          ))
        )}
      </div>
    </div>
  );
}

/* ===== CHARTS ===== */
const COLORS = {
  legitimate: '#10b981',
  suspicious: '#f59e0b',
  fraudulent: '#ef4444',
};

function FraudTrendChart({ transactions }) {
  const chartData = useMemo(() => {
    // Group transactions into 10-second buckets
    const buckets = {};
    transactions.forEach(txn => {
      const ts = txn.timestamp || Date.now();
      const bucket = Math.floor(ts / 10000) * 10000;
      const key = new Date(bucket).toLocaleTimeString();
      if (!buckets[key]) buckets[key] = { time: key, total: 0, fraud: 0 };
      buckets[key].total += 1;
      if (txn.classification === 'FRAUDULENT') buckets[key].fraud += 1;
    });
    return Object.values(buckets).slice(-20);
  }, [transactions]);

  return (
    <div className="card">
      <div className="card-header">
        <span className="card-title">Transaction Volume & Fraud Rate</span>
      </div>
      <div className="chart-wrapper">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart data={chartData}>
            <defs>
              <linearGradient id="colorTotal" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3}/>
                <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
              </linearGradient>
              <linearGradient id="colorFraud" x1="0" y1="0" x2="0" y2="1">
                <stop offset="5%" stopColor="#ef4444" stopOpacity={0.3}/>
                <stop offset="95%" stopColor="#ef4444" stopOpacity={0}/>
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
            <XAxis dataKey="time" stroke="#64748b" fontSize={11} />
            <YAxis stroke="#64748b" fontSize={11} />
            <Tooltip
              contentStyle={{
                background: '#1e293b', border: '1px solid rgba(255,255,255,0.1)',
                borderRadius: '8px', fontSize: '0.85rem'
              }}
            />
            <Area type="monotone" dataKey="total" stroke="#3b82f6" fill="url(#colorTotal)" name="Total" />
            <Area type="monotone" dataKey="fraud" stroke="#ef4444" fill="url(#colorFraud)" name="Fraud" />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}

function ClassificationPieChart({ transactions }) {
  const data = useMemo(() => {
    const counts = { LEGITIMATE: 0, SUSPICIOUS: 0, FRAUDULENT: 0 };
    transactions.forEach(t => {
      const cls = t.classification || 'LEGITIMATE';
      counts[cls] = (counts[cls] || 0) + 1;
    });
    return [
      { name: 'Legitimate', value: counts.LEGITIMATE, color: COLORS.legitimate },
      { name: 'Suspicious', value: counts.SUSPICIOUS, color: COLORS.suspicious },
      { name: 'Fraudulent', value: counts.FRAUDULENT, color: COLORS.fraudulent },
    ].filter(d => d.value > 0);
  }, [transactions]);

  return (
    <div className="card">
      <div className="card-header">
        <span className="card-title">Classification Distribution</span>
      </div>
      <div className="chart-wrapper" style={{ height: 280, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        {data.length === 0 ? (
          <span style={{ color: 'var(--text-muted)' }}>No data</span>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={data}
                cx="50%" cy="50%"
                innerRadius={60} outerRadius={100}
                paddingAngle={3}
                dataKey="value"
                animationBegin={0}
                animationDuration={800}
              >
                {data.map((entry, i) => (
                  <Cell key={i} fill={entry.color} stroke="transparent" />
                ))}
              </Pie>
              <Tooltip
                contentStyle={{
                  background: '#1e293b', border: '1px solid rgba(255,255,255,0.1)',
                  borderRadius: '8px'
                }}
              />
              <Legend
                verticalAlign="bottom"
                iconType="circle"
                wrapperStyle={{ fontSize: '0.8rem', color: '#94a3b8' }}
              />
            </PieChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}

/* ===== MAIN APP ===== */
export default function App() {
  const { isConnected, transactions } = useWebSocket();
  const [metrics, setMetrics] = useState(null);
  const [time, setTime] = useState(new Date());

  // Update clock every second
  useEffect(() => {
    const interval = setInterval(() => setTime(new Date()), 1000);
    return () => clearInterval(interval);
  }, []);

  // Fetch dashboard metrics periodically
  useEffect(() => {
    const fetchMetrics = async () => {
      const data = await api.getDashboard();
      if (data) setMetrics(data);
    };
    fetchMetrics();
    const interval = setInterval(fetchMetrics, 5000);
    return () => clearInterval(interval);
  }, []);

  // Computed stats from live data
  const liveStats = useMemo(() => {
    const total = transactions.length;
    const fraud = transactions.filter(t => t.classification === 'FRAUDULENT').length;
    const suspicious = transactions.filter(t => t.classification === 'SUSPICIOUS').length;
    const avgScore = total > 0
      ? transactions.reduce((sum, t) => sum + (t.fraud_score || 0), 0) / total
      : 0;
    const avgLatency = total > 0
      ? transactions.reduce((sum, t) => sum + (t.processing_time_ms || 0), 0) / total
      : 0;

    return {
      total,
      fraud,
      suspicious,
      legitimate: total - fraud - suspicious,
      fraudRate: total > 0 ? ((fraud / total) * 100).toFixed(1) : '0.0',
      avgScore: avgScore.toFixed(3),
      avgLatency: avgLatency.toFixed(1),
    };
  }, [transactions]);

  return (
    <div>
      <Navbar isConnected={isConnected} />

      <div className="dashboard">
        {/* Metric Cards */}
        <div className="metrics-grid">
          <MetricCard
            icon={<ActivityIcon />}
            label="Total Transactions"
            value={metrics?.total_transactions?.toLocaleString() || liveStats.total.toLocaleString()}
            color="blue"
          />
          <MetricCard
            icon={<AlertIcon />}
            label="Fraud Detected"
            value={metrics?.total_fraud || liveStats.fraud}
            change={parseFloat(liveStats.fraudRate)}
            color="red"
          />
          <MetricCard
            icon={<TrendUpIcon />}
            label="Fraud Rate"
            value={`${metrics?.fraud_rate || liveStats.fraudRate}%`}
            color="amber"
          />
          <MetricCard
            icon={<ClockIcon />}
            label="Avg Latency"
            value={`${metrics?.avg_processing_time_ms?.toFixed(1) || liveStats.avgLatency}ms`}
            color="green"
          />
          <MetricCard
            icon={<ShieldIcon />}
            label="Active Alerts"
            value={metrics?.active_alerts || liveStats.fraud + liveStats.suspicious}
            color="purple"
          />
        </div>

        {/* Charts Row */}
        <div className="charts-grid">
          <FraudTrendChart transactions={transactions} />
          <ClassificationPieChart transactions={transactions} />
        </div>

        {/* Bottom Row: Table + Alerts */}
        <div className="bottom-grid">
          <TransactionFeed transactions={transactions} />
          <FraudAlerts transactions={transactions} />
        </div>
      </div>
    </div>
  );
}

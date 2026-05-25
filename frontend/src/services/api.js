/**
 * API Client Service
 * Handles all REST API calls to the FastAPI backend.
 */

const API_BASE = '/api';

async function fetchJSON(url, options = {}) {
  try {
    const response = await fetch(`${API_BASE}${url}`, {
      headers: { 'Content-Type': 'application/json', ...options.headers },
      ...options,
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    return await response.json();
  } catch (error) {
    console.error(`API Error [${url}]:`, error);
    return null;
  }
}

export const api = {
  // Dashboard metrics
  getDashboard: () => fetchJSON('/analytics/dashboard'),

  // Transactions
  getTransactions: (limit = 50) => fetchJSON(`/transactions/?limit=${limit}`),
  getRecentTransactions: (limit = 20) => fetchJSON(`/transactions/recent?limit=${limit}`),
  getTransactionCounts: () => fetchJSON('/transactions/stats/count'),

  // Alerts
  getAlerts: (limit = 50) => fetchJSON(`/alerts/?limit=${limit}`),
  getAlertStats: () => fetchJSON('/alerts/stats'),

  // Analytics
  getTrends: (hours = 24) => fetchJSON(`/analytics/trends?hours=${hours}`),
  getByChannel: () => fetchJSON('/analytics/by-channel'),
  getByAmount: () => fetchJSON('/analytics/by-amount'),
  getModelInfo: () => fetchJSON('/analytics/model'),
};

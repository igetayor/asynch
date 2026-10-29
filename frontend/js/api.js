/**
 * API client module for communicating with the Async Worker Pool HTTP server.
 */
export const Api = {
  async getStats() {
    try {
      const res = await fetch('/api/stats');
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (err) {
      console.warn("Failed to fetch pool telemetry:", err);
      return null;
    }
  },

  async submitTasks({ count = 4, type = "API Sync", failure_rate = 0.2, priority = 2, duration = 0.6 }) {
    const res = await fetch('/api/tasks/submit', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ count, type, failure_rate, priority, duration })
    });
    return await res.json();
  },

  async clearStats() {
    const res = await fetch('/api/pool/clear', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' }
    });
    return await res.json();
  }
};

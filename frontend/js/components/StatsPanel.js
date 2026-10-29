/**
 * Component: Renders real-time aggregate pool metrics (Throughput, Failures, Queue Depth, Success Rate).
 */
export class StatsPanel {
  constructor(containerId) {
    this.container = document.getElementById(containerId);
  }

  update(stats) {
    if (!this.container || !stats) return;

    const totalCompleted = stats.processed_count || 0;
    const totalFailed = stats.failure_count || 0;
    const totalFinished = totalCompleted + totalFailed;
    const successRate = totalFinished > 0 
      ? Math.round((totalCompleted / totalFinished) * 100) 
      : 100;

    this.container.innerHTML = `
      <div class="metric-card">
        <div class="metric-label">Active Workers</div>
        <div class="metric-value">${stats.active_workers} <span style="font-size: 1rem; color: var(--text-dim);">/ ${stats.concurrency}</span></div>
        <div class="metric-desc">${stats.idle_workers} idle coroutines</div>
      </div>

      <div class="metric-card">
        <div class="metric-label">Queue Depth</div>
        <div class="metric-value" style="color: ${stats.queue_depth > 0 ? '#38bdf8' : 'inherit'};">${stats.queue_depth}</div>
        <div class="metric-desc">Pending prioritization</div>
      </div>

      <div class="metric-card">
        <div class="metric-label">Processed Jobs</div>
        <div class="metric-value" style="color: #10b981;">${totalCompleted}</div>
        <div class="metric-desc">${stats.retry_count || 0} retried attempts</div>
      </div>

      <div class="metric-card">
        <div class="metric-label">Success Rate</div>
        <div class="metric-value" style="color: ${successRate >= 90 ? '#10b981' : '#f59e0b'};">${successRate}%</div>
        <div class="metric-desc">${totalFailed} permanent failures</div>
      </div>
    `;
  }
}

/**
 * Component: Streams recent task telemetry logs with status badges and duration metrics.
 */
export class TaskQueueView {
  constructor(containerId) {
    this.container = document.getElementById(containerId);
    this.seenTasks = new Set();
  }

  update(tasks = []) {
    if (!this.container || tasks.length === 0) return;

    // Render latest tasks in descending chronological order
    this.container.innerHTML = tasks.slice().reverse().map(t => {
      let statusClass = "completed";
      let badgeHtml = '<span class="log-badge success">COMPLETED</span>';

      if (t.status === "FAILED") {
        statusClass = "failed";
        badgeHtml = '<span class="log-badge error">FAILED</span>';
      } else if (t.status === "RETRYING") {
        statusClass = "retrying";
        badgeHtml = `<span class="log-badge retry">RETRY (${t.retries}/${t.max_retries})</span>`;
      } else if (t.status === "RUNNING") {
        statusClass = "running";
        badgeHtml = '<span class="log-badge" style="background: rgba(14,165,233,0.2); color:#38bdf8;">RUNNING</span>';
      }

      const priorityTag = t.priority === 1 ? '[P1-HIGH]' : t.priority === 3 ? '[P3-LOW]' : '[P2]';
      const durationStr = t.execution_duration_ms > 0 ? `${t.execution_duration_ms}ms` : 'in flight';

      return `
        <div class="log-entry ${statusClass}">
          <span class="log-time">${priorityTag}</span>
          <span class="log-msg">
            <strong>${t.name}</strong> 
            <span style="color: var(--text-dim); margin-left: 0.5rem;">(${t.task_id})</span>
            ${t.error ? `<span style="color: #ef4444; margin-left: 0.5rem;">${t.error}</span>` : ''}
          </span>
          <span style="font-family: var(--font-mono); font-size: 0.75rem; color: var(--text-muted);">${durationStr}</span>
          ${badgeHtml}
        </div>
      `;
    }).join('');
  }
}

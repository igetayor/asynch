/**
 * Component: Visualizes individual concurrency worker slots and their active coroutine state.
 */
export class WorkerGrid {
  constructor(containerId) {
    this.container = document.getElementById(containerId);
  }

  update(slots = []) {
    if (!this.container) return;

    this.container.innerHTML = slots.map(slot => {
      const isActive = slot.active;
      return `
        <div class="worker-slot-item ${isActive ? 'active' : ''}">
          <div class="slot-top">
            <span style="font-weight: 600;">Worker #${slot.slot + 1}</span>
            <span class="slot-status-pill ${isActive ? 'working' : 'idle'}">
              ${isActive ? 'PROCESSING' : 'IDLE'}
            </span>
          </div>
          <div class="slot-task-desc">
            ${isActive ? `⚙️ Task ID: ${slot.current_task}` : 'Waiting for queued tasks...'}
          </div>
        </div>
      `;
    }).join('');
  }
}

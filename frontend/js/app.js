import { Api } from './api.js';
import { StatsPanel } from './components/StatsPanel.js';
import { WorkerGrid } from './components/WorkerGrid.js';
import { TaskQueueView } from './components/TaskQueueView.js';

class App {
  constructor() {
    this.statsPanel = new StatsPanel('metricsContainer');
    this.workerGrid = new WorkerGrid('workerSlotsContainer');
    this.taskQueueView = new TaskQueueView('activityStream');
    this.pollInterval = null;
  }

  async init() {
    this.bindEvents();
    await this.refresh();
    this.pollInterval = setInterval(() => this.refresh(), 800);
  }

  bindEvents() {
    const dispatchBtn = document.getElementById('btnDispatch');
    if (dispatchBtn) {
      dispatchBtn.addEventListener('click', () => this.handleDispatch());
    }

    const clearBtn = document.getElementById('btnClear');
    if (clearBtn) {
      clearBtn.addEventListener('click', async () => {
        await Api.clearStats();
        await this.refresh();
      });
    }
  }

  async handleDispatch() {
    const count = parseInt(document.getElementById('taskCount').value, 10) || 4;
    const type = document.getElementById('taskType').value || "Data Ingestion";
    const failureRate = parseFloat(document.getElementById('failureRate').value) || 0.2;
    const priority = parseInt(document.getElementById('priorityLevel').value, 10) || 2;
    const duration = parseFloat(document.getElementById('taskDuration').value) || 0.6;

    const btn = document.getElementById('btnDispatch');
    btn.disabled = true;
    btn.textContent = "Enqueueing...";

    try {
      await Api.submitTasks({ count, type, failure_rate: failureRate, priority, duration });
      await this.refresh();
    } finally {
      btn.disabled = false;
      btn.innerHTML = `⚡ Enqueue Batch of ${count} Tasks`;
    }
  }

  async refresh() {
    const stats = await Api.getStats();
    if (!stats) return;

    this.statsPanel.update(stats);
    this.workerGrid.update(stats.worker_slots);
    if (stats.recent_tasks) {
      this.taskQueueView.update(stats.recent_tasks);
    }
  }
}

document.addEventListener('DOMContentLoaded', () => {
  const app = new App();
  app.init();
});

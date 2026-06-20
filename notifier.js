const http = require("http");
const { taskEventEmitter } = require("./taskStore");

const WEBHOOK_URL = process.env.ALERT_WEBHOOK_URL || "";
const ALERT_ENABLED = process.env.ALERT_ENABLED !== "false";

const ALERT_AGGREGATION_WINDOW_MS =
  parseInt(process.env.ALERT_AGGREGATION_WINDOW_MS, 10) || 60 * 1000;
const ALERT_COOLDOWN_MS = parseInt(process.env.ALERT_COOLDOWN_MS, 10) || 5 * 60 * 1000;

const alertHistory = [];
const taskAlertState = new Map();

function formatAggregatedAlert(taskName, state) {
  const errors = Array.from(state.aggregatedErrors.entries()).map(([msg, count]) => ({
    error: msg,
    count,
  }));

  return {
    title: "定时任务失败汇总告警",
    taskName,
    aggregated: true,
    failureCount: state.consecutiveFailures,
    windowStart: state.aggregationWindowStart,
    windowEnd: new Date().toISOString(),
    errors,
    timestamp: new Date().toISOString(),
  };
}

function formatSuppressedRecord(taskName, record, state, reason) {
  return {
    title: "定时任务执行失败告警（已抑制）",
    taskName,
    status: record.status,
    startedAt: record.startedAt,
    finishedAt: record.finishedAt,
    duration: record.duration + "ms",
    error: record.error || "未知错误",
    timestamp: new Date().toISOString(),
    notifiedAt: new Date().toISOString(),
    suppressed: true,
    suppressedReason: reason,
    consecutiveFailures: state.consecutiveFailures,
    webhookSent: false,
  };
}

function formatPendingAggregationRecord(taskName, record, state) {
  return {
    title: "定时任务执行失败（聚合中）",
    taskName,
    status: record.status,
    startedAt: record.startedAt,
    finishedAt: record.finishedAt,
    duration: record.duration + "ms",
    error: record.error || "未知错误",
    timestamp: new Date().toISOString(),
    notifiedAt: new Date().toISOString(),
    aggregated: "pending",
    consecutiveFailures: state.consecutiveFailures,
    webhookSent: false,
  };
}

async function sendWebhook(payload) {
  if (!WEBHOOK_URL) {
    console.log("[Notifier] 未配置 ALERT_WEBHOOK_URL，跳过 Webhook 推送");
    return;
  }

  const data = JSON.stringify(payload);

  const url = new URL(WEBHOOK_URL);
  const options = {
    hostname: url.hostname,
    port: url.port || 80,
    path: url.pathname + url.search,
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "Content-Length": Buffer.byteLength(data),
    },
  };

  return new Promise((resolve, reject) => {
    const req = http.request(options, (res) => {
      let body = "";
      res.on("data", (chunk) => (body += chunk));
      res.on("end", () => {
        console.log(`[Notifier] Webhook 响应: ${res.statusCode} ${body}`);
        resolve({ statusCode: res.statusCode, body });
      });
    });

    req.on("error", (err) => {
      console.error(`[Notifier] Webhook 请求失败: ${err.message}`);
      reject(err);
    });

    req.write(data);
    req.end();
  });
}

function getTaskState(taskName) {
  if (!taskAlertState.has(taskName)) {
    taskAlertState.set(taskName, {
      consecutiveFailures: 0,
      lastNotifiedAt: 0,
      lastError: null,
      aggregationTimer: null,
      aggregationWindowStart: null,
      aggregatedErrors: new Map(),
      isAggregating: false,
      inCooldown: false,
    });
  }
  return taskAlertState.get(taskName);
}

function resetTaskState(taskName) {
  const state = taskAlertState.get(taskName);
  if (state) {
    state.consecutiveFailures = 0;
    state.lastError = null;
    state.aggregatedErrors = new Map();
    state.isAggregating = false;
    state.inCooldown = false;
    state.aggregationWindowStart = null;
    if (state.aggregationTimer) {
      clearTimeout(state.aggregationTimer);
      state.aggregationTimer = null;
    }
  }
}

function pushAlertRecord(record) {
  alertHistory.unshift(record);
  if (alertHistory.length > 200) {
    alertHistory.length = 200;
  }
}

async function flushAggregatedAlert(taskName) {
  const state = getTaskState(taskName);
  if (state.aggregationTimer) {
    clearTimeout(state.aggregationTimer);
    state.aggregationTimer = null;
  }
  state.isAggregating = false;

  const payload = formatAggregatedAlert(taskName, state);

  console.error(
    `[Notifier] 📊 推送汇总告警: ${taskName}，窗口内共失败 ${state.consecutiveFailures} 次`
  );

  const alertRecord = {
    ...payload,
    notifiedAt: new Date().toISOString(),
    webhookSent: false,
  };

  if (ALERT_ENABLED) {
    try {
      await sendWebhook(payload);
      alertRecord.webhookSent = true;
    } catch (err) {
      alertRecord.webhookError = err.message;
    }
  }

  state.lastNotifiedAt = Date.now();
  state.inCooldown = true;
  state.aggregationWindowStart = null;

  setTimeout(() => {
    const s = taskAlertState.get(taskName);
    if (s) {
      s.inCooldown = false;
      console.log(`[Notifier] ⏳ 冷却期结束: ${taskName}`);
    }
  }, ALERT_COOLDOWN_MS);

  pushAlertRecord(alertRecord);
  return alertRecord;
}

async function notify(taskName, record) {
  const error = record.error || "未知错误";
  const state = getTaskState(taskName);

  state.consecutiveFailures += 1;
  state.lastError = error;

  const errCount = state.aggregatedErrors.get(error) || 0;
  state.aggregatedErrors.set(error, errCount + 1);

  if (state.inCooldown) {
    console.warn(
      `[Notifier] 🚫 告警已抑制 (cool_down): ${taskName}，连续失败 ${state.consecutiveFailures} 次`
    );
    const rec = formatSuppressedRecord(taskName, record, state, "cool_down");
    pushAlertRecord(rec);
    return rec;
  }

  if (state.isAggregating) {
    console.log(
      `[Notifier] 📦 告警已聚合: ${taskName}，累计失败 ${state.consecutiveFailures} 次，等待窗口结束`
    );
    const rec = formatPendingAggregationRecord(taskName, record, state);
    pushAlertRecord(rec);
    return rec;
  }

  state.isAggregating = true;
  state.aggregationWindowStart = new Date().toISOString();
  state.aggregatedErrors = new Map([[error, 1]]);
  state.consecutiveFailures = 1;

  console.log(
    `[Notifier] 🕒 启动聚合窗口 (${ALERT_AGGREGATION_WINDOW_MS}ms): ${taskName}，第 1 次失败`
  );

  state.aggregationTimer = setTimeout(() => {
    flushAggregatedAlert(taskName).catch((err) => {
      console.error(`[Notifier] 推送汇总告警异常: ${err.message}`);
    });
  }, ALERT_AGGREGATION_WINDOW_MS);

  const rec = formatPendingAggregationRecord(taskName, record, state);
  pushAlertRecord(rec);
  return rec;
}

taskEventEmitter.on("task_failed", ({ taskName, record }) => {
  notify(taskName, record).catch((err) => {
    console.error(`[Notifier] 告警通知异常: ${err.message}`);
  });
});

taskEventEmitter.on("task_success", ({ taskName }) => {
  const state = taskAlertState.get(taskName);
  if (state && state.consecutiveFailures > 0) {
    if (state.aggregationTimer) {
      console.log(`[Notifier] 📭 取消未发送的聚合告警: ${taskName}（任务已恢复）`);
    }
    console.log(`[Notifier] ✅ 任务已恢复成功: ${taskName}，重置告警状态`);
    resetTaskState(taskName);
  }
});

function getAlertHistory(limit = 50) {
  return alertHistory.slice(0, limit);
}

module.exports = {
  notify,
  getAlertHistory,
  resetTaskState,
  getTaskState,
  flushAggregatedAlert,
};

const http = require("http");
const { taskEventEmitter } = require("./taskStore");

const WEBHOOK_URL = process.env.ALERT_WEBHOOK_URL || "";
const ALERT_ENABLED = process.env.ALERT_ENABLED !== "false";

const ALERT_COOLDOWN_MS = parseInt(process.env.ALERT_COOLDOWN_MS, 10) || 5 * 60 * 1000;
const ALERT_MAX_FAILURES_BEFORE_SUPPRESS =
  parseInt(process.env.ALERT_MAX_FAILURES_BEFORE_SUPPRESS, 10) || 3;

const alertHistory = [];
const taskAlertState = new Map();

function formatAlertMessage(taskName, record) {
  return {
    title: "定时任务执行失败告警",
    taskName,
    status: record.status,
    startedAt: record.startedAt,
    finishedAt: record.finishedAt,
    duration: record.duration + "ms",
    error: record.error || "未知错误",
    timestamp: new Date().toISOString(),
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
      suppressedCount: 0,
      lastError: null,
    });
  }
  return taskAlertState.get(taskName);
}

function resetTaskState(taskName) {
  const state = taskAlertState.get(taskName);
  if (state) {
    state.consecutiveFailures = 0;
    state.suppressedCount = 0;
    state.lastError = null;
  }
}

function shouldSuppress(taskName, error) {
  const state = getTaskState(taskName);
  state.consecutiveFailures += 1;
  state.lastError = error;

  const now = Date.now();
  const sinceLastNotify = now - state.lastNotifiedAt;
  const withinCooldown = sinceLastNotify < ALERT_COOLDOWN_MS;
  const sameErrorAsLast = state.lastNotifiedError === error;

  const exceededFailureThreshold = state.consecutiveFailures > ALERT_MAX_FAILURES_BEFORE_SUPPRESS;

  if (exceededFailureThreshold && withinCooldown && sameErrorAsLast) {
    state.suppressedCount += 1;
    return { suppressed: true, reason: "cooldown_and_same_error", state: { ...state } };
  }

  if (exceededFailureThreshold && withinCooldown) {
    state.suppressedCount += 1;
    return { suppressed: true, reason: "cooldown", state: { ...state } };
  }

  return { suppressed: false, state: { ...state } };
}

async function notify(taskName, record) {
  const error = record.error || "未知错误";
  const suppression = shouldSuppress(taskName, error);
  const state = getTaskState(taskName);

  if (suppression.suppressed) {
    console.warn(
      `[Notifier] 🚫 告警已抑制 (${suppression.reason}): ${taskName}，连续失败 ${state.consecutiveFailures} 次，已抑制 ${state.suppressedCount} 条`
    );
    const suppressedRecord = {
      title: "定时任务执行失败告警（已抑制）",
      taskName,
      status: record.status,
      startedAt: record.startedAt,
      finishedAt: record.finishedAt,
      duration: record.duration + "ms",
      error,
      timestamp: new Date().toISOString(),
      notifiedAt: new Date().toISOString(),
      suppressed: true,
      suppressedReason: suppression.reason,
      consecutiveFailures: state.consecutiveFailures,
      suppressedCount: state.suppressedCount,
      webhookSent: false,
    };
    alertHistory.unshift(suppressedRecord);
    if (alertHistory.length > 200) {
      alertHistory.length = 200;
    }
    return suppressedRecord;
  }

  const payload = formatAlertMessage(taskName, record);
  payload.consecutiveFailures = state.consecutiveFailures;
  if (state.suppressedCount > 0) {
    payload.suppressedSinceLastNotify = state.suppressedCount;
    state.suppressedCount = 0;
  }

  console.error(
    `[Notifier] ⚠️ 任务失败告警: ${taskName} - ${error} (连续第 ${state.consecutiveFailures} 次失败)`
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
  state.lastNotifiedError = error;

  alertHistory.unshift(alertRecord);
  if (alertHistory.length > 200) {
    alertHistory.length = 200;
  }

  return alertRecord;
}

taskEventEmitter.on("task_failed", ({ taskName, record }) => {
  notify(taskName, record).catch((err) => {
    console.error(`[Notifier] 告警通知异常: ${err.message}`);
  });
});

taskEventEmitter.on("task_success", ({ taskName }) => {
  const state = taskAlertState.get(taskName);
  if (state && state.consecutiveFailures > 0) {
    console.log(`[Notifier] ✅ 任务已恢复成功: ${taskName}，重置告警抑制状态`);
    resetTaskState(taskName);
  }
});

function getAlertHistory(limit = 50) {
  return alertHistory.slice(0, limit);
}

module.exports = {
  notify,
  getAlertHistory,
  formatAlertMessage,
  resetTaskState,
  getTaskState,
};

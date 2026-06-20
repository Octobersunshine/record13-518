const http = require("http");
const { taskEventEmitter } = require("./taskStore");

const WEBHOOK_URL = process.env.ALERT_WEBHOOK_URL || "";
const ALERT_ENABLED = process.env.ALERT_ENABLED !== "false";

const alertHistory = [];

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

async function notify(taskName, record) {
  const payload = formatAlertMessage(taskName, record);

  console.error(`[Notifier] ⚠️ 任务失败告警: ${taskName} - ${record.error}`);

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

  alertHistory.unshift(alertRecord);
  if (alertHistory.length > 200) {
    alertHistory.length = 200;
  }
}

taskEventEmitter.on("task_failed", ({ taskName, record }) => {
  notify(taskName, record).catch((err) => {
    console.error(`[Notifier] 告警通知异常: ${err.message}`);
  });
});

function getAlertHistory(limit = 50) {
  return alertHistory.slice(0, limit);
}

module.exports = {
  notify,
  getAlertHistory,
  formatAlertMessage,
};

const express = require("express");
const { createTask, scheduleAll, executeTask, stopTask, enableTask, getTaskInfo, getAllTasks } = require("./taskScheduler");
const { getRecords, getLatestRecord, getAllTaskNames } = require("./taskStore");
const { getAlertHistory } = require("./notifier");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.json());

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function demoSuccessTask() {
  await sleep(500 + Math.random() * 1000);
  if (Math.random() < 0.3) {
    throw new Error("模拟任务随机失败: 数据库连接超时");
  }
}

async function demoDataSyncTask() {
  await sleep(300 + Math.random() * 800);
  if (Math.random() < 0.4) {
    throw new Error("模拟数据同步失败: 远程服务不可用");
  }
}

async function demoCleanupTask() {
  await sleep(200 + Math.random() * 500);
  if (Math.random() < 0.2) {
    throw new Error("模拟清理任务失败: 磁盘空间不足");
  }
}

createTask({
  name: "数据备份",
  cronExpression: "*/1 * * * *",
  handler: demoSuccessTask,
});

createTask({
  name: "数据同步",
  cronExpression: "*/2 * * * *",
  handler: demoDataSyncTask,
});

createTask({
  name: "日志清理",
  cronExpression: "*/3 * * * *",
  handler: demoCleanupTask,
});

app.get("/api/tasks", (req, res) => {
  const tasks = getAllTasks().map((t) => {
    const latest = getLatestRecord(t.name);
    return { ...t, lastExecution: latest };
  });
  res.json({ success: true, data: tasks });
});

app.get("/api/tasks/:name", (req, res) => {
  const { name } = req.params;
  const info = getTaskInfo(name);
  if (!info) {
    return res.status(404).json({ success: false, message: `任务不存在: ${name}` });
  }
  const latest = getLatestRecord(name);
  res.json({ success: true, data: { ...info, lastExecution: latest } });
});

app.get("/api/tasks/:name/history", (req, res) => {
  const { name } = req.params;
  const limit = parseInt(req.query.limit, 10) || 20;
  const records = getRecords(name).slice(0, limit);
  res.json({ success: true, data: records });
});

app.post("/api/tasks/:name/run", async (req, res) => {
  const { name } = req.params;
  try {
    const record = await executeTask(name);
    res.json({ success: true, data: record });
  } catch (err) {
    res.status(500).json({ success: false, message: err.message });
  }
});

app.post("/api/tasks/:name/stop", (req, res) => {
  const { name } = req.params;
  stopTask(name);
  res.json({ success: true, message: `任务已停止: ${name}` });
});

app.post("/api/tasks/:name/enable", (req, res) => {
  const { name } = req.params;
  enableTask(name);
  res.json({ success: true, message: `任务已启用: ${name}` });
});

app.get("/api/alerts", (req, res) => {
  const limit = parseInt(req.query.limit, 10) || 50;
  res.json({ success: true, data: getAlertHistory(limit) });
});

app.get("/api/status", (req, res) => {
  const taskNames = getAllTaskNames();
  const failedTasks = [];
  for (const name of taskNames) {
    const latest = getLatestRecord(name);
    if (latest && latest.status === "failed") {
      failedTasks.push({ name, error: latest.error, finishedAt: latest.finishedAt });
    }
  }
  res.json({
    success: true,
    data: {
      totalTasks: getAllTasks().length,
      recentFailures: failedTasks,
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
    },
  });
});

app.use((err, req, res, _next) => {
  console.error(`[Server] 未捕获异常: ${err.message}`);
  res.status(500).json({ success: false, message: "服务器内部错误" });
});

scheduleAll();

app.listen(PORT, () => {
  console.log(`[Server] 定时任务监控服务已启动: http://localhost:${PORT}`);
  console.log("[Server] 接口列表:");
  console.log(`  GET    /api/tasks            - 获取所有任务状态`);
  console.log(`  GET    /api/tasks/:name      - 获取单个任务状态`);
  console.log(`  GET    /api/tasks/:name/history - 获取任务执行历史`);
  console.log(`  POST   /api/tasks/:name/run  - 手动触发任务执行`);
  console.log(`  POST   /api/tasks/:name/stop - 停止定时调度`);
  console.log(`  POST   /api/tasks/:name/enable - 启用定时调度`);
  console.log(`  GET    /api/alerts           - 获取告警历史`);
  console.log(`  GET    /api/status           - 获取系统状态概览`);
});

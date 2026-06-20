const cron = require("node-cron");
const { addRecord } = require("./taskStore");

const registeredTasks = new Map();

function createTask(config) {
  const { name, cronExpression, handler, enabled = true } = config;

  if (!name || !cronExpression || typeof handler !== "function") {
    throw new Error("任务配置缺少必要字段: name, cronExpression, handler");
  }

  if (!cron.validate(cronExpression)) {
    throw new Error(`无效的 cron 表达式: ${cronExpression}`);
  }

  const task = {
    name,
    cronExpression,
    handler,
    enabled,
    running: false,
    scheduledJob: null,
  };

  registeredTasks.set(name, task);
  return task;
}

async function executeTask(taskName) {
  const task = registeredTasks.get(taskName);
  if (!task) {
    throw new Error(`任务不存在: ${taskName}`);
  }

  if (task.running) {
    const record = {
      status: "skipped",
      startedAt: new Date().toISOString(),
      finishedAt: new Date().toISOString(),
      duration: 0,
      error: "任务正在执行中，跳过本次调度",
    };
    addRecord(taskName, record);
    return record;
  }

  task.running = true;
  const startedAt = new Date();
  const record = {
    status: "running",
    startedAt: startedAt.toISOString(),
    finishedAt: null,
    duration: null,
    error: null,
  };

  try {
    await task.handler();
    const finishedAt = new Date();
    record.status = "success";
    record.finishedAt = finishedAt.toISOString();
    record.duration = finishedAt - startedAt;
  } catch (err) {
    const finishedAt = new Date();
    record.status = "failed";
    record.finishedAt = finishedAt.toISOString();
    record.duration = finishedAt - startedAt;
    record.error = err.message || String(err);
  } finally {
    task.running = false;
    addRecord(taskName, record);
  }

  return record;
}

function scheduleTask(taskName) {
  const task = registeredTasks.get(taskName);
  if (!task) throw new Error(`任务不存在: ${taskName}`);

  if (task.scheduledJob) {
    task.scheduledJob.stop();
  }

  if (!task.enabled) {
    console.log(`[Scheduler] 任务 "${taskName}" 已禁用，跳过调度`);
    return;
  }

  const job = cron.schedule(task.cronExpression, () => {
    console.log(`[Scheduler] 执行定时任务: ${taskName}`);
    executeTask(taskName).catch((err) => {
      console.error(`[Scheduler] 任务 "${taskName}" 执行异常: ${err.message}`);
    });
  });

  task.scheduledJob = job;
  console.log(`[Scheduler] 已注册定时任务: ${taskName} (${task.cronExpression})`);
}

function scheduleAll() {
  for (const [name] of registeredTasks) {
    scheduleTask(name);
  }
}

function stopTask(taskName) {
  const task = registeredTasks.get(taskName);
  if (!task) return;
  if (task.scheduledJob) {
    task.scheduledJob.stop();
    task.scheduledJob = null;
  }
  task.enabled = false;
}

function enableTask(taskName) {
  const task = registeredTasks.get(taskName);
  if (!task) return;
  task.enabled = true;
  scheduleTask(taskName);
}

function getTaskInfo(taskName) {
  const task = registeredTasks.get(taskName);
  if (!task) return null;
  return {
    name: task.name,
    cronExpression: task.cronExpression,
    enabled: task.enabled,
    running: task.running,
  };
}

function getAllTasks() {
  const tasks = [];
  for (const [name] of registeredTasks) {
    tasks.push(getTaskInfo(name));
  }
  return tasks;
}

module.exports = {
  createTask,
  scheduleTask,
  scheduleAll,
  executeTask,
  stopTask,
  enableTask,
  getTaskInfo,
  getAllTasks,
};

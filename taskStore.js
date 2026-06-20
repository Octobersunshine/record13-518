const { EventEmitter } = require("events");

const taskEventEmitter = new EventEmitter();

const taskRecords = new Map();
const MAX_RECORDS_PER_TASK = 100;

function addRecord(taskName, record) {
  if (!taskRecords.has(taskName)) {
    taskRecords.set(taskName, []);
  }
  const records = taskRecords.get(taskName);
  records.unshift(record);
  if (records.length > MAX_RECORDS_PER_TASK) {
    records.length = MAX_RECORDS_PER_TASK;
  }
  if (record.status === "failed") {
    taskEventEmitter.emit("task_failed", { taskName, record });
  }
}

function getRecords(taskName) {
  return taskRecords.get(taskName) || [];
}

function getAllTaskNames() {
  return Array.from(taskRecords.keys());
}

function getLatestRecord(taskName) {
  const records = taskRecords.get(taskName);
  return records && records.length > 0 ? records[0] : null;
}

module.exports = {
  taskEventEmitter,
  addRecord,
  getRecords,
  getAllTaskNames,
  getLatestRecord,
};

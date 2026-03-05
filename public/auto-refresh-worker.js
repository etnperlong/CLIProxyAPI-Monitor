// Web Worker：后台标签页不会被浏览器降频，确保自动刷新计时准确
var timerId = null;
var intervalMs = 60000;

function tick() {
  self.postMessage({ type: "tick" });
  timerId = setTimeout(tick, intervalMs);
}

self.onmessage = function (e) {
  var data = e.data;
  if (data.type === "start") {
    intervalMs = data.interval > 0 ? data.interval : 60000;
    if (timerId !== null) clearTimeout(timerId);
    timerId = setTimeout(tick, intervalMs);
  } else if (data.type === "stop") {
    if (timerId !== null) {
      clearTimeout(timerId);
      timerId = null;
    }
  }
};

// Replays a recording of a real queue on the static page.
//
// The numbers come from record_demo.py, which ran the demo against Redis,
// sampled the counters twice a second, killed a worker partway through and kept
// sampling while the stalled sweep reassigned its jobs. Nothing here invents a
// value: this steps through what happened.
(function () {
  var panel = document.querySelector("[data-replay]");
  if (!panel) return;

  var out = {
    wait: panel.querySelector("[data-r=wait]"),
    active: panel.querySelector("[data-r=active]"),
    completed: panel.querySelector("[data-r=completed]"),
    failed: panel.querySelector("[data-r=failed]"),
    workers: panel.querySelector("[data-r=workers]"),
    totals: panel.querySelector("[data-r=totals]"),
    note: panel.querySelector("[data-r=note]"),
  };
  var button = panel.querySelector("[data-r=toggle]");
  var frames = [];
  var step = 0;
  var timer = null;
  var interval = 500;

  function paint(frame) {
    out.wait.textContent = frame.counts.wait;
    out.active.textContent = frame.counts.active;
    out.completed.textContent = frame.counts.completed;
    out.failed.textContent = frame.counts.failed;
    out.workers.textContent = frame.workers.join(" · ") || "none running";
    out.totals.textContent =
      frame.totals.completed + " completed · " + frame.totals.failed + " failed";
    if (frame.note) {
      out.note.textContent = frame.note;
      out.note.hidden = false;
    }
  }

  function advance() {
    paint(frames[step]);
    step = (step + 1) % frames.length;
    if (step === 0) out.note.hidden = true;
  }

  function play() {
    if (timer) return;
    timer = setInterval(advance, interval);
    button.textContent = "Pause";
  }

  function pause() {
    clearInterval(timer);
    timer = null;
    button.textContent = "Play";
  }

  button.addEventListener("click", function () {
    timer ? pause() : play();
  });

  fetch(panel.dataset.replay)
    .then(function (r) {
      return r.json();
    })
    .then(function (data) {
      frames = data.frames || [];
      if (!frames.length) return;
      interval = (data.sample_seconds || 0.5) * 1000;
      advance();
      button.hidden = false;
      var still = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
      if (still) button.textContent = "Play";
      else play();
    })
    .catch(function () {
      // No recording available: the panel keeps the numbers rendered into it.
    });
})();

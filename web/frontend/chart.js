// Tiny dependency-free line chart on a <canvas>. Plots a numeric series (OHLCV closes)
// with auto-scaled y-axis and a soft gradient fill. No build step, no library.
window.drawChart = function drawChart(canvas, values) {
  const ctx = canvas.getContext("2d");
  const W = canvas.width;
  const H = canvas.height;
  ctx.clearRect(0, 0, W, H);

  if (!values || values.length < 2) {
    ctx.fillStyle = "#8b949e";
    ctx.font = "13px system-ui";
    ctx.fillText("waiting for data…", 12, 24);
    return;
  }

  const pad = 8;
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  const x = (i) => pad + (i / (values.length - 1)) * (W - 2 * pad);
  const y = (v) => H - pad - ((v - min) / range) * (H - 2 * pad);

  const up = values[values.length - 1] >= values[0];
  const stroke = up ? "#3fb950" : "#f85149";

  // gradient fill under the line
  const grad = ctx.createLinearGradient(0, 0, 0, H);
  grad.addColorStop(0, up ? "rgba(63,185,80,0.25)" : "rgba(248,81,73,0.25)");
  grad.addColorStop(1, "rgba(0,0,0,0)");

  ctx.beginPath();
  ctx.moveTo(x(0), y(values[0]));
  values.forEach((v, i) => ctx.lineTo(x(i), y(v)));
  ctx.lineTo(x(values.length - 1), H - pad);
  ctx.lineTo(x(0), H - pad);
  ctx.closePath();
  ctx.fillStyle = grad;
  ctx.fill();

  ctx.beginPath();
  ctx.moveTo(x(0), y(values[0]));
  values.forEach((v, i) => ctx.lineTo(x(i), y(v)));
  ctx.strokeStyle = stroke;
  ctx.lineWidth = 2;
  ctx.stroke();
};

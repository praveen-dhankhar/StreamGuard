import http from "k6/http";
import { check, sleep } from "k6";
import { Rate, Trend } from "k6/metrics";

const targetURL = __ENV.TARGET_URL || "http://localhost:8080/v1/stream";
const apiKey = __ENV.API_KEY || "sg_live_demo";
const model = __ENV.MODEL || "k6-p95-p99";
const vus = Number(__ENV.VUS || "10");
const duration = __ENV.DURATION || "30s";
const p95ThresholdMS = Number(__ENV.P95_THRESHOLD_MS || "2500");
const p99ThresholdMS = Number(__ENV.P99_THRESHOLD_MS || "3500");

export const streamDuration = new Trend("stream_duration", true);
export const streamOK = new Rate("stream_ok");

export const options = {
  vus,
  duration,
  thresholds: {
    "http_req_failed{endpoint:stream}": ["rate<0.01"],
    "http_req_duration{endpoint:stream}": [
      `p(95)<${p95ThresholdMS}`,
      `p(99)<${p99ThresholdMS}`,
    ],
    "stream_duration{endpoint:stream}": [
      `p(95)<${p95ThresholdMS}`,
      `p(99)<${p99ThresholdMS}`,
    ],
    "stream_ok{endpoint:stream}": ["rate>0.99"],
  },
};

export default function () {
  const payload = JSON.stringify({
    model,
    stream: true,
    messages: [{ role: "user", content: "Measure StreamGuard p95 and p99 latency." }],
  });

  const res = http.post(targetURL, payload, {
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    tags: { endpoint: "stream" },
    timeout: "30s",
  });

  const ok = check(res, {
    "status is 200": (r) => r.status === 200,
    "has gateway status": (r) => r.body && r.body.includes("gateway_status"),
    "has streamed content": (r) => r.body && r.body.includes("choices"),
  });

  streamOK.add(ok, { endpoint: "stream" });
  streamDuration.add(res.timings.duration, { endpoint: "stream" });
  sleep(1);
}

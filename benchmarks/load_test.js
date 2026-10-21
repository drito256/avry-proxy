// k6 load testing script for avry-proxy with mixed traffic priorities
import http from 'k6/http';
import { check } from 'k6';

export const options = {
  scenarios: {
    steady_mixed_priority: {
      executor: 'constant-arrival-rate',
      rate: 2000,
      timeUnit: '1s',
      duration: '30s',
      preAllocatedVUs: 64,
      maxVUs: 256,
    },
  },
  thresholds: {
    http_req_duration: ['p(95)<50', 'p(99)<100'],
  },
};

const TARGET_URL = __ENV.PROXY_URL || 'http://localhost:8080/work';
const PRIORITIES = ['critical', 'high', 'high', 'high', 'low'];

export default function () {
  const prio = PRIORITIES[__ITER % PRIORITIES.length];
  const res = http.get(TARGET_URL, {
    headers: {
      'X-Traffic-Priority': prio,
      'X-API-Key': `client-${__VU}`,
    },
  });

  check(res, {
    'status is 200': (r) => r.status === 200,
  });
}

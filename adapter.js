import { createServer } from 'http';
import Redis from 'ioredis';

/**
 * CPA Metrics Adapter
 * 
 * 一个轻量级的中间件，用于从 CPA 的 Redis 队列中拉取使用数据，
 * 并将其重新聚合成兼容本项目（或其他工具）的 HTTP /usage 格式。
 * 
 * 运行方式: node adapter.js
 */

const CONFIG = {
  // CPA 管理端口的 Redis 地址
  redis: {
    host: process.env.CPA_REDIS_HOST || '127.0.0.1',
    port: parseInt(process.env.CPA_REDIS_PORT || '8317'),
    password: process.env.CPA_SECRET_KEY || '', // 对应 remote-management.secret-key
    key: process.env.CPA_REDIS_KEY || 'queue',
  },
  // 本适配器监听的端口
  port: parseInt(process.env.ADAPTER_PORT || '3001'),
  // 轮询间隔 (毫秒)
  pollInterval: parseInt(process.env.POLL_INTERVAL || '15000'),
  // 内存中保留的最大记录数
  maxBufferSize: parseInt(process.env.MAX_BUFFER_SIZE || '5000'),
  // 每次拉取的最大记录数
  batchSize: parseInt(process.env.BATCH_SIZE || '100'),
  // 访问 /usage 后是否清空内存缓冲区；true=增量导出，false=保留全量内存快照
  clearBufferOnRead: (process.env.CLEAR_BUFFER_ON_READ || 'false').toLowerCase() === 'true',
};

// 内存缓冲区，用于存放最近拉取的记录
let usageBuffer = [];

// 初始化 Redis 客户端
const redis = new Redis({
  host: CONFIG.redis.host,
  port: CONFIG.redis.port,
  password: CONFIG.redis.password,
  lazyConnect: true,
  retryStrategy: (times) => Math.min(times * 50, 2000),
});

function normalizeRecord(record) {
  if (!record || typeof record !== 'object') return null;

  const model = typeof record.model === 'string' && record.model.trim() ? record.model.trim() : 'unknown';
  const endpoint = typeof record.endpoint === 'string' && record.endpoint.trim() ? record.endpoint.trim() : 'default';
  const source = typeof record.source === 'string' ? record.source : '';
  const timestamp = typeof record.timestamp === 'string' && record.timestamp.trim()
    ? record.timestamp
    : new Date().toISOString();
  const auth_index = record.auth_index == null ? null : String(record.auth_index).trim() || null;
  const failed = Boolean(record.failed);
  const tokens = record.tokens && typeof record.tokens === 'object' ? record.tokens : {};

  const input = Number(tokens.input_tokens || 0);
  const output = Number(tokens.output_tokens || 0);
  const cached = Number(tokens.cached_tokens || 0);
  const reasoning = Number(tokens.reasoning_tokens || 0);
  const total = Number(tokens.total_tokens || (input + output + reasoning));

  return {
    ...record,
    model,
    endpoint,
    source,
    timestamp,
    auth_index,
    failed,
    tokens: {
      ...tokens,
      input_tokens: Number.isFinite(input) ? input : 0,
      output_tokens: Number.isFinite(output) ? output : 0,
      cached_tokens: Number.isFinite(cached) ? cached : 0,
      reasoning_tokens: Number.isFinite(reasoning) ? reasoning : 0,
      total_tokens: Number.isFinite(total) ? total : 0,
    }
  };
}

/**
 * 从 Redis 拉取并聚合数据
 */
async function drainQueue() {
  try {
    if (redis.status !== 'ready') {
      await redis.connect();
    }

    // 使用 LPOP count 拉取数据
    const rawData = await redis.lpop(CONFIG.redis.key, CONFIG.batchSize);

    if (!rawData) return;

    const records = Array.isArray(rawData) ? rawData : [rawData];
    const parsedRecords = [];

    for (const rawRecord of records) {
      try {
        const parsed = JSON.parse(rawRecord);
        const normalized = normalizeRecord(parsed);
        if (!normalized) {
          console.error('Skipped invalid record:', rawRecord);
          continue;
        }
        parsedRecords.push(normalized);
      } catch (e) {
        console.error('Failed to parse record:', rawRecord);
      }
    }

    if (parsedRecords.length > 0) {
      usageBuffer.push(...parsedRecords);
      // 保持缓冲区大小
      if (usageBuffer.length > CONFIG.maxBufferSize) {
        usageBuffer = usageBuffer.slice(-CONFIG.maxBufferSize);
      }
      console.log(`[${new Date().toISOString()}] Drained ${parsedRecords.length} records. Buffer: ${usageBuffer.length}`);
    }
  } catch (err) {
    console.error('Drain error:', err.message);
  }
}

// 定时任务
setInterval(drainQueue, CONFIG.pollInterval);
drainQueue();

/**
 * 将内存缓冲区的数据转换为旧版 /usage 聚合格式
 */
function getAggregatedUsage() {
  const result = {
    usage: {
      total_tokens: 0,
      apis: {}
    },
    meta: {
      buffer_size: usageBuffer.length,
      clear_on_read: CONFIG.clearBufferOnRead,
    }
  };

  for (const record of usageBuffer) {
    const { model, endpoint, tokens, timestamp, source, auth_index, failed } = record;
    const route = endpoint || 'default';
    const t = tokens || {};
    
    const input = t.input_tokens || 0;
    const output = t.output_tokens || 0;
    const cached = t.cached_tokens || 0;
    const reasoning = t.reasoning_tokens || 0;
    const total = t.total_tokens || (input + output + reasoning);

    result.usage.total_tokens += total;

    if (!result.usage.apis[route]) {
      result.usage.apis[route] = { total_tokens: 0, models: {} };
    }

    const api = result.usage.apis[route];
    api.total_tokens += total;

    if (!api.models[model]) {
      api.models[model] = {
        total_tokens: 0,
        input_tokens: 0,
        output_tokens: 0,
        cached_tokens: 0,
        reasoning_tokens: 0,
        details: []
      };
    }

    const m = api.models[model];
    m.total_tokens += total;
    m.input_tokens += input;
    m.output_tokens += output;
    m.cached_tokens += cached;
    m.reasoning_tokens += reasoning;
    
    m.details.push({
      timestamp,
      source,
      auth_index,
      tokens: t,
      failed: !!failed
    });
  }

  return result;
}

// 创建 HTTP 服务
const server = createServer((req, res) => {
  if (req.url === '/usage' || req.url === '/v0/management/usage') {
    const data = getAggregatedUsage();
    
    if (CONFIG.clearBufferOnRead) {
      usageBuffer = [];
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(data));
  } else {
    res.writeHead(404);
    res.end();
  }
});

server.listen(CONFIG.port, () => {
  console.log(`Adapter running at http://localhost:${CONFIG.port}`);
  console.log(`Polling CPA Redis at ${CONFIG.redis.host}:${CONFIG.redis.port}`);
  console.log(`Redis queue key: ${CONFIG.redis.key}`);
  console.log(`Clear buffer on read: ${CONFIG.clearBufferOnRead}`);
});

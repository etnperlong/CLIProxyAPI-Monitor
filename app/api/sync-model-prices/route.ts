import { NextResponse } from "next/server";
import { cookies } from "next/headers";
import { config } from "@/lib/config";
import { db } from "@/lib/db/client";
import { modelPrices } from "@/lib/db/schema";

export const runtime = "nodejs";

const PASSWORD = process.env.PASSWORD || process.env.CLIPROXY_SECRET_KEY || "";
const COOKIE_NAME = "dashboard_auth";

function unauthorized() {
  return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
}

async function hashPassword(value: string) {
  const data = new TextEncoder().encode(value);
  const hashBuffer = await crypto.subtle.digest("SHA-256", data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function isAuthorized(request: Request) {
  // 检查 Bearer token（用于 cron job 等外部调用）
  const allowed = [config.password, config.cronSecret].filter(Boolean).map((v) => `Bearer ${v}`);
  if (allowed.length > 0) {
    const auth = request.headers.get("authorization") || "";
    if (allowed.includes(auth)) return true;
  }
  
  // 检查用户的 dashboard cookie（用于前端调用）
  if (PASSWORD) {
    const cookieStore = await cookies();
    const authCookie = cookieStore.get(COOKIE_NAME);
    if (authCookie) {
      const expectedToken = await hashPassword(PASSWORD);
      if (authCookie.value === expectedToken) return true;
    }
  }
  
  return false;
}

type ModelsDevModel = {
  id: string;
  cost?: { input?: number; output?: number; cache_read?: number };
};

type ModelsDevProvider = {
  models: Record<string, ModelsDevModel>;
};

type ModelsDevResponse = Record<string, ModelsDevProvider>;

export async function POST(request: Request) {
  try {
    // 🔒 鉴权检查
    if (!(await isAuthorized(request))) {
      return unauthorized();
    }

    // 使用服务端配置的 API Key，而不是客户端传入
    const apiKey = config.cliproxy.apiKey;
    if (!apiKey) {
      return NextResponse.json({ error: "服务端未配置 CLIPROXY_SECRET_KEY" }, { status: 500 });
    }

    const envBaseUrl = process.env.CLIPROXY_API_BASE_URL || "";
    if (!envBaseUrl) {
      return NextResponse.json({ error: "服务端未配置 CLIPROXY_API_BASE_URL" }, { status: 500 });
    }

    if (!config.postgresUrl) {
      return NextResponse.json({ error: "服务端未配置 DATABASE_URL" }, { status: 500 });
    }

    const baseUrl = envBaseUrl.replace(/\/v0\/management\/?$/, "").replace(/\/$/, "");

    // 1. 从 models.dev 获取价格数据
    const modelsDevRes = await fetch("https://models.dev/api.json", {
      headers: { "Accept": "application/json" },
      cache: "no-store"
    });

    if (!modelsDevRes.ok) {
      return NextResponse.json({ error: `无法获取 models.dev 数据: ${modelsDevRes.status}` }, { status: 502 });
    }

    const modelsDevData: ModelsDevResponse = await modelsDevRes.json();

    // 2. 构建模型ID到价格的映射
    const priceMap = new Map<string, { input: number; output: number; cached: number }>();
    for (const provider of Object.values(modelsDevData)) {
      if (!provider.models) continue;
      for (const model of Object.values(provider.models)) {
        if (model.cost && (model.cost.input || model.cost.output)) {
          priceMap.set(model.id, {
            input: model.cost.input ?? 0,
            output: model.cost.output ?? 0,
            cached: model.cost.cache_read ?? 0
          });
        }
      }
    }

    // 3. 从 CLIProxyAPI 获取当前模型列表
    const modelsUrl = `${baseUrl}/v1/models`;
    const cliproxyRes = await fetch(modelsUrl, {
      headers: { "Authorization": `Bearer ${apiKey}`, "Accept": "application/json" },
      cache: "no-store"
    });

    if (!cliproxyRes.ok) {
      return NextResponse.json({ error: `无法获取模型列表: ${cliproxyRes.status}` }, { status: 502 });
    }

    const cliproxyData = await cliproxyRes.json();
    const models: { id: string }[] = cliproxyData.data || [];

    // 4. 匹配并更新价格到本地数据库
    let updatedCount = 0;
    let skippedCount = 0;
    let failedCount = 0;
    const details: { model: string; status: string; matchedWith?: string; reason?: string }[] = [];

    for (const { id: modelId } of models) {
      let priceInfo = priceMap.get(modelId);
      let matchedKey = modelId;

      // 尝试去掉前缀匹配
      if (!priceInfo) {
        const simpleName = modelId.split("/").pop() || modelId;
        priceInfo = priceMap.get(simpleName);
        if (priceInfo) matchedKey = simpleName;
      }

      // 模糊匹配
      if (!priceInfo) {
        const baseModelName = modelId.replace(/-\d{4,}.*$/, "").replace(/@.*$/, "");
        for (const [key, value] of priceMap.entries()) {
          if (key.includes(baseModelName) || baseModelName.includes(key)) {
            priceInfo = value;
            matchedKey = key;
            break;
          }
        }
      }

      if (!priceInfo) {
        skippedCount++;
        details.push({ model: modelId, status: "skipped", reason: "未找到价格信息" });
        continue;
      }

      try {
        await db.insert(modelPrices).values({
          model: modelId,
          inputPricePer1M: String(priceInfo.input),
          cachedInputPricePer1M: String(priceInfo.cached),
          outputPricePer1M: String(priceInfo.output)
        }).onConflictDoUpdate({
          target: modelPrices.model,
          set: {
            inputPricePer1M: String(priceInfo.input),
            cachedInputPricePer1M: String(priceInfo.cached),
            outputPricePer1M: String(priceInfo.output)
          }
        });
        updatedCount++;
        details.push({ model: modelId, status: "updated", matchedWith: matchedKey });
      } catch (err) {
        failedCount++;
        details.push({ model: modelId, status: "failed", reason: err instanceof Error ? err.message : "数据库写入失败" });
      }
    }

    return NextResponse.json({
      success: true,
      summary: { total: models.length, updated: updatedCount, skipped: skippedCount, failed: failedCount },
      details
    });

  } catch (error) {
    console.error("/api/sync-model-prices POST failed:", error);
    return NextResponse.json({ error: error instanceof Error ? error.message : "内部服务器错误" }, { status: 500 });
  }
}

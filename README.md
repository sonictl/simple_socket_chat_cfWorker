# 💬 Simple Socket Chat - Cloudflare Worker

一个基于 **Cloudflare Workers + Durable Objects** 构建的实时 WebSocket 聊天室应用。支持在线人数统计、昵称持久化，适配桌面端和移动端。

---

## 📋 功能描述

- **实时聊天** — 基于 WebSocket 的全双工通信，消息即时推送
- **昵称系统** — 首次进入需输入昵称，自动保存到 `localStorage`，后续免输入
- **在线人数** — 顶部实时显示当前房间在线人数
- **多行消息** — 桌面端 Enter 发送 / Shift+Enter 换行，移动端 Enter 换行 / 按钮发送
- **系统通知** — 有人加入/离开房间时自动提示
- **响应式布局** — 适配桌面端和移动端浏览器

---

## 🛠️ 技术栈

| 工具/技术 | 用途 |
|-----------|------|
| [Cloudflare Workers](https://workers.cloudflare.com/) | 无服务器计算平台，运行 Worker 代码 |
| [Durable Objects](https://developers.cloudflare.com/durable-objects/) | 有状态对象，管理 WebSocket 连接和房间状态 |
| [Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/) | Cloudflare Workers 官方 CLI 工具，用于开发、调试和部署 |
| [TypeScript](https://www.typescriptlang.org/) | 类型安全的 JavaScript 超集 |
| [WebSocket API](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket) | 浏览器原生实时通信协议 |
| [npm](https://www.npmjs.com/) | Node.js 包管理器 |

---

## 🏗️ 实现方案

### 核心架构

```
┌─────────────────────────────────────────────────────────┐
│                    用户浏览器 (Client)                     │
│  ┌──────────────────────────────────────────────────┐   │
│  │              WebSocket 连接 (wss://)              │   │
│  └──────────────┬───────────────────────┬───────────┘   │
│                 │                       │               │
│          HTTP 请求                   WebSocket 升级      │
│         (返回 HTML 页面)           (建立双向通信)        │
└─────────────────┼───────────────────────┼───────────────┘
                  │                       │
                  ▼                       ▼
┌─────────────────────────────────────────────────────────┐
│              Cloudflare Workers (Worker 入口)             │
│                                                         │
│  1. 普通 HTTP 请求 → 返回前端 HTML 页面                   │
│  2. WebSocket 升级请求 → 路由到 Durable Object            │
└─────────────────────────┬───────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│              Durable Object (ChatRoom)                   │
│                                                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │
│  │  WebSocket  │  │  WebSocket  │  │  WebSocket  │ ...  │
│  │  (用户 A)   │  │  (用户 B)   │  │  (用户 C)   │     │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘     │
│         │                │                │             │
│         └────────────────┼────────────────┘             │
│                   消息广播 (Broadcast)                    │
│                                                         │
│  功能:                                                  │
│  - 管理 WebSocket 连接集合                               │
│  - 消息广播（发送者除外）                                 │
│  - 在线人数统计与推送                                    │
│  - 加入/离开系统通知                                     │
└─────────────────────────────────────────────────────────┘
```

### 数据流

1. 用户访问 Worker URL → Worker 返回 HTML 页面
2. 页面加载后，JavaScript 建立 WebSocket 连接（自动升级协议）
3. Worker 检测到 WebSocket 升级请求 → 路由到 Durable Object 实例
4. Durable Object 接受连接，管理客户端集合
5. 用户发送消息 → WebSocket → Durable Object → 广播给房间内其他用户
6. 用户加入/离开 → Durable Object 更新人数并广播系统消息

---

## 📝 实现步骤

### 1. 创建 Worker 项目

```bash
npm create cloudflare@latest simple-chat-app
cd simple-chat-app
```

选择：
 - Helloworld Example
 - Worker + Durable Objects
 - TypeScript（推荐）

### 2. 配置 wrangler.jsonc

```jsonc
{
  "name": "simple-chat-app",
  "main": "src/index.ts",
  "compatibility_date": "2025-01-01",
  "durable_objects": {
    "bindings": [
      {
        "name": "CHAT_ROOM",
        "class_name": "ChatRoom"
      }
    ]
  },
  "migrations": [
    {
      "tag": "v1",
      "new_sqlite_classes": ["ChatRoom"]
    }
  ]
}
```

### 3. Worker 入口 — 路由分发

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const upgradeHeader = request.headers.get("Upgrade");

    // WebSocket 升级请求 -> 路由到 Durable Object
    if (upgradeHeader === "websocket") {
      const url = new URL(request.url);
      const roomId = url.searchParams.get("room") || "global";
      const id = env.CHAT_ROOM.idFromName(roomId);
      const stub = env.CHAT_ROOM.get(id);
      return stub.fetch(request);
    }

    // 普通 HTTP 请求 -> 返回前端页面
    return new Response(html, {
      headers: { "Content-Type": "text/html;charset=UTF-8" },
    });
  },
};
```

### 4. Durable Object — 聊天室核心逻辑

```typescript
export class ChatRoom {
  state: DurableObjectState;
  clients: Map<WebSocket, string>; // ws -> nickname

  constructor(state: DurableObjectState) {
    this.state = state;
    this.clients = new Map();
  }

  async fetch(request: Request): Promise<Response> {
    // 处理 WebSocket 升级
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.handleSocket(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  handleSocket(ws: WebSocket) {
    ws.accept();
    // 监听消息、连接关闭事件
    // 广播消息给其他客户端
    // 更新在线人数
  }
}
```

### 5. 前端页面

前端 HTML 内嵌在 Worker 中返回，包含：
- 昵称输入弹窗（首次访问）
- 消息列表（支持多行文本）
- 输入框（桌面端 Enter 发送，移动端按钮发送）
- 在线人数显示
- 连接状态指示

### 6. 生成类型定义

```bash
npx wrangler types
```

### 7. 安装依赖

```bash
npm install --save-dev @cloudflare/workers-types
```

---

## 🚀 部署到 Cloudflare Workers

### 前提条件

1. 拥有 [Cloudflare 账号](https://dash.cloudflare.com/sign-up)
2. 安装 Node.js (v18+)
3. 登录 Wrangler:

```bash
npx wrangler login
```

### 本地开发

```bash
npm run dev
# 访问 http://localhost:8787
```

### 部署到生产环境

```bash
npm run deploy
```

部署成功后，终端会显示你的 Worker URL，例如：
```
https://simple-chat-app.mawefd.workers.dev
```

### 自定义域名（可选）

在 Cloudflare Dashboard 中，进入 Workers & Pages → 你的 Worker → Domains，添加你的域名。

---

## ☁️ Cloudflare Workers 基础介绍

### 什么是 Cloudflare Workers？

Cloudflare Workers 是一个**无服务器计算平台**，允许你在 Cloudflare 全球 330+ 个数据中心运行 JavaScript/TypeScript 代码。它类似于 AWS Lambda，但部署在边缘节点上，延迟极低。

### 核心概念

| 概念 | 说明 |
|------|------|
| **Worker** | 一个无状态函数，处理 HTTP 请求并返回响应 |
| **Durable Objects** | 有状态对象，提供强一致性存储和 WebSocket 支持 |
| **Wrangler** | 官方 CLI 工具，用于开发、测试和部署 |
| **KV** | 键值存储（本项目中未使用） |
| **R2** | 对象存储（本项目中未使用） |

### Workers 的优势

- 🌍 **全球边缘部署** — 代码运行在离用户最近的数据中心
- ⚡ **极低延迟** — 冷启动 < 5ms
- 💰 **免费额度** — 每天 10 万次请求免费
- 🔌 **丰富的集成** — 与 Cloudflare 生态（DNS、CDN、D1 数据库等）无缝集成
- 📦 **零运维** — 无需管理服务器

### 免费计划限制

| 资源 | 限制 |
|------|------|
| 请求数 | 10 万/天 |
| CPU 时间 | 10ms/请求 |
| 内存 | 128MB |
| Durable Objects | 支持（需使用 `new_sqlite_classes` 迁移） |

---

## 📁 项目结构

```
simple-chat-app/
├── src/
│   └── index.ts          # Worker 入口 + Durable Object
├── public/
│   └── index.html        # 前端页面（备用，实际内嵌在 Worker 中）
├── wrangler.jsonc        # Cloudflare Workers 配置
├── tsconfig.json         # TypeScript 配置
├── worker-configuration.d.ts  # 自动生成的类型定义
├── package.json          # 项目依赖
└── .gitignore            # Git 忽略规则
```

---

## 📄 License

MIT

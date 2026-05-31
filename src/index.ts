export class ChatRoom {
  state: DurableObjectState;
  clients: Map<WebSocket, string>; // ws -> nickname

  constructor(state: DurableObjectState) {
    this.state = state;
    this.clients = new Map();
  }

  async fetch(request: Request): Promise<Response> {
    const upgradeHeader = request.headers.get("Upgrade");

    if (upgradeHeader !== "websocket") {
      return new Response("Expected WebSocket", { status: 400 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    this.handleSocket(server);

    return new Response(null, {
      status: 101,
      webSocket: client,
    });
  }

  broadcast(data: object, excludeWs?: WebSocket) {
    const msg = JSON.stringify(data);
    for (const ws of this.clients.keys()) {
      if (ws !== excludeWs) {
        ws.send(msg);
      }
    }
  }

  broadcastPeopleCount() {
    this.broadcast({
      type: "people_count",
      count: this.clients.size,
    });
  }

  // Heartbeat interval handle
  heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  // Track last pong time per WebSocket (timestamp in ms)
  lastPong: Map<WebSocket, number> = new Map();

  // Heartbeat interval (ms)
  HEARTBEAT_INTERVAL = 30_000; // 30s
  // Timeout after which a client is considered dead (ms)
  HEARTBEAT_TIMEOUT = 60_000; // 60s

  startHeartbeat() {
    if (this.heartbeatTimer) return;

    this.heartbeatTimer = setInterval(() => {
      const now = Date.now();

      // Check for dead clients
      for (const [ws, lastPongTime] of this.lastPong.entries()) {
        if (now - lastPongTime > this.HEARTBEAT_TIMEOUT) {
          // Client hasn't responded in time, close the connection
          try {
            ws.close(1000, "Heartbeat timeout");
          } catch {
            // Already closed
          }
          // Clean up will happen in the 'close' event handler
        }
      }

      // Send ping to all connected clients
      this.broadcast({ type: "ping" });
    }, this.HEARTBEAT_INTERVAL);
  }

  stopHeartbeat() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  handleSocket(ws: WebSocket) {
    ws.accept();

    // Initialize last pong time for this client
    this.lastPong.set(ws, Date.now());

    // Start heartbeat if not already running
    this.startHeartbeat();

    ws.addEventListener("message", (msg: MessageEvent) => {
      let data: any;
      try {
        data = JSON.parse(msg.data);
      } catch {
        return;
      }

      if (data.type === "pong") {
        // Update last pong time for this client
        this.lastPong.set(ws, Date.now());
        return;
      }

      if (data.type === "join") {
        const nickname = data.nickname || "Anonymous";
        this.clients.set(ws, nickname);
        this.broadcastPeopleCount();
        this.broadcast({
          type: "system",
          text: `${nickname} joined the room`,
        });
      } else if (data.type === "message") {
        const nickname = this.clients.get(ws) || "Anonymous";
        this.broadcast(
          {
            type: "chat",
            nickname,
            text: data.text,
          },
          ws
        );
        ws.send(
          JSON.stringify({
            type: "chat",
            nickname,
            text: data.text,
            self: true,
          })
        );
      } else if (data.type === "image") {
        const nickname = this.clients.get(ws) || "Anonymous";
        const imageData = {
          type: "image",
          nickname,
          imageId: data.imageId,
          mimeType: data.mimeType,
          fileName: data.fileName,
          fileSize: data.fileSize,
          width: data.width,
          height: data.height,
        };
        this.broadcast(imageData, ws);
        ws.send(JSON.stringify({ ...imageData, self: true }));
      } else if (data.type === "file") {
        const nickname = this.clients.get(ws) || "Anonymous";
        const fileData = {
          type: "file",
          nickname,
          fileId: data.fileId,
          fileName: data.fileName,
          fileSize: data.fileSize,
          mimeType: data.mimeType,
        };
        this.broadcast(fileData, ws);
        ws.send(JSON.stringify({ ...fileData, self: true }));
      }
    });

    ws.addEventListener("close", () => {
      const nickname = this.clients.get(ws) || "Anonymous";
      this.clients.delete(ws);
      this.lastPong.delete(ws);
      this.broadcast({
        type: "system",
        text: `${nickname} left the room`,
      });
      this.broadcastPeopleCount();

      // Stop heartbeat if no more clients
      if (this.clients.size === 0) {
        this.stopHeartbeat();
      }
    });
  }
}

// Default max file size (50MB) and TTL (10 minutes)
let maxFileSize = 50 * 1024 * 1024;
let fileTTL = 600; // seconds (10 minutes)

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    // Admin route
    if (path === "/admin") {
      return handleAdminRoute(request, url, env);
    }

    // API settings route
    if (path === "/api/settings") {
      return handleApiSettings(request, env);
    }

    // File upload route
    if (path === "/upload") {
      return handleFileUpload(request, env);
    }

    // File download route
    if (path.startsWith("/download/")) {
      const fileId = path.slice("/download/".length);
      return handleFileDownload(fileId, env);
    }

    // Image download route
    if (path.startsWith("/image/")) {
      const imageId = path.slice("/image/".length);
      return handleImageDownload(imageId, env);
    }

    const upgradeHeader = request.headers.get("Upgrade");

    // WebSocket upgrade
    if (upgradeHeader === "websocket") {
      const roomId = url.searchParams.get("room") || "global";
      const id = env.CHAT_ROOM.idFromName(roomId);
      const stub = env.CHAT_ROOM.get(id);
      return stub.fetch(request);
    }

    // Serve the frontend HTML
    const html = getFrontendHTML();
    return new Response(html, {
      headers: { "Content-Type": "text/html;charset=UTF-8" },
    });
  },

  // Cron trigger: clean up expired files every 6 hours
  async scheduled(event: ScheduledEvent, env: Env, ctx: ExecutionContext): Promise<void> {
    const now = Date.now();
    const maxAge = fileTTL * 1000; // Convert seconds to ms
    let deletedCount = 0;
    let errorCount = 0;

    try {
      // List all objects in the bucket with pagination
      let cursor: string | undefined;
      let hasMore = true;

      while (hasMore) {
        const listOptions: R2ListOptions = {
          limit: 1000,
        };
        if (cursor) {
          listOptions.cursor = cursor;
        }

        const listResult = await env.CHAT_FILES.list(listOptions);

        for (const object of listResult.objects) {
          try {
            // Check if the object has an uploadedAt timestamp
            const head = await env.CHAT_FILES.head(object.key);
            if (head?.customMetadata?.uploadedAt) {
              const uploadedAt = parseInt(head.customMetadata.uploadedAt, 10);
              if (!isNaN(uploadedAt) && (now - uploadedAt) > maxAge) {
                // File is expired, delete it
                await env.CHAT_FILES.delete(object.key);
                deletedCount++;
              }
            } else if (head?.uploaded) {
              // Fallback: use the R2 object's uploaded timestamp
              const uploadedAt = head.uploaded.getTime();
              if ((now - uploadedAt) > maxAge) {
                await env.CHAT_FILES.delete(object.key);
                deletedCount++;
              }
            }
          } catch {
            errorCount++;
          }
        }

        if (listResult.truncated) {
          cursor = listResult.cursor;
        } else {
          hasMore = false;
        }
      }
    } catch (err) {
      console.error('Cleanup cron error:', err);
    }

    console.log(`Cleanup complete: deleted ${deletedCount} expired files, ${errorCount} errors`);
  },
};

function handleAdminRoute(request: Request, url: URL, env: Env): Response {
  const html = getAdminHTML(env);
  return new Response(html, {
    headers: { "Content-Type": "text/html;charset=UTF-8" },
  });
}

async function handleApiSettings(request: Request, env: Env): Promise<Response> {
  if (request.method === "GET") {
    return new Response(
      JSON.stringify({ maxFileSize, fileTTL }),
      {
        headers: { "Content-Type": "application/json" },
      }
    );
  }

  if (request.method === "POST") {
    try {
      const body: any = await request.json();
      const adminPass = env.ADMIN_PASS || "admin_password";
      if (body.password !== adminPass) {
        return new Response(
          JSON.stringify({ success: false, error: "Unauthorized" }),
          { status: 401, headers: { "Content-Type": "application/json" } }
        );
      }
      if (body.maxFileSize !== undefined) {
        const newSize = parseInt(body.maxFileSize, 10);
        if (isNaN(newSize) || newSize < 1024 * 1024) {
          return new Response(
            JSON.stringify({ success: false, error: "Invalid size (min 1 MB)" }),
            { status: 400, headers: { "Content-Type": "application/json" } }
          );
        }
        maxFileSize = newSize;
      }
      if (body.fileTTL !== undefined) {
        const newTTL = parseInt(body.fileTTL, 10);
        if (isNaN(newTTL) || newTTL < 60 || newTTL > 86400) {
          return new Response(
            JSON.stringify({ success: false, error: "Invalid TTL (60-86400 seconds)" }),
            { status: 400, headers: { "Content-Type": "application/json" } }
          );
        }
        fileTTL = newTTL;
      }
      return new Response(
        JSON.stringify({ success: true, maxFileSize, fileTTL }),
        {
          headers: { "Content-Type": "application/json" },
        }
      );
    } catch {
      return new Response(
        JSON.stringify({ success: false, error: "Invalid request" }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }
  }

  return new Response("Method not allowed", { status: 405 });
}

async function handleFileUpload(request: Request, env: Env): Promise<Response> {
  if (request.method !== "POST") {
    return new Response("Method not allowed", { status: 405 });
  }

  try {
    const formData = await request.formData();
    const file = formData.get("file") as File | null;

    if (!file) {
      return new Response(JSON.stringify({ error: "No file provided" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }

    // Check file size
    if (file.size > maxFileSize) {
      return new Response(
        JSON.stringify({
          error: `File too large. Maximum allowed size is ${formatFileSize(maxFileSize)}`,
          maxSize: maxFileSize,
        }),
        {
          status: 413,
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    const fileId = crypto.randomUUID();
    const arrayBuffer = await file.arrayBuffer();

    // Store in R2 with custom metadata and TTL
    // Use expiresIn for automatic deletion after fileTTL seconds
    // Note: expiresIn is supported at runtime by Cloudflare R2
    await env.CHAT_FILES.put(fileId, arrayBuffer, {
      httpMetadata: {
        contentType: file.type || "application/octet-stream",
        contentDisposition: `attachment; filename="${file.name.replace(/[^\x20-\x7E]/g, '_')}"`,
      },
      customMetadata: {
        fileName: file.name,
        mimeType: file.type || "application/octet-stream",
        uploadedAt: Date.now().toString(),
      },
      expiresIn: fileTTL,
    } as any);

    return new Response(
      JSON.stringify({
        fileId,
        fileName: file.name,
        fileSize: arrayBuffer.byteLength,
        mimeType: file.type || "application/octet-stream",
      }),
      {
        headers: { "Content-Type": "application/json" },
      }
    );
  } catch (err) {
    return new Response(JSON.stringify({ error: "Upload failed" }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
}

async function handleFileDownload(fileId: string, env: Env): Promise<Response> {
  const object = await env.CHAT_FILES.get(fileId);
  if (!object) {
    return new Response("File not found", { status: 404 });
  }

  const fileName = object.customMetadata?.fileName || "download";
  const mimeType = object.customMetadata?.mimeType || "application/octet-stream";
  const safeFilename = fileName.replace(/[^\x20-\x7E]/g, '_');

  return new Response(object.body, {
    headers: {
      "Content-Type": mimeType,
      "Content-Disposition": `attachment; filename="${safeFilename}"`,
      "Content-Length": object.size.toString(),
      "Cache-Control": "public, max-age=31536000",
    },
  });
}

async function handleImageDownload(imageId: string, env: Env): Promise<Response> {
  const object = await env.CHAT_FILES.get(imageId);
  if (!object) {
    return new Response("Image not found", { status: 404 });
  }

  const mimeType = object.customMetadata?.mimeType || "image/png";

  return new Response(object.body, {
    headers: {
      "Content-Type": mimeType,
      "Cache-Control": "public, max-age=31536000",
      "Access-Control-Allow-Origin": "*",
    },
  });
}

function formatFileSize(bytes: number): string {
  if (bytes >= 1024 * 1024 * 1024) {
    return (bytes / (1024 * 1024 * 1024)).toFixed(1) + " GB";
  }
  if (bytes >= 1024 * 1024) {
    return (bytes / (1024 * 1024)).toFixed(1) + " MB";
  }
  if (bytes >= 1024) {
    return (bytes / 1024).toFixed(1) + " KB";
  }
  return bytes + " B";
}

function getFrontendHTML(): string {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Chat App</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a2e; color: #eee; height: 100dvh; display: flex; justify-content: center; align-items: flex-start; }
    .chat-container { width: 100%; max-width: 480px; height: 100dvh; background: #16213e; display: flex; flex-direction: column; position: relative; }
    @media (min-width: 768px) {
      body { align-items: center; padding: 20px 0; }
      .chat-container { width: 400px; height: 600px; border-radius: 12px; box-shadow: 0 8px 32px rgba(0,0,0,0.3); }
    }
    .chat-header { padding: 12px 20px; background: #0f3460; text-align: center; font-weight: 600; font-size: 18px; cursor: pointer; user-select: none; position: relative; z-index: 5; transition: background 0.2s; }
    .chat-header:hover { background: #1a4a7a; }
    .chat-header .people-count { font-size: 13px; font-weight: 400; color: #8ab4f8; margin-top: 2px; }
    .chat-header .dropdown-arrow { font-size: 12px; margin-left: 6px; display: inline-block; transition: transform 0.3s; }
    .chat-header .dropdown-arrow.open { transform: rotate(180deg); }

    .dropdown-panel {
      position: absolute; top: 100%; left: 0; right: 0;
      height: 0; overflow: hidden;
      background: #0f3460;
      z-index: 4;
      transition: height 0.35s cubic-bezier(0.4, 0, 0.2, 1);
      border-bottom: 2px solid #1a4a7a;
    }
    .dropdown-panel.open {
      height: var(--dropdown-target-height, 300px);
    }
    @media (min-width: 768px) {
      .dropdown-panel.open { border-radius: 0 0 12px 12px; }
    }
    .dropdown-panel-inner {
      display: flex; flex-direction: column; height: 100%; padding: 16px; gap: 12px;
      overflow-y: auto;
    }
    .dropdown-section {
      background: #1a1a40; border-radius: 10px; padding: 16px; flex: 1;
      display: flex; flex-direction: column;
    }
    .dropdown-section h3 {
      font-size: 14px; color: #8ab4f8; margin-bottom: 12px;
      display: flex; align-items: center; gap: 8px;
    }
    .dropdown-section h3 .icon { font-size: 18px; }

    .image-drop-zone {
      flex: 1; border: 2px dashed #0f3460; border-radius: 8px;
      display: flex; flex-direction: column; justify-content: center; align-items: center;
      gap: 8px; cursor: pointer; transition: all 0.2s; padding: 12px;
      min-height: 80px; position: relative;
    }
    .image-drop-zone:hover, .image-drop-zone.drag-over { border-color: #e94560; background: rgba(233, 69, 96, 0.1); }
    .image-drop-zone .hint-text { font-size: 13px; color: #888; text-align: center; }
    .image-drop-zone .hint-text strong { color: #8ab4f8; }
    .image-drop-zone input[type="file"] { display: none; }
    .image-preview { max-width: 100%; max-height: 120px; border-radius: 6px; object-fit: contain; display: none; }
    .image-preview.show { display: block; }
    .image-send-btn {
      padding: 6px 16px; border: none; border-radius: 16px;
      background: #e94560; color: #fff; font-weight: 600; font-size: 12px;
      cursor: pointer; transition: background 0.2s; display: none;
    }
    .image-send-btn:hover { background: #d63851; }
    .image-send-btn.show { display: inline-block; }

    .file-drop-zone {
      flex: 1; border: 2px dashed #0f3460; border-radius: 8px;
      display: flex; flex-direction: column; justify-content: center; align-items: center;
      gap: 8px; cursor: pointer; transition: all 0.2s; padding: 12px;
      min-height: 80px; position: relative;
    }
    .file-drop-zone:hover, .file-drop-zone.drag-over { border-color: #e94560; background: rgba(233, 69, 96, 0.1); }
    .file-drop-zone .hint-text { font-size: 13px; color: #888; text-align: center; }
    .file-drop-zone .hint-text strong { color: #8ab4f8; }
    .file-drop-zone input[type="file"] { display: none; }
    .file-info { font-size: 12px; color: #aaa; display: none; text-align: center; }
    .file-info.show { display: block; }
    .file-send-btn {
      padding: 6px 16px; border: none; border-radius: 16px;
      background: #e94560; color: #fff; font-weight: 600; font-size: 12px;
      cursor: pointer; transition: background 0.2s; display: none;
    }
    .file-send-btn:hover { background: #d63851; }
    .file-send-btn.show { display: inline-block; }

    .messages { flex: 1; overflow-y: auto; padding: 16px 20px; display: flex; flex-direction: column; gap: 8px; }
    .messages::-webkit-scrollbar { width: 6px; }
    .messages::-webkit-scrollbar-thumb { background: #0f3460; border-radius: 3px; }
    .message { padding: 8px 14px; border-radius: 18px; max-width: 80%; word-break: break-word; line-height: 1.4; }
    .message .nickname { font-size: 11px; opacity: 0.7; margin-bottom: 2px; }
    .message.self { background: #0f3460; align-self: flex-end; border-bottom-right-radius: 4px; }
    .message.other { background: #1a1a40; align-self: flex-start; border-bottom-left-radius: 4px; }
    .message.system { background: transparent; align-self: center; font-size: 12px; color: #888; }
    .message .file-attachment, .message .image-attachment {
      display: flex; align-items: center; gap: 8px; cursor: pointer;
      padding: 6px 10px; border-radius: 8px; transition: background 0.2s;
    }
    .message .file-attachment:hover, .message .image-attachment:hover { background: rgba(255,255,255,0.05); }
    .message .file-icon { font-size: 24px; }
    .message .file-info-text { font-size: 12px; color: #aaa; }
    .message .file-name { font-weight: 500; color: #8ab4f8; }
    .message .image-thumb { width: 40px; height: 40px; border-radius: 6px; object-fit: cover; background: #1a1a40; }
    .input-area { display: flex; padding: 12px 16px; gap: 8px; border-top: 1px solid #0f3460; align-items: flex-end; }
    .input-area textarea { flex: 1; padding: 10px 16px; border: none; border-radius: 18px; background: #1a1a40; color: #eee; font-size: 14px; outline: none; resize: none; min-height: 40px; max-height: 120px; line-height: 1.4; font-family: inherit; }
    .input-area textarea::placeholder { color: #666; }
    .input-area textarea::-webkit-scrollbar { width: 4px; }
    .input-area textarea::-webkit-scrollbar-thumb { background: #0f3460; border-radius: 2px; }
    .input-area button { padding: 10px 20px; border: none; border-radius: 24px; background: #e94560; color: #fff; font-weight: 600; cursor: pointer; transition: background 0.2s; flex-shrink: 0; }
    .input-area button:hover { background: #d63851; }
    .message { padding: 8px 14px; border-radius: 18px; max-width: 80%; word-break: break-word; line-height: 1.4; white-space: pre-wrap; }
    .status { text-align: center; padding: 8px; font-size: 12px; color: #888; }

    .nickname-overlay {
      position: absolute; inset: 0; background: rgba(22, 33, 62, 0.95);
      display: flex; flex-direction: column; justify-content: center; align-items: center;
      gap: 16px; z-index: 10;
    }
    @media (min-width: 768px) {
      .nickname-overlay { border-radius: 12px; }
    }
    .nickname-overlay h2 { font-size: 22px; color: #eee; }
    .nickname-overlay input {
      padding: 12px 20px; border: 2px solid #0f3460; border-radius: 24px;
      background: #1a1a40; color: #eee; font-size: 16px; text-align: center;
      outline: none; width: 240px; transition: border-color 0.2s;
    }
    .nickname-overlay input:focus { border-color: #e94560; }
    .nickname-overlay button {
      padding: 10px 32px; border: none; border-radius: 24px;
      background: #e94560; color: #fff; font-weight: 600; font-size: 15px;
      cursor: pointer; transition: background 0.2s;
    }
    .nickname-overlay button:hover { background: #d63851; }
    .nickname-overlay .error { color: #e94560; font-size: 13px; display: none; }

    .image-viewer {
      position: fixed; inset: 0; background: rgba(0,0,0,0.92);
      display: none; flex-direction: column; justify-content: center; align-items: center;
      z-index: 100; cursor: pointer;
    }
    .image-viewer.show { display: flex; }
    .image-viewer .image-wrapper {
      display: flex; flex-direction: column; align-items: center;
      max-width: 95vw; max-height: 95vh;
    }
    .image-viewer img {
      max-width: 100%; max-height: 90vh; object-fit: contain;
      border-radius: 8px; box-shadow: 0 8px 40px rgba(0,0,0,0.5);
    }
    .image-viewer .hint-text {
      color: rgba(255,255,255,0.5); font-size: 13px;
      margin-top: 12px; text-align: center;
      user-select: none;
    }
    .image-viewer .close-btn {
      position: absolute; top: 20px; right: 20px;
      width: 40px; height: 40px; border-radius: 50%;
      background: rgba(255,255,255,0.15); border: none;
      color: #fff; font-size: 24px; cursor: pointer;
      display: flex; justify-content: center; align-items: center;
      transition: background 0.2s;
    }
    .image-viewer .close-btn:hover { background: rgba(255,255,255,0.3); }

    .upload-progress {
      position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%);
      background: #16213e; border-radius: 12px; padding: 24px 32px;
      box-shadow: 0 8px 32px rgba(0,0,0,0.5); z-index: 50;
      display: none; text-align: center; min-width: 200px;
    }
    .upload-progress.show { display: block; }
    .upload-progress .spinner {
      width: 32px; height: 32px; border: 3px solid #0f3460;
      border-top-color: #e94560; border-radius: 50%;
      animation: spin 0.8s linear infinite; margin: 0 auto 12px;
    }
    @keyframes spin { to { transform: rotate(360deg); } }
    .upload-progress .progress-text { font-size: 14px; color: #aaa; }
  </style>
</head>
<body>
  <div class="chat-container" id="chatContainer">
    <div style="position: relative;">
      <div class="chat-header" id="chatHeader">
        💬 Chat Room
        <span class="dropdown-arrow" id="dropdownArrow">▼</span>
        <div class="people-count" id="peopleCount">👤 0 online</div>
      </div>

      <div class="dropdown-panel" id="dropdownPanel">
      <div class="dropdown-panel-inner">
        <div class="dropdown-section">
          <h3><span class="icon">🖼️</span> Send Image</h3>
          <div class="image-drop-zone" id="imageDropZone">
            <div class="hint-text" id="imageHint">
              <strong>Click</strong> to select<br/>
              or <strong>Ctrl+V</strong> to paste<br/>
              or <strong>drag & drop</strong> image here
            </div>
            <img class="image-preview" id="imagePreview" />
            <input type="file" id="imageFileInput" accept="image/png,image/jpeg,image/gif,image/webp,image/bmp" />
          </div>
          <button class="image-send-btn" id="imageSendBtn">Send Image</button>
        </div>

        <div class="dropdown-section">
          <h3><span class="icon">📁</span> Send File</h3>
          <div class="file-drop-zone" id="fileDropZone">
            <div class="hint-text" id="fileHint">
              <strong>Click</strong> to select file<br/>
              or <strong>drag & drop</strong> file here<br/>
              <span style="font-size:11px;color:#666;">Max: <span id="maxFileSizeLabel">50 MB</span></span>
            </div>
            <div class="file-info" id="fileInfo">
              📄 <span id="fileNameDisplay"></span><br/>
              <span id="fileSizeDisplay"></span>
            </div>
            <input type="file" id="fileFileInput" />
          </div>
          <button class="file-send-btn" id="fileSendBtn">Send File</button>
        </div>
      </div>
    </div>
    </div>

    <div class="nickname-overlay" id="nicknameOverlay">
      <h2>👋 Welcome!</h2>
      <p style="color:#888;font-size:14px;">Enter your nickname to join</p>
      <input type="text" id="nicknameInput" placeholder="Your nickname..." maxlength="20" />
      <div class="error" id="nicknameError">Nickname cannot be empty</div>
      <button id="joinBtn">Join Chat</button>
    </div>

    <div class="messages" id="messages"></div>
    <div class="status" id="status">Connecting...</div>
    <div class="input-area">
      <textarea id="input" placeholder="输入消息..." disabled rows="1"></textarea>
      <button id="sendBtn" disabled>Send</button>
    </div>
  </div>

  <div class="image-viewer" id="imageViewer">
    <button class="close-btn" id="imageViewerClose">✕</button>
    <div class="image-wrapper">
      <div class="hint-text">👇 单击关闭详图</div>
      <img id="imageViewerImg" src="" alt="Full size image" />
      <div class="hint-text">👆 单击关闭详图</div>
    </div>
  </div>

  <div class="upload-progress" id="uploadProgress">
    <div class="spinner"></div>
    <div class="progress-text" id="uploadProgressText">Uploading...</div>
  </div>

  <script>
    const wsUrl = window.location.origin.replace(/^http/, 'ws') + '?room=global';
    const ws = new WebSocket(wsUrl);
    const messagesEl = document.getElementById('messages');
    const inputEl = document.getElementById('input');
    const sendBtn = document.getElementById('sendBtn');
    const statusEl = document.getElementById('status');
    const peopleCountEl = document.getElementById('peopleCount');
    const nicknameOverlay = document.getElementById('nicknameOverlay');
    const nicknameInput = document.getElementById('nicknameInput');
    const nicknameError = document.getElementById('nicknameError');
    const joinBtn = document.getElementById('joinBtn');
    const chatHeader = document.getElementById('chatHeader');
    const dropdownPanel = document.getElementById('dropdownPanel');
    const dropdownArrow = document.getElementById('dropdownArrow');
    const chatContainer = document.getElementById('chatContainer');

    const imageDropZone = document.getElementById('imageDropZone');
    const imageFileInput = document.getElementById('imageFileInput');
    const imagePreview = document.getElementById('imagePreview');
    const imageSendBtn = document.getElementById('imageSendBtn');
    const imageHint = document.getElementById('imageHint');

    const fileDropZone = document.getElementById('fileDropZone');
    const fileFileInput = document.getElementById('fileFileInput');
    const fileInfo = document.getElementById('fileInfo');
    const fileNameDisplay = document.getElementById('fileNameDisplay');
    const fileSizeDisplay = document.getElementById('fileSizeDisplay');
    const fileSendBtn = document.getElementById('fileSendBtn');
    const fileHint = document.getElementById('fileHint');
    const maxFileSizeLabel = document.getElementById('maxFileSizeLabel');

    const imageViewer = document.getElementById('imageViewer');
    const imageViewerImg = document.getElementById('imageViewerImg');
    const imageViewerClose = document.getElementById('imageViewerClose');

    const uploadProgress = document.getElementById('uploadProgress');
    const uploadProgressText = document.getElementById('uploadProgressText');

    let pendingImageData = null;
    let pendingFileData = null;
    let dropdownOpen = false;
    let joined = false;
    let pageVisible = true;
    let unreadCount = 0;
    let titleInterval = null;
    const originalTitle = document.title;

    // Track page visibility
    document.addEventListener('visibilitychange', () => {
      pageVisible = !document.hidden;
      if (pageVisible) {
        clearUnread();
      }
    });
    window.addEventListener('focus', () => {
      pageVisible = true;
      clearUnread();
    });
    window.addEventListener('blur', () => {
      pageVisible = false;
    });

    function clearUnread() {
      unreadCount = 0;
      if (titleInterval) {
        clearInterval(titleInterval);
        titleInterval = null;
      }
      document.title = originalTitle;
    }

    function notifyNewMessage(nickname, text) {
      if (pageVisible) return;
      unreadCount++;
      // Flash title
      if (!titleInterval) {
        let showUnread = true;
        titleInterval = setInterval(() => {
          document.title = showUnread ? '[' + unreadCount + '] New Message - ' + originalTitle : originalTitle;
          showUnread = !showUnread;
        }, 1000);
      } else {
        // Just update the count
        document.title = '[' + unreadCount + '] New Message - ' + originalTitle;
      }
      // Browser notification
      if ('Notification' in window && Notification.permission === 'granted') {
        new Notification('Chat Message', {
          body: nickname ? nickname + ': ' + text : text,
          icon: 'data:image/svg+xml,<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100"><text y=".9em" font-size="90">💬</text></svg>',
        });
      }
    }

    // Request notification permission
    if ('Notification' in window && Notification.permission === 'default') {
      Notification.requestPermission();
    }

    const isMobile = 'ontouchstart' in window || navigator.maxTouchPoints > 0;

    // Fix mobile keyboard overlapping input area
    function fixMobileLayout() {
      const container = document.querySelector('.chat-container');
      if (!container) return;
      if (window.visualViewport) {
        // Use visualViewport for accurate visible area (accounts for keyboard)
        const vv = window.visualViewport;
        container.style.height = vv.height + 'px';
        container.style.width = container.clientWidth + 'px';
        container.style.position = 'fixed';
        container.style.top = vv.offsetTop + 'px';
        container.style.left = '50%';
        container.style.transform = 'translateX(-50%)';
        // Scroll the input into view
        const inputArea = document.querySelector('.input-area');
        if (inputArea) {
          setTimeout(() => inputArea.scrollIntoView({ block: 'nearest' }), 100);
        }
      } else {
        // Fallback for browsers without visualViewport
        const isKeyboardOpen = window.innerHeight < window.outerHeight * 0.8;
        if (isKeyboardOpen) {
          container.style.height = window.innerHeight + 'px';
          container.style.position = 'fixed';
          container.style.top = '0';
          container.style.left = '50%';
          container.style.transform = 'translateX(-50%)';
        } else {
          container.style.height = window.innerHeight + 'px';
          container.style.position = '';
          container.style.top = '';
          container.style.left = '';
          container.style.transform = '';
        }
      }
    }

    if (isMobile) {
      // Fix on load
      setTimeout(fixMobileLayout, 100);
      // Listen to visualViewport changes (keyboard open/close)
      if (window.visualViewport) {
        window.visualViewport.addEventListener('resize', fixMobileLayout);
      } else {
        window.addEventListener('resize', fixMobileLayout);
      }
      // Also fix on focus/blur of input
      const inputArea = document.getElementById('input');
      inputArea.addEventListener('focus', () => setTimeout(fixMobileLayout, 300));
      inputArea.addEventListener('blur', () => setTimeout(fixMobileLayout, 100));
    }

    inputEl.placeholder = isMobile ? '输入消息... (点击 Send 发送)' : '输入消息... (Enter 发送，Shift+Enter 换行)';

    let currentMaxFileSize = 50 * 1024 * 1024;
    fetch('/api/settings')
      .then(r => r.json())
      .then(data => {
        currentMaxFileSize = data.maxFileSize;
        const sizeMB = (data.maxFileSize / (1024 * 1024)).toFixed(1);
        maxFileSizeLabel.textContent = sizeMB + ' MB';
      })
      .catch(() => {});

    function setDropdownHeight() {
      const chatHeight = chatContainer.clientHeight;
      const headerHeight = chatHeader.offsetHeight;
      const targetHeight = Math.floor(chatHeight * 2 / 3);
      dropdownPanel.style.setProperty('--dropdown-target-height', targetHeight + 'px');
    }
    chatHeader.addEventListener('click', (e) => {
      dropdownOpen = !dropdownOpen;
      if (dropdownOpen) {
        setDropdownHeight();
        dropdownPanel.classList.add('open');
        dropdownArrow.classList.add('open');
      } else {
        dropdownPanel.classList.remove('open');
        dropdownArrow.classList.remove('open');
      }
    });

    ws.onopen = () => {
      statusEl.textContent = 'Connected';
    };

    ws.onclose = () => {
      statusEl.textContent = 'Disconnected. Reconnecting...';
      inputEl.disabled = true;
      sendBtn.disabled = true;
      setTimeout(() => location.reload(), 3000);
    };

    ws.onerror = () => {
      statusEl.textContent = 'Connection error';
    };

    ws.onmessage = (event) => {
      let data;
      try {
        data = JSON.parse(event.data);
      } catch {
        return;
      }

      if (data.type === 'ping') {
        // Respond to heartbeat ping
        ws.send(JSON.stringify({ type: 'pong' }));
        return;
      }

      if (data.type === 'people_count') {
        peopleCountEl.textContent = '👤 ' + data.count + ' online';
      } else if (data.type === 'system') {
        const msg = document.createElement('div');
        msg.className = 'message system';
        msg.textContent = data.text;
        messagesEl.appendChild(msg);
        messagesEl.scrollTop = messagesEl.scrollHeight;
        if (!data.self) notifyNewMessage(null, data.text);
      } else if (data.type === 'chat') {
        const msg = document.createElement('div');
        if (data.self) {
          msg.className = 'message self';
          msg.textContent = data.text;
        } else {
          msg.className = 'message other';
          msg.innerHTML = '<div class="nickname">' + escapeHtml(data.nickname) + '</div>' + escapeHtml(data.text);
          notifyNewMessage(data.nickname, data.text);
        }
        messagesEl.appendChild(msg);
        messagesEl.scrollTop = messagesEl.scrollHeight;
      } else if (data.type === 'image') {
        const msg = document.createElement('div');
        if (data.self) {
          msg.className = 'message self';
        } else {
          msg.className = 'message other';
          notifyNewMessage(data.nickname, '[Image] ' + (data.fileName || ''));
        }
        const fileSizeStr = formatFileSize(data.fileSize);
        msg.innerHTML = (data.self ? '' : '<div class="nickname">' + escapeHtml(data.nickname) + '</div>') +
          '<div class="image-attachment" onclick="openImageViewer(\\'' + data.imageId.replace(/'/g, '') + '\\')">' +
          '<img class="image-thumb" src="/image/' + data.imageId + '" alt="image" />' +
          '<div class="file-info-text">🖼️ ' + escapeHtml(data.fileName || 'Image') + '<br/>' + fileSizeStr + '</div>' +
          '</div>';
        messagesEl.appendChild(msg);
        messagesEl.scrollTop = messagesEl.scrollHeight;
      } else if (data.type === 'file') {
        const msg = document.createElement('div');
        if (data.self) {
          msg.className = 'message self';
        } else {
          msg.className = 'message other';
          notifyNewMessage(data.nickname, '[File] ' + (data.fileName || ''));
        }
        const fileSizeStr = formatFileSize(data.fileSize);
        msg.innerHTML = (data.self ? '' : '<div class="nickname">' + escapeHtml(data.nickname) + '</div>') +
          '<div class="file-attachment" onclick="downloadFile(\\'' + data.fileId + '\\', \\'' + escapeHtml(data.fileName) + '\\')">' +
          '<span class="file-icon">📄</span>' +
          '<div class="file-info-text"><span class="file-name">' + escapeHtml(data.fileName) + '</span><br/>' + fileSizeStr + '</div>' +
          '</div>';
        messagesEl.appendChild(msg);
        messagesEl.scrollTop = messagesEl.scrollHeight;
      }
    };

    function escapeHtml(text) {
      const div = document.createElement('div');
      div.textContent = text;
      return div.innerHTML;
    }

    function formatFileSize(bytes) {
      if (bytes >= 1024 * 1024 * 1024) return (bytes / (1024 * 1024 * 1024)).toFixed(1) + ' GB';
      if (bytes >= 1024 * 1024) return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
      if (bytes >= 1024) return (bytes / 1024).toFixed(1) + ' KB';
      return bytes + ' B';
    }

    const savedNickname = localStorage.getItem('chat_nickname');

    function joinChat(nickname) {
      nicknameOverlay.style.display = 'none';
      joined = true;
      localStorage.setItem('chat_nickname', nickname);
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: 'join', nickname }));
      } else {
        // Wait for WebSocket to open before sending
        const checkOpen = setInterval(() => {
          if (ws.readyState === WebSocket.OPEN) {
            clearInterval(checkOpen);
            ws.send(JSON.stringify({ type: 'join', nickname }));
          }
        }, 50);
      }
      inputEl.disabled = false;
      sendBtn.disabled = false;
      inputEl.focus();
    }

    function handleJoinClick() {
      const nickname = nicknameInput.value.trim();
      if (!nickname) {
        nicknameError.style.display = 'block';
        return;
      }
      nicknameError.style.display = 'none';
      joinChat(nickname);
    }

    joinBtn.addEventListener('click', handleJoinClick);
    nicknameInput.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') handleJoinClick();
    });

    if (savedNickname) {
      // Wait for WebSocket to be open, then join
      const checkOpen = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
          clearInterval(checkOpen);
          joinChat(savedNickname);
        }
      }, 50);
    }

    function sendMsg() {
      const text = inputEl.value.trim();
      if (!text) return;
      ws.send(JSON.stringify({ type: 'message', text }));
      inputEl.value = '';
      inputEl.style.height = 'auto';
    }

    inputEl.addEventListener('input', () => {
      inputEl.style.height = 'auto';
      inputEl.style.height = Math.min(inputEl.scrollHeight, 120) + 'px';
    });

    sendBtn.addEventListener('click', sendMsg);
    inputEl.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        if (isMobile) return;
        if (!e.shiftKey) {
          e.preventDefault();
          sendMsg();
        }
      }
    });

    function compressImage(file, maxDimension) {
      return new Promise((resolve, reject) => {
        const img = new Image();
        const url = URL.createObjectURL(file);
        img.onload = function() {
          URL.revokeObjectURL(url);
          let w = img.width;
          let h = img.height;
          if (w > maxDimension || h > maxDimension) {
            const ratio = Math.min(maxDimension / w, maxDimension / h);
            w = Math.round(w * ratio);
            h = Math.round(h * ratio);
          }
          const canvas = document.createElement('canvas');
          canvas.width = w;
          canvas.height = h;
          const ctx = canvas.getContext('2d');
          ctx.drawImage(img, 0, 0, w, h);
          canvas.toBlob(function(blob) {
            resolve({ blob, width: w, height: h });
          }, file.type || 'image/png', 0.85);
        };
        img.onerror = reject;
        img.src = url;
      });
    }

    function handleImageFile(file) {
      if (!file) return;
      if (!file.type.startsWith('image/')) {
        alert('Please select an image file');
        return;
      }
      const reader = new FileReader();
      reader.onload = function(e) {
        imagePreview.src = e.target.result;
        imagePreview.classList.add('show');
        imageHint.style.display = 'none';
        imageSendBtn.classList.add('show');
      };
      reader.readAsDataURL(file);
      pendingImageData = { file };
    }

    imageDropZone.addEventListener('click', () => {
      imageFileInput.click();
    });

    imageFileInput.addEventListener('change', (e) => {
      if (e.target.files && e.target.files[0]) {
        handleImageFile(e.target.files[0]);
      }
    });

    imageDropZone.addEventListener('dragover', (e) => {
      e.preventDefault();
      e.stopPropagation();
      imageDropZone.classList.add('drag-over');
    });

    imageDropZone.addEventListener('dragleave', (e) => {
      e.preventDefault();
      e.stopPropagation();
      imageDropZone.classList.remove('drag-over');
    });

    imageDropZone.addEventListener('drop', (e) => {
      e.preventDefault();
      e.stopPropagation();
      imageDropZone.classList.remove('drag-over');
      const files = e.dataTransfer.files;
      if (files && files.length > 0) {
        handleImageFile(files[0]);
      }
    });

    document.addEventListener('paste', (e) => {
      const items = e.clipboardData && e.clipboardData.items;
      if (!items) return;
      for (const item of items) {
        if (item.type.startsWith('image/')) {
          const file = item.getAsFile();
          if (file) {
            handleImageFile(file);
            if (!dropdownOpen) {
              chatHeader.click();
            }
          }
          break;
        }
      }
    });

    imageSendBtn.addEventListener('click', async () => {
      if (!pendingImageData || !pendingImageData.file) return;

      uploadProgress.classList.add('show');
      uploadProgressText.textContent = 'Compressing image...';

      try {
        const { blob, width, height } = await compressImage(pendingImageData.file, 1024);

        uploadProgressText.textContent = 'Uploading image...';

        const formData = new FormData();
        formData.append('file', blob, pendingImageData.file.name);
        formData.append('type', 'image');

        const resp = await fetch('/upload', {
          method: 'POST',
          body: formData,
        });

        if (!resp.ok) {
          const err = await resp.json();
          alert(err.error || 'Upload failed');
          uploadProgress.classList.remove('show');
          return;
        }

        const result = await resp.json();

        ws.send(JSON.stringify({
          type: 'image',
          imageId: result.fileId,
          mimeType: result.mimeType,
          fileName: result.fileName,
          fileSize: result.fileSize,
          width: width,
          height: height,
        }));

        pendingImageData = null;
        imagePreview.classList.remove('show');
        imagePreview.src = '';
        imageHint.style.display = 'block';
        imageSendBtn.classList.remove('show');

        if (dropdownOpen) {
          chatHeader.click();
        }
      } catch (err) {
        alert('Failed to send image: ' + err.message);
      } finally {
        uploadProgress.classList.remove('show');
      }
    });

    function handleFileSelect(file) {
      if (!file) return;
      if (file.size > currentMaxFileSize) {
        const sizeMB = (currentMaxFileSize / (1024 * 1024)).toFixed(1);
        alert('File too large. Maximum allowed size is ' + sizeMB + ' MB');
        return;
      }
      fileNameDisplay.textContent = file.name;
      fileSizeDisplay.textContent = formatFileSize(file.size);
      fileInfo.classList.add('show');
      fileHint.style.display = 'none';
      fileSendBtn.classList.add('show');
      pendingFileData = { file };
    }

    fileDropZone.addEventListener('click', () => {
      fileFileInput.click();
    });

    fileFileInput.addEventListener('change', (e) => {
      if (e.target.files && e.target.files[0]) {
        handleFileSelect(e.target.files[0]);
      }
    });

    fileDropZone.addEventListener('dragover', (e) => {
      e.preventDefault();
      e.stopPropagation();
      fileDropZone.classList.add('drag-over');
    });

    fileDropZone.addEventListener('dragleave', (e) => {
      e.preventDefault();
      e.stopPropagation();
      fileDropZone.classList.remove('drag-over');
    });

    fileDropZone.addEventListener('drop', (e) => {
      e.preventDefault();
      e.stopPropagation();
      fileDropZone.classList.remove('drag-over');
      const files = e.dataTransfer.files;
      if (files && files.length > 0) {
        handleFileSelect(files[0]);
      }
    });

    fileSendBtn.addEventListener('click', async () => {
      if (!pendingFileData || !pendingFileData.file) return;

      uploadProgress.classList.add('show');
      uploadProgressText.textContent = 'Uploading file...';

      try {
        const formData = new FormData();
        formData.append('file', pendingFileData.file);
        formData.append('type', 'file');

        const resp = await fetch('/upload', {
          method: 'POST',
          body: formData,
        });

        if (!resp.ok) {
          const err = await resp.json();
          alert(err.error || 'Upload failed');
          uploadProgress.classList.remove('show');
          return;
        }

        const result = await resp.json();

        ws.send(JSON.stringify({
          type: 'file',
          fileId: result.fileId,
          fileName: result.fileName,
          fileSize: result.fileSize,
          mimeType: result.mimeType,
        }));

        pendingFileData = null;
        fileInfo.classList.remove('show');
        fileHint.style.display = 'block';
        fileSendBtn.classList.remove('show');

        if (dropdownOpen) {
          chatHeader.click();
        }
      } catch (err) {
        alert('Failed to send file: ' + err.message);
      } finally {
        uploadProgress.classList.remove('show');
      }
    });

    // Prevent swipe-back gesture on image viewer (only horizontal swipes)
    let touchStartX = 0;
    let touchStartY = 0;

    function handleTouchStart(e) {
      touchStartX = e.touches[0].clientX;
      touchStartY = e.touches[0].clientY;
    }

    function handleTouchMove(e) {
      if (!touchStartX) return;
      const dx = e.touches[0].clientX - touchStartX;
      const dy = e.touches[0].clientY - touchStartY;
      // Only prevent horizontal swipes (more horizontal than vertical)
      if (Math.abs(dx) > Math.abs(dy) && Math.abs(dx) > 30) {
        e.preventDefault();
      }
    }

    function openImageViewer(imageId) {
      imageViewerImg.src = '/image/' + imageId;
      imageViewer.classList.add('show');
      // Block horizontal swipe gestures to prevent browser swipe-back
      imageViewer.addEventListener('touchstart', handleTouchStart, { passive: true });
      imageViewer.addEventListener('touchmove', handleTouchMove, { passive: false });
    }

    function closeImageViewer() {
      imageViewer.classList.remove('show');
      imageViewerImg.src = '';
      // Re-enable touch gestures
      imageViewer.removeEventListener('touchstart', handleTouchStart);
      imageViewer.removeEventListener('touchmove', handleTouchMove);
    }

    imageViewer.addEventListener('click', (e) => {
      if (e.target === imageViewer || e.target === imageViewerImg || e.target.closest('.image-wrapper')) {
        closeImageViewer();
      }
    });

    imageViewerClose.addEventListener('click', () => {
      closeImageViewer();
    });

    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape') {
        closeImageViewer();
      }
    });

    function downloadFile(fileId, fileName) {
      const a = document.createElement('a');
      a.href = '/download/' + fileId;
      a.download = fileName;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
    }
  </script>
</body>
</html>`;
}

function getAdminHTML(env: Env): string {
  const adminPass = env.ADMIN_PASS || "admin_password";
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Admin - Chat Settings</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a2e; color: #eee; min-height: 100vh; display: flex; justify-content: center; align-items: center; }
    .admin-container { width: 420px; max-width: 90vw; background: #16213e; border-radius: 12px; padding: 32px; box-shadow: 0 8px 32px rgba(0,0,0,0.3); }
    h1 { text-align: center; margin-bottom: 24px; font-size: 22px; color: #e94560; }
    .form-group { margin-bottom: 20px; }
    label { display: block; margin-bottom: 8px; font-size: 14px; color: #8ab4f8; }
    input[type="password"], input[type="number"] { width: 100%; padding: 12px 16px; border: 2px solid #0f3460; border-radius: 8px; background: #1a1a40; color: #eee; font-size: 15px; outline: none; transition: border-color 0.2s; }
    input:focus { border-color: #e94560; }
    .btn { width: 100%; padding: 12px; border: none; border-radius: 8px; background: #e94560; color: #fff; font-weight: 600; font-size: 15px; cursor: pointer; transition: background 0.2s; }
    .btn:hover { background: #d63851; }
    .error { color: #e94560; font-size: 13px; margin-top: 8px; display: none; }
    .success { color: #4caf50; font-size: 13px; margin-top: 8px; display: none; }
    .current-setting { background: #1a1a40; border-radius: 8px; padding: 12px 16px; margin-bottom: 20px; font-size: 14px; }
    .current-setting span { color: #8ab4f8; }
  </style>
</head>
<body>
  <div class="admin-container">
    <h1>⚙️ Admin Settings</h1>
    <div id="loginForm">
      <div class="form-group">
        <label>Admin Password</label>
        <input type="password" id="adminPass" placeholder="Enter admin password..." />
      </div>
      <button class="btn" onclick="login()">Login</button>
      <div class="error" id="loginError">Incorrect password</div>
    </div>
    <div id="settingsForm" style="display:none;">
      <div class="current-setting">
        Current max file size: <span id="currentMaxSize">50 MB</span><br/>
        File retention: <span id="currentTTL">10 minutes</span>
      </div>
      <div class="form-group">
        <label>Max File Size (MB)</label>
        <input type="number" id="maxFileSizeInput" min="1" max="500" value="50" />
      </div>
      <div class="form-group">
        <label>File Retention Time (seconds, 60-86400)</label>
        <input type="number" id="fileTTLInput" min="60" max="86400" value="600" />
        <div style="font-size:11px;color:#666;margin-top:4px;">600 = 10 min | 3600 = 1 hr | 86400 = 24 hrs</div>
      </div>
      <button class="btn" onclick="saveSettings()">Save Settings</button>
      <div class="success" id="saveSuccess">Settings saved successfully!</div>
      <div class="error" id="saveError">Failed to save settings</div>
    </div>
  </div>
  <script>
    const ADMIN_PASS = "${adminPass}";

    function login() {
      const pass = document.getElementById('adminPass').value;
      if (pass === ADMIN_PASS) {
        document.getElementById('loginForm').style.display = 'none';
        document.getElementById('settingsForm').style.display = 'block';
        document.getElementById('loginError').style.display = 'none';
        loadCurrentSettings();
      } else {
        document.getElementById('loginError').style.display = 'block';
      }
    }

    function formatTTL(seconds) {
      if (seconds >= 3600) return (seconds / 3600).toFixed(1) + ' hours';
      if (seconds >= 60) return (seconds / 60).toFixed(0) + ' minutes';
      return seconds + ' seconds';
    }

    function loadCurrentSettings() {
      fetch('/api/settings')
        .then(r => r.json())
        .then(data => {
          const sizeMB = (data.maxFileSize / (1024 * 1024)).toFixed(1);
          document.getElementById('currentMaxSize').textContent = sizeMB + ' MB';
          document.getElementById('maxFileSizeInput').value = parseFloat(sizeMB);
          document.getElementById('currentTTL').textContent = formatTTL(data.fileTTL);
          document.getElementById('fileTTLInput').value = data.fileTTL;
        })
        .catch(() => {});
    }

    function saveSettings() {
      const sizeMB = parseFloat(document.getElementById('maxFileSizeInput').value);
      const ttl = parseInt(document.getElementById('fileTTLInput').value, 10);
      if (!sizeMB || sizeMB < 1) {
        document.getElementById('saveError').textContent = 'Please enter a valid size (min 1 MB)';
        document.getElementById('saveError').style.display = 'block';
        return;
      }
      if (!ttl || ttl < 60 || ttl > 86400) {
        document.getElementById('saveError').textContent = 'Please enter a valid TTL (60-86400 seconds)';
        document.getElementById('saveError').style.display = 'block';
        return;
      }
      fetch('/api/settings', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: ADMIN_PASS, maxFileSize: sizeMB * 1024 * 1024, fileTTL: ttl })
      })
      .then(r => r.json())
      .then(data => {
        if (data.success) {
          document.getElementById('saveSuccess').style.display = 'block';
          document.getElementById('saveError').style.display = 'none';
          document.getElementById('currentMaxSize').textContent = sizeMB + ' MB';
          document.getElementById('currentTTL').textContent = formatTTL(data.fileTTL);
          setTimeout(() => document.getElementById('saveSuccess').style.display = 'none', 3000);
        } else {
          document.getElementById('saveError').textContent = data.error || 'Failed to save';
          document.getElementById('saveError').style.display = 'block';
        }
      })
      .catch(() => {
        document.getElementById('saveError').textContent = 'Network error';
        document.getElementById('saveError').style.display = 'block';
      });
    }

    document.getElementById('adminPass').addEventListener('keydown', (e) => {
      if (e.key === 'Enter') login();
    });
  </script>
</body>
</html>`;
}

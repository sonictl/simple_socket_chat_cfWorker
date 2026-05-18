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

  handleSocket(ws: WebSocket) {
    ws.accept();

    ws.addEventListener("message", (msg: MessageEvent) => {
      let data: any;
      try {
        data = JSON.parse(msg.data);
      } catch {
        return;
      }

      if (data.type === "join") {
        const nickname = data.nickname || "Anonymous";
        this.clients.set(ws, nickname);

        // 通知所有人人数更新
        this.broadcastPeopleCount();

        // 广播系统消息：xxx 加入了房间
        this.broadcast({
          type: "system",
          text: `${nickname} joined the room`,
        });
      } else if (data.type === "message") {
        const nickname = this.clients.get(ws) || "Anonymous";
        // 广播聊天消息给其他人
        this.broadcast(
          {
            type: "chat",
            nickname,
            text: data.text,
          },
          ws
        );
        // 也发回给自己（确认送达）
        ws.send(
          JSON.stringify({
            type: "chat",
            nickname,
            text: data.text,
            self: true,
          })
        );
      }
    });

    ws.addEventListener("close", () => {
      const nickname = this.clients.get(ws) || "Anonymous";
      this.clients.delete(ws);

      // 广播系统消息：xxx 离开了房间
      this.broadcast({
        type: "system",
        text: `${nickname} left the room`,
      });

      // 通知人数更新
      this.broadcastPeopleCount();
    });
  }
}

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
    const html = `<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Chat App</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #1a1a2e; color: #eee; height: 100dvh; display: flex; justify-content: center; align-items: flex-start; }
    .chat-container { width: 100%; max-width: 480px; height: 100dvh; background: #16213e; display: flex; flex-direction: column; overflow: hidden; position: relative; }
    @media (min-width: 768px) {
      body { align-items: center; padding: 20px 0; }
      .chat-container { width: 400px; height: 600px; border-radius: 12px; box-shadow: 0 8px 32px rgba(0,0,0,0.3); }
    }
    .chat-header { padding: 12px 20px; background: #0f3460; text-align: center; font-weight: 600; font-size: 18px; }
    .chat-header .people-count { font-size: 13px; font-weight: 400; color: #8ab4f8; margin-top: 2px; }
    .messages { flex: 1; overflow-y: auto; padding: 16px 20px; display: flex; flex-direction: column; gap: 8px; }
    .messages::-webkit-scrollbar { width: 6px; }
    .messages::-webkit-scrollbar-thumb { background: #0f3460; border-radius: 3px; }
    .message { padding: 8px 14px; border-radius: 18px; max-width: 80%; word-break: break-word; line-height: 1.4; }
    .message .nickname { font-size: 11px; opacity: 0.7; margin-bottom: 2px; }
    .message.self { background: #0f3460; align-self: flex-end; border-bottom-right-radius: 4px; }
    .message.other { background: #1a1a40; align-self: flex-start; border-bottom-left-radius: 4px; }
    .message.system { background: transparent; align-self: center; font-size: 12px; color: #888; }
    .input-area { display: flex; padding: 12px 16px; gap: 8px; border-top: 1px solid #0f3460; align-items: flex-end; }
    .input-area textarea { flex: 1; padding: 10px 16px; border: none; border-radius: 18px; background: #1a1a40; color: #eee; font-size: 14px; outline: none; resize: none; min-height: 40px; max-height: 120px; line-height: 1.4; font-family: inherit; }
    .input-area textarea::placeholder { color: #666; }
    .input-area textarea::-webkit-scrollbar { width: 4px; }
    .input-area textarea::-webkit-scrollbar-thumb { background: #0f3460; border-radius: 2px; }
    .input-area button { padding: 10px 20px; border: none; border-radius: 24px; background: #e94560; color: #fff; font-weight: 600; cursor: pointer; transition: background 0.2s; flex-shrink: 0; }
    .input-area button:hover { background: #d63851; }
    .message { padding: 8px 14px; border-radius: 18px; max-width: 80%; word-break: break-word; line-height: 1.4; white-space: pre-wrap; }
    .status { text-align: center; padding: 8px; font-size: 12px; color: #888; }

    /* Nickname overlay */
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
  </style>
</head>
<body>
  <div class="chat-container">
    <div class="chat-header">
      💬 Chat Room
      <div class="people-count" id="peopleCount">👤 0 online</div>
    </div>

    <!-- Nickname overlay -->
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

    // 检测是否为移动端（支持触摸的设备）
    const isMobile = 'ontouchstart' in window || navigator.maxTouchPoints > 0;

    // 移动端浏览器地址栏问题：用 JS 确保容器高度正确
    function fixHeight() {
      const container = document.querySelector('.chat-container');
      if (container) {
        container.style.height = window.innerHeight + 'px';
      }
    }
    if (isMobile) {
      fixHeight();
      window.addEventListener('resize', fixHeight);
    }

    // 根据设备设置不同的 placeholder
    inputEl.placeholder = isMobile ? '输入消息... (点击 Send 发送)' : '输入消息... (Enter 发送，Shift+Enter 换行)';

    let joined = false;

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

      if (data.type === 'people_count') {
        peopleCountEl.textContent = '👤 ' + data.count + ' online';
      } else if (data.type === 'system') {
        const msg = document.createElement('div');
        msg.className = 'message system';
        msg.textContent = data.text;
        messagesEl.appendChild(msg);
        messagesEl.scrollTop = messagesEl.scrollHeight;
      } else if (data.type === 'chat') {
        const msg = document.createElement('div');
        if (data.self) {
          msg.className = 'message self';
          msg.textContent = data.text;
        } else {
          msg.className = 'message other';
          msg.innerHTML = '<div class="nickname">' + escapeHtml(data.nickname) + '</div>' + escapeHtml(data.text);
        }
        messagesEl.appendChild(msg);
        messagesEl.scrollTop = messagesEl.scrollHeight;
      }
    };

    function escapeHtml(text) {
      const div = document.createElement('div');
      div.textContent = text;
      return div.innerHTML;
    }

    // 尝试从 localStorage 读取已保存的昵称
    const savedNickname = localStorage.getItem('chat_nickname');

    function joinChat(nickname) {
      nicknameOverlay.style.display = 'none';
      joined = true;

      // 保存昵称到 localStorage
      localStorage.setItem('chat_nickname', nickname);

      // 发送加入消息
      ws.send(JSON.stringify({ type: 'join', nickname }));

      // 启用聊天
      inputEl.disabled = false;
      sendBtn.disabled = false;
      inputEl.focus();
    }

    // 如果有已保存的昵称，自动加入
    if (savedNickname) {
      // 等 WebSocket 连接建立后再加入
      const origOnOpen = ws.onopen;
      ws.onopen = function() {
        if (typeof origOnOpen === 'function') origOnOpen.call(this);
        joinChat(savedNickname);
      };
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

    function sendMsg() {
      const text = inputEl.value.trim();
      if (!text) return;

      ws.send(JSON.stringify({ type: 'message', text }));
      inputEl.value = '';
      inputEl.style.height = 'auto';
    }

    // Auto-resize textarea
    inputEl.addEventListener('input', () => {
      inputEl.style.height = 'auto';
      inputEl.style.height = Math.min(inputEl.scrollHeight, 120) + 'px';
    });

    sendBtn.addEventListener('click', sendMsg);
    inputEl.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') {
        if (isMobile) {
          // 移动端：Enter 换行，不发送
          return;
        }
        // 桌面端：Enter 发送（Shift+Enter 换行）
        if (!e.shiftKey) {
          e.preventDefault();
          sendMsg();
        }
      }
    });
  </script>
</body>
</html>`;

    return new Response(html, {
      headers: { "Content-Type": "text/html;charset=UTF-8" },
    });
  },
};

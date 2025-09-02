import asyncio
import json
import os
import time
import uuid
from collections import deque
from typing import Dict, Optional, Any, List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from fastapi.responses import PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

SERVER_VERSION = "pigeon-rendezvous v0.1.0"
HEARTBEAT_GRACE_MS = 60_000       # prune if no heartbeat for 60s
DEFAULT_HEARTBEAT_MS = 20_000     # clients send every 20s
ROOM_HISTORY_LEN = 500            # per-room ring buffer length
MAX_MSG_SIZE = 64 * 1024          # 64KB
RATE_LIMIT_WINDOW_S = 10
RATE_LIMIT_MAX_IN_WINDOW = 50     # per client

def now_ms() -> int:
    return int(time.time() * 1000)

class Client:
    def __init__(self, client_id: str, username: str, room: str, ws: WebSocket):
        self.client_id = client_id
        self.username = username
        self.room = room
        self.ws = ws
        self.last_seen_ms = now_ms()
        self.msg_times: deque = deque()  # for simple rate limiting

    def mark_seen(self):
        self.last_seen_ms = now_ms()

    def is_stale(self) -> bool:
        return now_ms() - self.last_seen_ms > HEARTBEAT_GRACE_MS

    def record_message(self) -> bool:
        # simple sliding-window rate limiter
        t = time.time()
        self.msg_times.append(t)
        # drop old
        while self.msg_times and (t - self.msg_times[0]) > RATE_LIMIT_WINDOW_S:
            self.msg_times.popleft()
        return len(self.msg_times) <= RATE_LIMIT_MAX_IN_WINDOW

class Room:
    def __init__(self, name: str):
        self.name = name
        self.clients: Dict[str, Client] = {}
        self.history: deque = deque(maxlen=ROOM_HISTORY_LEN)
        self.seq = 0

    @property
    def leader_id(self) -> Optional[str]:
        if not self.clients:
            return None
        # Deterministic leader: smallest client_id
        return sorted(self.clients.keys())[0]

    def next_seq(self) -> int:
        self.seq += 1
        return self.seq

class Hub:
    def __init__(self):
        self.rooms: Dict[str, Room] = {}
        # background pruning
        self._prune_task = asyncio.create_task(self._prune_stale_clients())

    def room(self, name: str) -> Room:
        if name not in self.rooms:
            self.rooms[name] = Room(name)
        return self.rooms[name]

    async def _prune_stale_clients(self):
        while True:
            try:
                await asyncio.sleep(5)
                for room in list(self.rooms.values()):
                    for cid, client in list(room.clients.items()):
                        if client.is_stale():
                            await self.disconnect_client(room, cid, reason="stale")
            except Exception:
                # keep running
                pass

    async def disconnect_client(self, room: Room, client_id: str, reason: str = "disconnect"):
        client = room.clients.pop(client_id, None)
        if client:
            try:
                await client.ws.close()
            except Exception:
                pass
            await self.broadcast_presence(room, action="leave", client=client)
            # If leader changed, notify
            await self.maybe_broadcast_election(room)

    async def broadcast_presence(self, room: Room, action: str, client: Client):
        payload = {
            "type": "presence",
            "action": action,  # "join" | "leave" | "snapshot"
            "room": room.name,
            "participants": [
                {"client_id": c.client_id, "username": c.username}
                for c in room.clients.values()
            ],
            "server_ts": now_ms()
        }
        await self._broadcast(room, payload)

    async def broadcast_snapshot(self, room: Room, to_client: Client):
        payload = {
            "type": "presence",
            "action": "snapshot",
            "room": room.name,
            "participants": [
                {"client_id": c.client_id, "username": c.username}
                for c in room.clients.values()
            ],
            "server_ts": now_ms()
        }
        await self._send(to_client, payload)

    async def maybe_broadcast_election(self, room: Room):
        leader = room.leader_id
        if leader is not None:
            payload = {
                "type": "election",
                "room": room.name,
                "leader_id": leader,
                "server_ts": now_ms(),
            }
            await self._broadcast(room, payload)

    async def _send(self, client: Client, data: Dict[str, Any]):
        try:
            await client.ws.send_text(json.dumps(data))
        except Exception:
            # treat as disconnect on error
            await self.disconnect_client(self.room(client.room), client.client_id, reason="send-failed")

    async def _broadcast(self, room: Room, data: Dict[str, Any], exclude: Optional[str] = None):
        text = json.dumps(data)
        stale: List[str] = []
        for cid, client in room.clients.items():
            if cid == exclude:
                continue
            try:
                await client.ws.send_text(text)
            except Exception:
                stale.append(cid)
        for cid in stale:
            await self.disconnect_client(room, cid, reason="broadcast-failed")

hub = Hub()
app = FastAPI()

# CORS (optional; useful if you later add a web UI for testing)
origins = os.getenv("ALLOWED_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins if origins != ["*"] else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"ok": True, "service": SERVER_VERSION}

@app.get("/healthz", response_class=PlainTextResponse)
def healthz():
    return "ok"

@app.websocket("/ws")
async def ws_endpoint(
    websocket: WebSocket,
    room: str = Query(..., min_length=1),
    username: str = Query(..., min_length=1),
    client_id: Optional[str] = Query(None)
):
    await websocket.accept()
    room_obj = hub.room(room)

    # assign or validate client_id
    cid = client_id or ("c_" + uuid.uuid4().hex[:12])
    client = Client(client_id=cid, username=username, room=room, ws=websocket)
    room_obj.clients[cid] = client

    # Welcome + snapshot + election + presence join
    welcome = {
        "type": "welcome",
        "room": room,
        "client_id": cid,
        "leader_id": room_obj.leader_id,
        "server_version": SERVER_VERSION,
        "heartbeat_interval": DEFAULT_HEARTBEAT_MS,
        "server_ts": now_ms(),
    }
    await hub._send(client, welcome)
    await hub.broadcast_snapshot(room_obj, client)
    await hub.broadcast_presence(room_obj, action="join", client=client)
    await hub.maybe_broadcast_election(room_obj)

    try:
        while True:
            raw = await websocket.receive_text()
            if len(raw) > MAX_MSG_SIZE:
                await hub._send(client, {"type": "error", "error": "Message too large"})
                continue

            client.mark_seen()
            try:
                data = json.loads(raw)
            except Exception:
                await hub._send(client, {"type": "error", "error": "Invalid JSON"})
                continue

            mtype = data.get("type")
            if mtype == "heartbeat":
                # just update last_seen
                continue

            if mtype == "history_request":
                limit = int(data.get("limit", 50))
                limit = max(1, min(limit, ROOM_HISTORY_LEN))
                recent = list(room_obj.history)[-limit:]
                payload = {
                    "type": "history",
                    "room": room,
                    "messages": recent,
                    "server_ts": now_ms()
                }
                await hub._send(client, payload)
                continue

            if mtype == "chat":
                if not client.record_message():
                    await hub._send(client, {"type": "error", "error": "rate_limited"})
                    continue

                message = data.get("message", {})
                # minimal validation
                text = message.get("text", "")
                if not isinstance(text, str):
                    await hub._send(client, {"type": "error", "error": "invalid_message"})
                    continue

                # assign defaults/ids
                msg_id = message.get("id") or ("m_" + uuid.uuid4().hex[:12])
                message["id"] = msg_id
                message["sender_id"] = client.client_id
                message["username"] = client.username

                server_seq = room_obj.next_seq()
                payload = {
                    "type": "chat",
                    "room": room,
                    "server_seq": server_seq,
                    "server_ts": now_ms(),
                    "message": message,
                }

                # store in history
                room_obj.history.append(payload)

                # ack to sender
                await hub._send(client, {"type": "ack", "server_seq": server_seq, "id": msg_id, "server_ts": now_ms()})
                # broadcast to others (and also to sender for uniformity)
                await hub._broadcast(room_obj, payload, exclude=None)
                continue

            # unknown type
            await hub._send(client, {"type": "error", "error": f"Unknown type: {mtype}"})

    except WebSocketDisconnect:
        pass
    except Exception:
        # Any unexpected error; treat as disconnect
        pass
    finally:
        # Clean up on disconnect
        await hub.disconnect_client(room_obj, cid, reason="ws-closed")

if __name__ == "__main__":  # local run
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)

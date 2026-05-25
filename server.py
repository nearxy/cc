#!/usr/bin/env python3
"""LAN Clipboard - 局域网跨设备剪贴板"""

import asyncio
import json
import uuid
import socket
import subprocess
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

HOST = "0.0.0.0"
PORT = 18081
MAX_HISTORY = 100

STATIC_DIR = Path(__file__).parent / "static"

clients: dict[str, WebSocket] = {}
message_history: list[dict] = []

async def hourly_cleanup():
    """Clear all messages every hour."""
    while True:
        await asyncio.sleep(3600)
        message_history.clear()
        await broadcast({"type": "clear_all", "payload": {"reason": "hourly"}})


@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(hourly_cleanup())
    yield
    task.cancel()


app = FastAPI(title="LAN Clipboard", lifespan=lifespan)


def get_lan_ips() -> list[str]:
    """Get all LAN IPv4 addresses (excluding loopback and Docker bridges)."""
    ips = []
    try:
        r = subprocess.run(["ip", "-4", "addr", "show"], capture_output=True, text=True, check=True)
        for m in re.finditer(r"inet (\d+\.\d+\.\d+\.\d+)", r.stdout):
            ip = m.group(1)
            if not ip.startswith("127.") and not ip.startswith("172."):
                ips.append(ip)
    except Exception:
        pass

    if not ips:
        # Last-resort fallback
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("10.255.255.255", 1))
            ips.append(s.getsockname()[0])
        except Exception:
            ips.append("127.0.0.1")
        s.close()

    return ips


async def broadcast(data: dict):
    msg = json.dumps(data, ensure_ascii=False)
    dead = []
    for cid, ws in clients.items():
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(cid)
    for cid in dead:
        clients.pop(cid, None)


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index():
    html = (STATIC_DIR / "index.html").read_text("utf-8")
    ips = get_lan_ips()
    ip_json = json.dumps(ips)
    html = html.replace("<!-- LAN_IPS -->", f'<script>window.LAN_IPS={ip_json}</script>')
    return HTMLResponse(html)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    client_id = str(uuid.uuid4())
    clients[client_id] = ws

    try:
        raw = await asyncio.wait_for(ws.receive_text(), timeout=15)
        handshake = json.loads(raw)
        device_name = handshake.get("device_name", f"设备-{client_id[:4]}")

        await ws.send_text(json.dumps({
            "type": "welcome",
            "payload": {
                "client_id": client_id,
                "history": message_history[-50:],
            }
        }, ensure_ascii=False))

        await broadcast({"type": "client_count", "payload": {"count": len(clients)}})

        async for raw in ws.iter_text():
            try:
                msg = json.loads(raw)
                t = msg.get("type")

                if t == "new_message":
                    p = msg["payload"]
                    entry = {
                        "id": str(uuid.uuid4()),
                        "content_type": p.get("content_type", "text"),
                        "content": p.get("content", ""),
                        "device_name": device_name,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                    if "filename" in p:
                        entry["filename"] = p["filename"]
                    message_history.append(entry)
                    if len(message_history) > MAX_HISTORY:
                        message_history.pop(0)
                    await broadcast({"type": "new_message", "payload": entry})

                elif t == "delete_message":
                    msg_id = msg.get("payload", {}).get("id")
                    if msg_id:
                        message_history[:] = [m for m in message_history if m["id"] != msg_id]
                        await broadcast({"type": "delete_message", "payload": {"id": msg_id}})

                elif t == "clear_all":
                    message_history.clear()
                    await broadcast({"type": "clear_all", "payload": {"reason": "manual"}})

            except json.JSONDecodeError:
                pass

    except (asyncio.TimeoutError, WebSocketDisconnect, Exception):
        pass
    finally:
        clients.pop(client_id, None)
        await broadcast({"type": "client_count", "payload": {"count": len(clients)}})


if __name__ == "__main__":
    ips = get_lan_ips()
    sep = "=" * 54
    banner = (
        f"\n{sep}\n"
        f"  LAN 局域网剪贴板已启动!\n"
        f"  {sep}\n"
        f"  本地访问:   http://localhost:{PORT}\n"
    )
    for ip in ips:
        banner += f"  局域网访问: http://{ip}:{PORT}\n"
    banner += (
        f"  {sep}\n"
        f"  同一局域网内的设备请使用浏览器打开以上地址\n"
        f"  支持 纯文本 / 图片 实时同步\n"
        f"  {sep}\n"
    )
    # stderr is unbuffered, ensures banner shows before uvicorn logs
    import sys
    sys.stderr.write(banner)
    sys.stderr.flush()
    uvicorn.run(app, host=HOST, port=PORT)

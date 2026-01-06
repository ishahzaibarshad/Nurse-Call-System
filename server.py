# server.py
import socket
import asyncio
import json
import re
import time
import base64
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from urllib.parse import quote
from typing import Optional, Any, Dict

import uvicorn
import pymysql
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Response
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from pydantic import BaseModel, ConfigDict, Field, AliasChoices
from sqlalchemy import inspect, func, text
from sqlalchemy.orm import joinedload

from decoder import decode_l7700_packet
from camera_stream import CameraManager
from config import Config
from models import (
    get_db_engine, get_db_session, init_database,
    Floor, Ward, Room, Bed, Camera, Event, ColorScheme, CallSession,
    get_karachi_time
)

# ----------------- Globals -----------------

engine = get_db_engine(
    Config.DATABASE_URL,
    Config.DB_SOCKET if Config.DB_USE_SOCKET else None
)

camera_manager = CameraManager(use_simulation=False)
active_connections: Dict[int, WebSocket] = {}


# ----------------- Helpers -----------------

def _s(v) -> str:
    return (v or "").strip()

def _norm(s: str) -> str:
    if not s:
        return ""
    return "".join(ch for ch in s.upper() if ch.isalnum())

def _room_key(name: str) -> str:
    return (name or "").strip()

def _set_if_attr(obj, key: str, value: Any):
    if hasattr(obj, key):
        setattr(obj, key, value)

def ensure_column_safe(table: str, col: str, ddl: str):
    """
    Safe ALTER: runs only after tables exist.
    """
    try:
        inspector = inspect(engine)
        cols = [c["name"] for c in inspector.get_columns(table)]
        if col not in cols:
            with engine.connect() as conn:
                conn.execute(text(f"ALTER TABLE `{table}` ADD COLUMN {ddl}"))
                conn.commit()
            print(f"[DB] Added column {table}.{col}")
    except Exception as e:
        print(f"[DB] ensure_column_safe warning: {e}")

def init_database_tables():
    try:
        print(f"[DB] Using DB: {Config.DB_NAME}")
        if Config.DB_USE_SOCKET:
            print(f"[DB] Using MySQL socket: {Config.DB_SOCKET}")
            connection = pymysql.connect(
                unix_socket=Config.DB_SOCKET,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
                charset="utf8mb4",
            )
        else:
            print(f"[DB] Using TCP: {Config.DB_HOST}:{Config.DB_PORT}")
            connection = pymysql.connect(
                host=Config.DB_HOST,
                port=Config.DB_PORT,
                user=Config.DB_USER,
                password=Config.DB_PASSWORD,
                charset="utf8mb4",
            )

        cursor = connection.cursor()
        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS `{Config.DB_NAME}` "
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        cursor.close()
        connection.close()

        init_database(engine)
        print("[DB] Database and tables initialized successfully")

        # ensure column exists
        ensure_column_safe("beds", "intercom_ip", "`intercom_ip` VARCHAR(50) NULL")

        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print(f"[DB] Tables: {', '.join(tables)}")

    except Exception as e:
        print(f"[DB] Error initializing database: {e}")
        raise


# ----------------- Bed resolving (multi-bed safe) -----------------

def resolve_bed_by_ip(session, incoming_ip: str) -> Optional[Bed]:
    ip = _s(incoming_ip)
    if not ip:
        return None
    return session.query(Bed).options(joinedload(Bed.room)).filter(
        Bed.intercom_ip == ip
    ).order_by(Bed.id.asc()).first()

def resolve_bed_in_room(session, room: Room, decoded: dict) -> Optional[Bed]:
    if not room:
        return None

    beds = session.query(Bed).filter(Bed.room_id == room.id).all()
    if not beds:
        return None

    tokens = []
    for key in ("bed_name", "bed", "room_name", "room", "device"):
        v = _s(decoded.get(key))
        if v and v not in tokens:
            tokens.append(v)

    # exact match bed_number / bed_name
    for t in tokens:
        tl = t.lower()
        for b in beds:
            if _s(getattr(b, "bed_number", "")).lower() == tl:
                return b
            if _s(getattr(b, "bed_name", "")).lower() == tl:
                return b

    # normalized fuzzy match
    ntokens = [_norm(t) for t in tokens if t]
    for b in beds:
        bn = _norm(_s(getattr(b, "bed_number", "")))
        bname = _norm(_s(getattr(b, "bed_name", "")))
        for nt in ntokens:
            if not nt:
                continue
            if (nt in bname) or (nt in bn) or (bname and bname in nt) or (bn and bn in nt):
                return b

    # numeric match
    joined = " ".join(tokens)
    nums = re.findall(r"\b\d+\b", joined)
    if nums:
        for n in nums:
            for b in beds:
                if _s(getattr(b, "bed_number", "")) == n:
                    return b

    # fallback
    return sorted(beds, key=lambda x: x.id)[0]


# ----------------- CallSession helper -----------------

def get_or_create_call_session(session, bed_id, event_type):
    cs = session.query(CallSession).filter(
        CallSession.bed_id == bed_id,
        CallSession.status == "active"
    ).first()

    if cs:
        cs.current_event_type = event_type
        session.commit()
        return cs

    cs = CallSession(
        bed_id=bed_id,
        current_event_type=event_type,
        status="active"
    )
    session.add(cs)
    session.commit()
    return cs


# ----------------- Cameras / Serialization -----------------

def get_room_camera(session, room: Room):
    if not room:
        return None
    return session.query(Camera).filter(
        Camera.room_id == room.id,
        Camera.status == "active"
    ).order_by(Camera.id.desc()).first()

def serialize_event_with_camera(session, event: Event):
    bed = event.bed
    room = event.room or (bed.room if bed else None)
    ward = room.ward if room else None
    floor = ward.floor if ward else None

    cam = get_room_camera(session, room)
    room_name = _room_key(room.room_name if room else None)

    streaming_rooms = set(camera_manager.get_all_rooms())
    camera_streaming = bool(room_name) and (room_name in streaming_rooms)
    camera_live = bool(room_name) and camera_manager.has_frame(room_name)
    cam_url = f"/camera/{quote(room_name, safe='')}" if camera_streaming else None

    ts = event.system_timestamp
    ts_str = ts.strftime("%Y-%m-%dT%H:%M:%S") if ts else None

    return {
        "id": event.id,
        "callSessionId": event.call_session_id,
        "eventType": event.event_type,
        "status": event.status,
        "systemTimestamp": ts_str,

        "bedId": bed.id if bed else None,
        "bedName": bed.bed_name if bed else None,
        "bedNumber": bed.bed_number if bed else None,

        "roomId": room.id if room else None,
        "roomName": room_name or None,
        "roomNumber": room.room_number if room else None,

        "wardName": ward.name if ward else None,
        "floorName": floor.name if floor else None,

        "cameraAvailable": camera_streaming,
        "cameraLive": camera_live,
        "cameraUrl": cam_url,
        "cameraConfigured": True if cam else False,

        "intercomIp": getattr(bed, "intercom_ip", None) if bed else None,
    }

def mjpeg_generator(room_name: str, fps: int = 10):
    room_name = _room_key(room_name)
    frame_delay = 1.0 / max(1, int(fps))
    last_placeholder = 0.0

    while True:
        frame = camera_manager.get_frame(room_name)

        if frame:
            payload = frame
            time.sleep(frame_delay)
        else:
            now = time.time()
            if now - last_placeholder < 1.0:
                time.sleep(0.05)
                continue
            last_placeholder = now
            payload = _PLACEHOLDER_JPEG

        yield (
            b"--frame\r\n"
            b"Content-Type: image/jpeg\r\n"
            b"Cache-Control: no-store\r\n"
            b"Content-Length: " + str(len(payload)).encode() + b"\r\n\r\n" +
            payload + b"\r\n"
        )

_PLACEHOLDER_JPEG = base64.b64decode(
    b"/9j/4AAQSkZJRgABAQAAAQABAAD/2wCEAAkGBxAQEBUQEA8QFQ8QDw8QDw8PDw8PFRUWFhUVFRUY"
    b"HSggGBolGxUVITEhJSkrLi4uFx8zODMsNygtLisBCgoKDg0OGxAQGy0lICUtLS0tLS0tLS0tLS0t"
    b"LS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLf/AABEIAH4A5QMBIgACEQEDEQH/xAAX"
    b"AAEBAQEAAAAAAAAAAAAAAAAAAQID/8QAHhAAAQQCAwEAAAAAAAAAAAAAAQACAxESITFBYXH/xAAX"
    b"AQEBAQEAAAAAAAAAAAAAAAAAAQID/8QAHBEBAAICAwEAAAAAAAAAAAAAAAECEQMhEjFB/9oADAMB"
    b"AAIRAxEAPwDqkYq0Gm1l8p3Wqkq6zq0gK1kQmQp7j2p0o3bGx1m2m1yYq1pWkq0c1QmGmQ+9T9m"
    b"j6yN2s2j2yqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkq"
    b"gqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqk"
    b"qgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgq"
    b"kqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqgqkqg//9k="
)


# ----------------- Lifespan -----------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_database_tables()

    session = get_db_session(engine)
    try:
        cameras = session.query(Camera).options(joinedload(Camera.room)).filter(Camera.status == "active").all()
        for cam in cameras:
            if cam.room and cam.rtsp_url:
                try:
                    camera_manager.add_camera(cam.room.room_name, cam.rtsp_url)
                except Exception as e:
                    print(f"[Camera] Failed to start camera id={cam.id}: {e}")
    finally:
        session.close()

    asyncio.create_task(udp_listener())
    yield
    camera_manager.shutdown()


app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")


# ----------------- Pages -----------------

@app.get("/")
async def get_home():
    with open("templates/dashboard.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/calls")
async def get_calls_page():
    with open("templates/calls.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/history")
async def get_history_page():
    with open("templates/history.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/config")
async def get_config_page():
    with open("templates/config.html", "r") as f:
        return HTMLResponse(content=f.read())

@app.get("/sidebar.html")
async def get_sidebar_html():
    try:
        with open("templates/sidebar.html", "r") as f:
            return HTMLResponse(content=f.read())
    except Exception:
        return HTMLResponse(content="", status_code=404)


# ----------------- Camera Streams -----------------

@app.head("/camera/{room_name:path}")
async def camera_by_room_head(room_name: str):
    return Response(status_code=200)

@app.get("/camera/{room_name:path}")
async def camera_by_room(room_name: str):
    room_name = _room_key(room_name)

    if room_name not in camera_manager.get_all_rooms():
        return JSONResponse(
            {"ok": False, "success": False, "status": "error", "error": f"No active camera stream for room: {room_name}"},
            status_code=404
        )

    return StreamingResponse(
        mjpeg_generator(room_name, fps=10),
        media_type="multipart/x-mixed-replace; boundary=frame",
        headers={"Cache-Control": "no-store", "Pragma": "no-cache", "X-Accel-Buffering": "no"}
    )


# ----------------- APIs -----------------

@app.get("/api/stats")
async def get_stats():
    session = get_db_session(engine)
    try:
        seven_days_ago = datetime.now() - timedelta(days=7)
        one_hour_ago = datetime.now() - timedelta(hours=1)

        total_active = session.query(Event).filter(
            Event.created_at >= seven_days_ago,
            Event.status == "active"
        ).count()

        urgent_alarms = session.query(Event).filter(
            Event.created_at >= seven_days_ago,
            Event.event_type.in_(("Emergency", "Alarm", "Assistance")),
            Event.status == "active"
        ).count()

        ongoing_calls = session.query(Event).filter(
            Event.created_at >= seven_days_ago,
            Event.event_type == "Call",
            Event.status == "active"
        ).count()

        recently_cleared = session.query(Event).filter(
            Event.created_at >= one_hour_ago,
            Event.status == "cleared"
        ).count()

        return {
            "total_active": total_active,
            "urgent_alarms": urgent_alarms,
            "ongoing_calls": ongoing_calls,
            "recently_cleared": recently_cleared
        }
    finally:
        session.close()

@app.get("/api/events/recent")
async def get_recent_events(limit: int = 100):
    session = get_db_session(engine)
    try:
        events = session.query(Event).options(
            joinedload(Event.bed).joinedload(Bed.room).joinedload(Room.ward).joinedload(Ward.floor),
            joinedload(Event.room).joinedload(Room.ward).joinedload(Ward.floor),
        ).order_by(Event.id.desc()).limit(limit).all()

        return [serialize_event_with_camera(session, e) for e in events]
    finally:
        session.close()

@app.get("/api/calls")
async def api_calls(limit: int = 100, only_active: bool = True):
    session = get_db_session(engine)
    try:
        q = session.query(Event).options(
            joinedload(Event.bed).joinedload(Bed.room).joinedload(Room.ward).joinedload(Ward.floor),
            joinedload(Event.room).joinedload(Room.ward).joinedload(Ward.floor),
        ).order_by(Event.id.desc())

        if only_active:
            q = q.filter(Event.status == "active")

        events = q.limit(limit).all()
        return [serialize_event_with_camera(session, e) for e in events]
    finally:
        session.close()

@app.get("/api/timeline")
async def api_timeline(days: int = 7):
    session = get_db_session(engine)
    try:
        if days < 1:
            days = 1
        start = datetime.now() - timedelta(days=days)

        rows = session.query(
            func.date(Event.created_at).label("d"),
            Event.event_type,
            func.count(Event.id)
        ).filter(
            Event.created_at >= start
        ).group_by(
            func.date(Event.created_at),
            Event.event_type
        ).all()

        timeline: Dict[str, Dict[str, int]] = {}
        for d, event_type, cnt in rows:
            if d is None:
                continue
            ds = str(d)
            timeline.setdefault(ds, {})
            timeline[ds][event_type or "Unknown"] = int(cnt or 0)

        return {"timeline": timeline}
    finally:
        session.close()


# ----------------- Config GET endpoints (no duplicate keys) -----------------

@app.get("/api/config/floors")
async def get_floors():
    session = get_db_session(engine)
    try:
        floors = session.query(Floor).order_by(Floor.floor_number.asc()).all()
        out = []
        for f in floors:
            wards_count = session.query(func.count(Ward.id)).filter(Ward.floor_id == f.id).scalar() or 0
            out.append({
                "id": f.id,
                "name": f.name,
                "floorNumber": f.floor_number,
                "description": f.description,
                "status": f.status,
                "wards": int(wards_count),
            })
        return {"floors": out}
    finally:
        session.close()

@app.get("/api/config/wards")
async def get_wards():
    session = get_db_session(engine)
    try:
        wards = session.query(Ward).order_by(Ward.id.asc()).all()
        out = []
        for w in wards:
            rooms_count = session.query(func.count(Room.id)).filter(Room.ward_id == w.id).scalar() or 0
            out.append({
                "id": w.id,
                "name": w.name,
                "floorId": w.floor_id,
                "wardType": w.ward_type,
                "description": w.description,
                "status": w.status,
                "rooms": int(rooms_count),
            })
        return {"wards": out}
    finally:
        session.close()

@app.get("/api/config/rooms")
async def get_rooms():
    session = get_db_session(engine)
    try:
        rooms = session.query(Room).all()
        return {"rooms": [{
            "id": r.id,
            "roomName": r.room_name,
            "roomNumber": r.room_number,
            "wardId": r.ward_id,
            "systemIp": r.system_ip,
            "status": r.status,
        } for r in rooms]}
    finally:
        session.close()

@app.get("/api/config/beds")
async def get_beds():
    session = get_db_session(engine)
    try:
        beds = session.query(Bed).options(joinedload(Bed.room)).all()
        return {"beds": [{
            "id": b.id,
            "roomId": b.room_id,
            "roomName": b.room.room_name if b.room else None,
            "bedName": b.bed_name,
            "bedNumber": b.bed_number,
            "cameraId": b.camera_id,
            "status": b.status,
            "ipAddress": getattr(b, "intercom_ip", None),
        } for b in beds]}
    finally:
        session.close()

@app.get("/api/config/colors")
async def get_colors():
    session = get_db_session(engine)
    try:
        colors = session.query(ColorScheme).all()
        return {"colors": [{
            "id": c.id,
            "eventType": c.event_type,
            "color": c.color
        } for c in colors]}
    finally:
        session.close()


# ----------------- ✅ Config CRUD (aliases friendly) -----------------

class FloorUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: Optional[str] = Field(default=None, validation_alias=AliasChoices("name"))
    floor_number: Optional[int] = Field(default=None, validation_alias=AliasChoices("floor_number", "floorNumber", "no", "floorNo"))
    description: Optional[str] = None
    status: Optional[str] = None

class WardUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: Optional[str] = Field(default=None, validation_alias=AliasChoices("name"))
    floor_id: Optional[int] = Field(default=None, validation_alias=AliasChoices("floor_id", "floorId"))
    ward_type: Optional[str] = Field(default=None, validation_alias=AliasChoices("ward_type", "wardType", "type"))
    description: Optional[str] = None
    status: Optional[str] = None

class RoomUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ward_id: Optional[int] = Field(default=None, validation_alias=AliasChoices("ward_id", "wardId"))
    room_name: Optional[str] = Field(default=None, validation_alias=AliasChoices("room_name", "roomName", "name"))
    room_number: Optional[str] = Field(default=None, validation_alias=AliasChoices("room_number", "roomNumber", "number"))
    system_ip: Optional[str] = Field(default=None, validation_alias=AliasChoices("system_ip", "systemIp", "ip"))
    status: Optional[str] = None

class BedUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    room_id: Optional[int] = Field(default=None, validation_alias=AliasChoices("room_id", "roomId"))
    room: Optional[Any] = Field(default=None, validation_alias=AliasChoices("room", "roomName", "room_name", "roomNumber", "room_number"))
    bed_name: Optional[str] = Field(default=None, validation_alias=AliasChoices("bed_name", "bedName"))
    bed_number: Optional[str] = Field(default=None, validation_alias=AliasChoices("bed_number", "bedNumber", "bedNo", "bed"))
    camera_id: Optional[int] = Field(default=None, validation_alias=AliasChoices("camera_id", "cameraId"))
    status: Optional[str] = Field(default=None, validation_alias=AliasChoices("status"))
    ip_address: Optional[str] = Field(default=None, validation_alias=AliasChoices("ip_address", "ipAddress", "ip", "intercom_ip", "intercomIp"))

class ColorUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    event_type: Optional[str] = Field(default=None, validation_alias=AliasChoices("event_type", "eventType", "type"))
    color: Optional[str] = Field(default=None, validation_alias=AliasChoices("color"))

def _resolve_room_id_for_bed(session, payload: BedUpsert) -> Optional[int]:
    if payload.room_id:
        return int(payload.room_id)

    if payload.room is not None:
        if isinstance(payload.room, int):
            return payload.room
        if isinstance(payload.room, str):
            rname = payload.room.strip()
            if rname.isdigit():
                return int(rname)

            r = session.query(Room).filter(
                (func.lower(Room.room_name) == rname.lower()) |
                (func.lower(Room.room_number) == rname.lower())
            ).first()
            if r:
                return r.id

    return None

def _bed_ip(payload: BedUpsert) -> Optional[str]:
    v = _s(payload.ip_address)
    return v or None

def _bed_out(b: Bed, room: Optional[Room] = None) -> dict:
    rn = room.room_name if room else None
    return {
        "id": b.id,
        "roomId": b.room_id,
        "roomName": rn,
        "bedName": b.bed_name,
        "bedNumber": b.bed_number,
        "cameraId": b.camera_id,
        "status": b.status,
        "ipAddress": getattr(b, "intercom_ip", None),
    }

def _ensure_intercom_ip_unique(session, ip: str, ignore_bed_id: Optional[int] = None) -> Optional[str]:
    ip = _s(ip)
    if not ip:
        return None
    q = session.query(Bed).filter(Bed.intercom_ip == ip)
    if ignore_bed_id is not None:
        q = q.filter(Bed.id != int(ignore_bed_id))
    exists = q.first()
    if exists:
        return f"intercom ip '{ip}' already used by bed id={exists.id}"
    return None


# ---------- Floors CRUD ----------
@app.post("/api/config/floors")
@app.put("/api/config/floors")
async def create_floor(payload: FloorUpsert):
    session = get_db_session(engine)
    try:
        data = payload.model_dump(exclude_unset=True)

        if not _s(data.get("name")):
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "name is required"}, status_code=422)
        if data.get("floor_number") is None:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "floor_number is required"}, status_code=422)

        f = Floor()
        f.name = _s(data.get("name"))
        f.floor_number = int(data.get("floor_number"))
        f.description = data.get("description")
        f.status = data.get("status") or "active"

        session.add(f)
        session.commit()
        session.refresh(f)
        return {"ok": True, "success": True, "status": "success", "message": "Floor created", "floor": {
            "id": f.id, "name": f.name, "floorNumber": f.floor_number,
            "description": f.description, "status": f.status
        }}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()

@app.put("/api/config/floors/{floor_id}")
@app.patch("/api/config/floors/{floor_id}")
@app.post("/api/config/floors/{floor_id}")
async def update_floor(floor_id: int, payload: FloorUpsert):
    session = get_db_session(engine)
    try:
        f = session.query(Floor).filter(Floor.id == floor_id).first()
        if not f:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Floor not found"}, status_code=404)

        data = payload.model_dump(exclude_unset=True)
        if "name" in data:
            f.name = _s(data.get("name"))
        if "floor_number" in data and data.get("floor_number") is not None:
            f.floor_number = int(data.get("floor_number"))
        if "description" in data:
            f.description = data.get("description")
        if "status" in data:
            f.status = data.get("status") or f.status

        session.commit()
        session.refresh(f)
        return {"ok": True, "success": True, "status": "success", "message": "Floor saved", "floor": {
            "id": f.id, "name": f.name, "floorNumber": f.floor_number,
            "description": f.description, "status": f.status
        }}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()

@app.delete("/api/config/floors/{floor_id}")
async def delete_floor(floor_id: int):
    session = get_db_session(engine)
    try:
        f = session.query(Floor).filter(Floor.id == floor_id).first()
        if not f:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Floor not found"}, status_code=404)
        session.delete(f)
        session.commit()
        return {"ok": True, "success": True, "status": "success", "message": "Floor deleted"}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()


# ---------- Wards CRUD ----------
@app.post("/api/config/wards")
@app.put("/api/config/wards")
async def create_ward(payload: WardUpsert):
    session = get_db_session(engine)
    try:
        data = payload.model_dump(exclude_unset=True)

        if not _s(data.get("name")):
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "name is required"}, status_code=422)
        if data.get("floor_id") is None:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "floor_id is required"}, status_code=422)

        w = Ward()
        w.name = _s(data.get("name"))
        w.floor_id = int(data.get("floor_id"))
        w.ward_type = data.get("ward_type")
        w.description = data.get("description")
        w.status = data.get("status") or "active"

        session.add(w)
        session.commit()
        session.refresh(w)

        return {"ok": True, "success": True, "status": "success", "message": "Ward created", "ward": {
            "id": w.id, "name": w.name, "floorId": w.floor_id,
            "wardType": w.ward_type, "description": w.description, "status": w.status
        }}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()

@app.put("/api/config/wards/{ward_id}")
@app.patch("/api/config/wards/{ward_id}")
@app.post("/api/config/wards/{ward_id}")
async def update_ward(ward_id: int, payload: WardUpsert):
    session = get_db_session(engine)
    try:
        w = session.query(Ward).filter(Ward.id == ward_id).first()
        if not w:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Ward not found"}, status_code=404)

        data = payload.model_dump(exclude_unset=True)
        if "name" in data:
            w.name = _s(data.get("name"))
        if "floor_id" in data and data.get("floor_id") is not None:
            w.floor_id = int(data.get("floor_id"))
        if "ward_type" in data:
            w.ward_type = data.get("ward_type") or w.ward_type
        if "description" in data:
            w.description = data.get("description")
        if "status" in data:
            w.status = data.get("status") or w.status

        session.commit()
        session.refresh(w)

        return {"ok": True, "success": True, "status": "success", "message": "Ward saved", "ward": {
            "id": w.id, "name": w.name, "floorId": w.floor_id,
            "wardType": w.ward_type, "description": w.description, "status": w.status
        }}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()

@app.delete("/api/config/wards/{ward_id}")
async def delete_ward(ward_id: int):
    session = get_db_session(engine)
    try:
        w = session.query(Ward).filter(Ward.id == ward_id).first()
        if not w:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Ward not found"}, status_code=404)
        session.delete(w)
        session.commit()
        return {"ok": True, "success": True, "status": "success", "message": "Ward deleted"}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()


# ---------- Rooms CRUD ----------
@app.post("/api/config/rooms")
@app.put("/api/config/rooms")
async def create_room(payload: RoomUpsert):
    session = get_db_session(engine)
    try:
        r = Room()
        data = payload.model_dump(exclude_unset=True)
        for k, v in data.items():
            _set_if_attr(r, k, v)

        # prevent duplicate system_ip
        rip = _s(getattr(r, "system_ip", None))
        if rip:
            exists = session.query(Room).filter(Room.system_ip == rip).first()
            if exists:
                return JSONResponse(
                    {"ok": False, "success": False, "status": "error", "error": "system_ip already exists in another room"},
                    status_code=400
                )

        session.add(r)
        session.commit()
        session.refresh(r)

        return {"ok": True, "success": True, "status": "success", "message": "Room created", "room": {
            "id": r.id,
            "wardId": r.ward_id,
            "roomName": r.room_name,
            "roomNumber": r.room_number,
            "systemIp": r.system_ip,
            "status": r.status
        }}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()

@app.put("/api/config/rooms/{room_id}")
@app.patch("/api/config/rooms/{room_id}")
async def update_room(room_id: int, payload: RoomUpsert):
    session = get_db_session(engine)
    try:
        r = session.query(Room).filter(Room.id == room_id).first()
        if not r:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Room not found"}, status_code=404)

        data = payload.model_dump(exclude_unset=True)
        for k, v in data.items():
            _set_if_attr(r, k, v)

        rip = _s(getattr(r, "system_ip", None))
        if rip:
            exists = session.query(Room).filter(Room.system_ip == rip, Room.id != r.id).first()
            if exists:
                return JSONResponse(
                    {"ok": False, "success": False, "status": "error", "error": "system_ip already exists in another room"},
                    status_code=400
                )

        session.commit()
        session.refresh(r)

        return {"ok": True, "success": True, "status": "success", "message": "Room saved", "room": {
            "id": r.id,
            "wardId": r.ward_id,
            "roomName": r.room_name,
            "roomNumber": r.room_number,
            "systemIp": r.system_ip,
            "status": r.status
        }}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()

@app.delete("/api/config/rooms/{room_id}")
async def delete_room(room_id: int):
    session = get_db_session(engine)
    try:
        r = session.query(Room).filter(Room.id == room_id).first()
        if not r:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Room not found"}, status_code=404)
        session.delete(r)
        session.commit()
        return {"ok": True, "success": True, "status": "success", "message": "Room deleted"}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()


# ---------- Beds CRUD ----------
@app.post("/api/config/beds")
@app.put("/api/config/beds")
@app.post("/api/config/bed")
@app.put("/api/config/bed")
@app.post("/api/beds")
@app.put("/api/beds")
@app.post("/api/bed")
@app.put("/api/bed")
async def create_bed(payload: BedUpsert):
    session = get_db_session(engine)
    try:
        rid = _resolve_room_id_for_bed(session, payload)
        if rid is None:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "roomId/room is required"}, status_code=422)

        if not _s(payload.bed_number):
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "bedNumber is required"}, status_code=422)

        ip = _bed_ip(payload)
        if ip:
            msg = _ensure_intercom_ip_unique(session, ip)
            if msg:
                return JSONResponse({"ok": False, "success": False, "status": "error", "error": msg}, status_code=400)

        b = Bed()
        b.room_id = rid
        b.bed_number = _s(payload.bed_number)
        b.bed_name = _s(payload.bed_name) or None
        b.camera_id = payload.camera_id
        b.status = payload.status or "available"
        b.intercom_ip = ip

        session.add(b)
        session.commit()
        session.refresh(b)

        room = session.query(Room).filter(Room.id == b.room_id).first()
        return {"ok": True, "success": True, "status": "success", "message": "Bed created", "bed": _bed_out(b, room)}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()

@app.put("/api/config/beds/{bed_id}")
@app.patch("/api/config/beds/{bed_id}")
@app.post("/api/config/beds/{bed_id}")
@app.post("/api/config/beds/{bed_id}/edit")
@app.post("/api/config/beds/{bed_id}/update")
async def update_bed(bed_id: int, payload: BedUpsert):
    session = get_db_session(engine)
    try:
        b = session.query(Bed).filter(Bed.id == bed_id).first()
        if not b:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Bed not found"}, status_code=404)

        rid = _resolve_room_id_for_bed(session, payload)
        if rid is not None:
            b.room_id = rid

        if payload.bed_number is not None:
            b.bed_number = _s(payload.bed_number)

        if payload.bed_name is not None:
            b.bed_name = _s(payload.bed_name) or None

        if payload.camera_id is not None:
            b.camera_id = payload.camera_id

        if payload.status is not None:
            b.status = payload.status or b.status

        ip = _bed_ip(payload)
        if payload.ip_address is not None:
            # uniqueness check if ip is being set
            if ip:
                msg = _ensure_intercom_ip_unique(session, ip, ignore_bed_id=bed_id)
                if msg:
                    return JSONResponse({"ok": False, "success": False, "status": "error", "error": msg}, status_code=400)
            b.intercom_ip = ip  # allow setting None/"" -> null

        session.commit()
        session.refresh(b)

        room = session.query(Room).filter(Room.id == b.room_id).first()
        return {"ok": True, "success": True, "status": "success", "message": "Bed saved", "bed": _bed_out(b, room)}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()

@app.delete("/api/config/beds/{bed_id}")
async def delete_bed(bed_id: int):
    session = get_db_session(engine)
    try:
        b = session.query(Bed).filter(Bed.id == bed_id).first()
        if not b:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Bed not found"}, status_code=404)

        session.delete(b)
        session.commit()
        return {"ok": True, "success": True, "status": "success", "message": "Bed deleted"}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()


# ---------- Colors CRUD ----------
@app.post("/api/config/colors")
@app.put("/api/config/colors")
async def create_color(payload: ColorUpsert):
    session = get_db_session(engine)
    try:
        data = payload.model_dump(exclude_unset=True)
        et = _s(data.get("event_type"))
        col = _s(data.get("color"))
        if not et or not col:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "eventType and color required"}, status_code=422)

        c = ColorScheme(event_type=et, color=col)
        session.add(c)
        session.commit()
        session.refresh(c)
        return {"ok": True, "success": True, "status": "success", "message": "Color created", "color": {
            "id": c.id, "eventType": c.event_type, "color": c.color
        }}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()

@app.put("/api/config/colors/{color_id}")
@app.patch("/api/config/colors/{color_id}")
async def update_color(color_id: int, payload: ColorUpsert):
    session = get_db_session(engine)
    try:
        c = session.query(ColorScheme).filter(ColorScheme.id == color_id).first()
        if not c:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Color not found"}, status_code=404)

        data = payload.model_dump(exclude_unset=True)
        if "event_type" in data and data.get("event_type"):
            c.event_type = _s(data.get("event_type"))
        if "color" in data and data.get("color"):
            c.color = _s(data.get("color"))

        session.commit()
        session.refresh(c)
        return {"ok": True, "success": True, "status": "success", "message": "Color saved", "color": {
            "id": c.id, "eventType": c.event_type, "color": c.color
        }}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()

@app.delete("/api/config/colors/{color_id}")
async def delete_color(color_id: int):
    session = get_db_session(engine)
    try:
        c = session.query(ColorScheme).filter(ColorScheme.id == color_id).first()
        if not c:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Color not found"}, status_code=404)
        session.delete(c)
        session.commit()
        return {"ok": True, "success": True, "status": "success", "message": "Color deleted"}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()


# ----------------- WebSocket -----------------

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    client_id = id(ws)
    active_connections[client_id] = ws
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        active_connections.pop(client_id, None)


# ----------------- UDP Listener -----------------

async def udp_listener():
    loop = asyncio.get_running_loop()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        sock.bind((Config.UDP_IP, Config.UDP_PORT))
        print(f"✓ UDP listener bound to {Config.UDP_IP}:{Config.UDP_PORT}")
    except OSError as e:
        print(f"✗ Error binding UDP {Config.UDP_PORT}: {e}")
        return

    print(f"🎧 Listening for L7700 UDP on {Config.UDP_IP}:{Config.UDP_PORT}")
    print(f"⏰ {get_karachi_time().strftime('%Y-%m-%d %H:%M:%S')}")

    while True:
        try:
            data, addr = await loop.run_in_executor(None, sock.recvfrom, 2048)
            incoming_ip = addr[0]

            decoded = decode_l7700_packet(data)
            if not decoded:
                continue

            session = get_db_session(engine)
            try:
                # 1) Best: Bed match by intercom_ip == incoming_ip (multi-bed correct)
                bed = resolve_bed_by_ip(session, incoming_ip)
                room = bed.room if bed else None

                # 2) Fallback: if bed not found but room found by system_ip
                if not room:
                    room = session.query(Room).filter(Room.system_ip == incoming_ip).first()

                # 3) If room found but bed missing -> try resolve by decoded tokens (bedName / bedNumber)
                if room and not bed:
                    bed = resolve_bed_in_room(session, room, decoded)

                    # optional auto-learn ONLY if bed has no intercom_ip (safe)
                    if bed and not _s(getattr(bed, "intercom_ip", None)):
                        # uniqueness guard
                        msg = _ensure_intercom_ip_unique(session, incoming_ip, ignore_bed_id=bed.id)
                        if not msg:
                            bed.intercom_ip = incoming_ip
                            session.commit()

                system_time = get_karachi_time()
                event_type = decoded.get("event") or "Unknown"

                room_id = room.id if room else (bed.room_id if bed else None)
                bed_id = bed.id if bed else None

                # reset ends session
                if event_type.lower() == "reset" and bed_id:
                    cs = session.query(CallSession).filter(
                        CallSession.bed_id == bed_id,
                        CallSession.status == "active"
                    ).first()
                    if cs:
                        cs.status = "ended"
                        cs.ended_at = system_time
                        session.commit()

                call_session_id = None
                if event_type.lower() != "reset" and bed_id:
                    cs = get_or_create_call_session(session, bed_id, event_type)
                    call_session_id = cs.id

                room_identifier = decoded.get("room_name") or decoded.get("room") or None
                device_type = decoded.get("device") or "UNKNOWN"

                print(f"[UDP] ip={incoming_ip} room={room.room_name if room else None} bed={bed.bed_name if bed else None} event={event_type}")

                event = Event(
                    room_id=room_id,
                    bed_id=bed_id,
                    call_session_id=call_session_id,
                    device_timestamp=decoded.get("timestamp"),
                    system_timestamp=system_time,
                    room_identifier=room_identifier,
                    device_type=device_type,
                    event_type=event_type,
                    status="active",
                    raw_hex=decoded.get("raw_hex")
                )
                session.add(event)
                session.commit()

                payload = serialize_event_with_camera(session, event)
                message = json.dumps(payload)

                dead = []
                for cid, w in active_connections.items():
                    try:
                        await w.send_text(message)
                    except Exception:
                        dead.append(cid)
                for cid in dead:
                    active_connections.pop(cid, None)

            except Exception as e:
                session.rollback()
                print(f"✗ UDP/DB error: {e}")
            finally:
                session.close()

        except Exception as e:
            print(f"✗ UDP loop error: {e}")


if __name__ == "__main__":
    uvicorn.run("server:app", host=Config.SERVER_HOST, port=Config.SERVER_PORT, reload=False)

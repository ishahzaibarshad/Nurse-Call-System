
import os
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
from sqlalchemy import inspect, func, text
from sqlalchemy.orm import joinedload
from fastapi import (
    FastAPI, WebSocket, WebSocketDisconnect, Request,
    HTTPException, status, Form, Depends
)
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from passlib.context import CryptContext
from starlette.middleware.sessions import SessionMiddleware

from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from pydantic import BaseModel, ConfigDict, Field

from decoder import decode_l7700_packet
from camera_stream import CameraManager
from config import Config
from models import (
    get_db_engine, get_db_session, init_database,
    Floor, Department, Wing, Ward, Room, Bed, Camera, Event as EventModel, ColorScheme, CallSession,
    get_karachi_time
)


# ----------------- Globals -----------------

SECRET_KEY = "your-secret-key-change-this-in-production"

engine = get_db_engine(
    Config.DATABASE_URL,
    Config.DB_SOCKET if Config.DB_USE_SOCKET else None
)

SQLALCHEMY_DATABASE_URL = Config.DATABASE_URL
user_engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_pre_ping=True)

UserSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=user_engine)

Base = declarative_base()

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    password_hash = Column(String)
    role = Column(String)

Base.metadata.create_all(bind=user_engine)

camera_manager = CameraManager(use_simulation=False)
active_connections: Dict[int, WebSocket] = {}

templates = Jinja2Templates(directory="templates")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

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

def now_karachi_naive() -> datetime:
    try:
        return get_karachi_time().replace(tzinfo=None)
    except Exception:
        return datetime.now()

def ensure_column_safe(table: str, col: str, ddl: str):
    try:
        inspector = inspect(engine)
        cols = [c["name"] for c in inspector.get_columns(table)]
        if col not in cols:
            with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE `{table}` ADD COLUMN {ddl}"))
            print(f"[DB] Added column {table}.{col}")
    except Exception as e:
        print(f"[DB] ensure_column_safe warning: {e}")

def ensure_table_safe(create_sql: str, table_name: str):
    """
    Create table if not exists, safe for repeated runs.
    """
    try:
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        if table_name not in tables:
            with engine.begin() as conn:
                conn.execute(text(create_sql))
            print(f"[DB] Created table '{table_name}'")
    except Exception as e:
        print(f"[DB] ensure_table_safe warning for '{table_name}': {e}")

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

        # Existing
        ensure_column_safe("beds", "intercom_ip", "`intercom_ip` VARCHAR(50) NULL")

        # ✅ NEW: Departments & Wings tables (no ORM changes needed)
        ensure_table_safe(
            """
            CREATE TABLE IF NOT EXISTS `departments` (
              `id` INT NOT NULL AUTO_INCREMENT,
              `name` VARCHAR(120) NOT NULL,
              `floor_id` INT NOT NULL,
              `description` VARCHAR(255) NULL,
              `status` VARCHAR(20) NOT NULL DEFAULT 'active',
              `created_at` DATETIME NULL,
              `updated_at` DATETIME NULL,
              PRIMARY KEY (`id`),
              INDEX `idx_departments_floor` (`floor_id`),
              INDEX `idx_departments_status` (`status`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """,
            "departments"
        )

        ensure_table_safe(
            """
            CREATE TABLE IF NOT EXISTS `wings` (
              `id` INT NOT NULL AUTO_INCREMENT,
              `name` VARCHAR(120) NOT NULL,
              `department_id` INT NOT NULL,
              `description` VARCHAR(255) NULL,
              `status` VARCHAR(20) NOT NULL DEFAULT 'active',
              `created_at` DATETIME NULL,
              `updated_at` DATETIME NULL,
              PRIMARY KEY (`id`),
              INDEX `idx_wings_department` (`department_id`),
              INDEX `idx_wings_status` (`status`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """,
            "wings"
        )

        # ✅ Optional (for future hierarchy): add nullable department_id and wing_id to wards table.
        # This does NOT break your existing system if not used.
        ensure_column_safe("wards", "department_id", "`department_id` INT NULL")
        ensure_column_safe("wards", "wing_id", "`wing_id` INT NULL")

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
    return (
        session.query(Bed)
        .options(joinedload(Bed.room))
        .filter(Bed.intercom_ip == ip)
        .order_by(Bed.id.asc())
        .first()
    )

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

    for t in tokens:
        tl = t.lower()
        for b in beds:
            if _s(getattr(b, "bed_number", "")).lower() == tl:
                return b
            if _s(getattr(b, "bed_name", "")).lower() == tl:
                return b

    ntokens = [_norm(t) for t in tokens if t]
    for b in beds:
        bn = _norm(_s(getattr(b, "bed_number", "")))
        bname = _norm(_s(getattr(b, "bed_name", "")))
        for nt in ntokens:
            if not nt:
                continue
            if (nt in bname) or (nt in bn) or (bname and bname in nt) or (bn and bn in nt):
                return b

    joined = " ".join(tokens)
    nums = re.findall(r"\b\d+\b", joined)
    if nums:
        for n in nums:
            for b in beds:
                if _s(getattr(b, "bed_number", "")) == n:
                    return b

    return sorted(beds, key=lambda x: x.id)[0]

# ----------------- CallSession helper -----------------

def get_or_create_call_session(session, bed_id, event_type):
    cs = (
        session.query(CallSession)
        .filter(CallSession.bed_id == bed_id, CallSession.status == "active")
        .first()
    )

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

# ----------------- Authentication Helpers -----------------

def get_user_db():
    db = UserSessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_current_user(request: Request):
    username = request.session.get("username")
    role = request.session.get("role")
    if not username or not role:
        return None
    return {"username": username, "role": role}

def check_password_hash(stored_password, provided_password):
    try:
        return pwd_context.verify(provided_password, stored_password)
    except Exception as e:
        print(f"Error verifying password: {e}")
        return False

def rehash_passwords():
    db = UserSessionLocal()
    try:
        users = db.query(User).all()
        for user in users:
            if not pwd_context.identify(user.password_hash):
                print(f"Rehashing password for user {user.username}")
                hashed_password = pwd_context.hash(user.password_hash)
                user.password_hash = hashed_password
                db.commit()
    finally:
        db.close()

# ----------------- Cameras / Serialization -----------------

def get_room_camera(session, room: Room):
    if not room:
        return None
    return (
        session.query(Camera)
        .filter(Camera.room_id == room.id, Camera.status == "active")
        .order_by(Camera.id.desc())
        .first()
    )

def _stream_key_for_event(bed: Optional[Bed], room: Optional[Room]) -> str:
    if bed and _s(bed.bed_name):
        return _room_key(bed.bed_name)
    if room and _s(room.room_name):
        return _room_key(room.room_name)
    return ""

def serialize_event_with_camera(session, event: EventModel):
    bed = event.bed
    room = event.room or (bed.room if bed else None)
    ward = room.ward if room else None
    floor = ward.floor if ward else None

    cam = get_room_camera(session, room)
    stream_key = _stream_key_for_event(bed, room)

    streaming_keys = set(camera_manager.get_all_rooms())
    is_running = bool(stream_key) and (stream_key in streaming_keys)
    is_live = bool(stream_key) and camera_manager.has_frame(stream_key)

    cam_url = f"/camera/{quote(stream_key, safe='')}" if (stream_key and (cam or is_running)) else None

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
        "roomName": _room_key(room.room_name if room else None) or None,
        "roomNumber": room.room_number if room else None,

        "wardName": ward.name if ward else None,
        "floorName": floor.name if floor else None,

        "cameraAvailable": True if (cam and stream_key) else is_running,
        "cameraLive": is_live,
        "cameraUrl": cam_url,
        "cameraConfigured": True if cam else False,

        "intercomIp": getattr(bed, "intercom_ip", None) if bed else None,
    }

def mjpeg_generator(stream_key: str, fps: int = 10):
    stream_key = _room_key(stream_key)
    frame_delay = 1.0 / max(1, int(fps))
    last_placeholder = 0.0

    while True:
        frame = camera_manager.get_frame(stream_key)

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
    "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkSEw8UHRofHh0a"
    "HBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/2wBDAQkJCQwLDBgNDRgyIRwhMjIyMjIy"
    "MjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjL/wAARCAABAAEDASIAAhEB"
    "AxEB/8QAFQABAQAAAAAAAAAAAAAAAAAAAAv/xAAUEAEAAAAAAAAAAAAAAAAAAAAA/8QAFQEBAQAA"
    "AAAAAAAAAAAAAAAAAX/xAAUEQEAAAAAAAAAAAAAAAAAAAAA/9oADAMBAAIRAxEAPwCVAAB//2Q=="
)

# ----------------- Lifespan -----------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_database_tables()
    rehash_passwords()

    session = get_db_session(engine)
    try:
        beds = (
            session.query(Bed)
            .options(joinedload(Bed.camera))
            .filter(Bed.camera_id.isnot(None))
            .all()
        )

        for bed in beds:
            if not _s(bed.bed_name):
                continue

            cam = bed.camera
            if not cam or cam.status != "active" or not _s(cam.rtsp_url):
                continue

            try:
                camera_manager.add_camera(bed.bed_name, cam.rtsp_url)
            except Exception as e:
                print(f"[Camera] Failed to start camera for bed='{bed.bed_name}' (camera_id={bed.camera_id}): {e}")
    finally:
        session.close()

    asyncio.create_task(udp_listener())
    yield
    camera_manager.shutdown()

# ----------------- Create FastAPI app with lifespan -----------------
app = FastAPI(lifespan=lifespan)

app.add_middleware(
    SessionMiddleware,
    secret_key=SECRET_KEY,
    session_cookie="session",
    max_age=86400,
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ----------------- Authentication Routes -----------------

@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/login")

@app.get("/login")
async def get_login(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_user_db)
):
    user = db.query(User).filter(User.username == username).first()

    if user and check_password_hash(user.password_hash, password):
        request.session["username"] = user.username
        request.session["role"] = user.role

        if user.role == "admin":
            return RedirectResponse(url="/dashboard", status_code=303)
        elif user.role == "staff":
            return RedirectResponse(url="/calls", status_code=303)
        elif user.role == "user":
            return RedirectResponse(url="/user_calls", status_code=303)
        else:
            return RedirectResponse(url="/login", status_code=303)
    else:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Invalid credentials"}
        )

# ----------------- Role-Specific Page Routes -----------------

@app.get("/dashboard", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")

    if user["role"] != "admin":
        if user["role"] == "staff":
            return RedirectResponse(url="/calls")
        elif user["role"] == "user":
            return RedirectResponse(url="/user_calls")

    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "user": user, "show_sidebar": True}
    )

@app.get("/calls", response_class=HTMLResponse)
async def admin_calls_page(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")

    if user["role"] not in ["admin", "staff"]:
        if user["role"] == "user":
            return RedirectResponse(url="/user_calls")

    show_sidebar = user["role"] == "admin"
    return templates.TemplateResponse(
        "calls.html",
        {"request": request, "user": user, "show_sidebar": show_sidebar}
    )

@app.get("/user_calls", response_class=HTMLResponse)
async def admin_user_calls_page(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")

    if user["role"] not in ["admin", "user"]:
        if user["role"] == "staff":
            return RedirectResponse(url="/calls")

    show_sidebar = user["role"] == "admin"
    return templates.TemplateResponse(
        "user_calls.html",
        {"request": request, "user": user, "show_sidebar": show_sidebar}
    )

@app.get("/history", response_class=HTMLResponse)
async def history_page(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")

    if user["role"] != "admin":
        if user["role"] == "staff":
            return RedirectResponse(url="/calls")
        elif user["role"] == "user":
            return RedirectResponse(url="/user_calls")

    return templates.TemplateResponse(
        "history.html",
        {"request": request, "user": user, "show_sidebar": True}
    )

@app.get("/config", response_class=HTMLResponse)
async def config_page(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")

    if user["role"] != "admin":
        if user["role"] == "staff":
            return RedirectResponse(url="/calls")
        elif user["role"] == "user":
            return RedirectResponse(url="/user_calls")

    return templates.TemplateResponse(
        "config.html",
        {"request": request, "user": user, "show_sidebar": True}
    )

@app.get("/sidebar.html", response_class=HTMLResponse, include_in_schema=False)
async def sidebar_partial(request: Request):
    user = get_current_user(request)
    return templates.TemplateResponse("sidebar.html", {"request": request, "user": user})

@app.get("/templates/sidebar.html", response_class=HTMLResponse, include_in_schema=False)
async def sidebar_partial_alias(request: Request):
    return await sidebar_partial(request)

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login")

# ----------------- Camera Streams -----------------

@app.get("/camera/{key:path}")
async def camera_by_key(key: str):
    key = _room_key(key)

    session = get_db_session(engine)
    try:
        bed = (
            session.query(Bed)
            .options(joinedload(Bed.camera), joinedload(Bed.room))
            .filter(
                (func.lower(Bed.bed_name) == key.lower()) |
                (func.lower(Bed.bed_number) == key.lower())
            )
            .first()
        )

        if bed:
            cam = bed.camera
            if not cam or cam.status != "active" or not _s(cam.rtsp_url):
                return JSONResponse(
                    {"ok": False, "success": False, "status": "error", "error": f"No active camera for bed '{key}'"},
                    status_code=404
                )

            if key not in set(camera_manager.get_all_rooms()):
                camera_manager.add_camera(key, cam.rtsp_url)

            return StreamingResponse(
                mjpeg_generator(key, fps=10),
                media_type="multipart/x-mixed-replace; boundary=frame",
                headers={"Cache-Control": "no-store", "Pragma": "no-cache", "X-Accel-Buffering": "no"}
            )

        room = (
            session.query(Room)
            .filter(
                (func.lower(Room.room_name) == key.lower()) |
                (func.lower(Room.room_number) == key.lower())
            )
            .first()
        )

        if not room:
            return JSONResponse(
                {"ok": False, "success": False, "status": "error", "error": f"Bed/Room '{key}' not found"},
                status_code=404
            )

        cam = get_room_camera(session, room)
        if not cam or cam.status != "active" or not _s(cam.rtsp_url):
            return JSONResponse(
                {"ok": False, "success": False, "status": "error", "error": f"No active camera for room '{key}'"},
                status_code=404
            )

        if key not in set(camera_manager.get_all_rooms()):
            camera_manager.add_camera(key, cam.rtsp_url)

        return StreamingResponse(
            mjpeg_generator(key, fps=10),
            media_type="multipart/x-mixed-replace; boundary=frame",
            headers={"Cache-Control": "no-store", "Pragma": "no-cache", "X-Accel-Buffering": "no"}
        )
    finally:
        session.close()

# ----------------- APIs -----------------

@app.get("/api/stats")
async def get_stats():
    session = get_db_session(engine)
    try:
        now = now_karachi_naive()
        seven_days_ago = now - timedelta(days=7)
        one_hour_ago = now - timedelta(hours=1)

        total_active = session.query(EventModel).filter(
            EventModel.created_at >= seven_days_ago,
            EventModel.status == "active"
        ).count()

        urgent_alarms = session.query(EventModel).filter(
            EventModel.created_at >= seven_days_ago,
            EventModel.event_type.in_(("Emergency", "Alarm", "Assistance")),
            EventModel.status == "active"
        ).count()

        ongoing_calls = session.query(EventModel).filter(
            EventModel.created_at >= seven_days_ago,
            EventModel.event_type == "Call",
            EventModel.status == "active"
        ).count()

        recently_cleared = session.query(EventModel).filter(
            EventModel.created_at >= one_hour_ago,
            EventModel.status == "cleared"
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
        events = (
            session.query(EventModel)
            .options(
                joinedload(EventModel.bed).joinedload(Bed.room).joinedload(Room.ward).joinedload(Ward.floor),
                joinedload(EventModel.room).joinedload(Room.ward).joinedload(Ward.floor),
            )
            .order_by(EventModel.id.desc())
            .limit(limit)
            .all()
        )
        return [serialize_event_with_camera(session, e) for e in events]
    finally:
        session.close()

@app.get("/api/calls")
async def api_calls(limit: int = 100, only_active: bool = True):
    session = get_db_session(engine)
    try:
        q = (
            session.query(EventModel)
            .options(
                joinedload(EventModel.bed).joinedload(Bed.room).joinedload(Room.ward).joinedload(Ward.floor),
                joinedload(EventModel.room).joinedload(Room.ward).joinedload(Ward.floor),
            )
            .order_by(EventModel.id.desc())
        )

        if only_active:
            q = q.filter(EventModel.status == "active")

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
        start = now_karachi_naive() - timedelta(days=days)

        rows = session.query(
            func.date(EventModel.created_at).label("d"),
            EventModel.event_type,
            func.count(EventModel.id)
        ).filter(
            EventModel.created_at >= start
        ).group_by(
            func.date(EventModel.created_at),
            EventModel.event_type
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

# ----------------- Config GET endpoints -----------------

@app.get("/api/config/floors")
async def get_floors():
    session = get_db_session(engine)
    try:
        floors = session.query(Floor).order_by(Floor.floor_number.asc()).all()
        out = []
        for f in floors:
            wards_count = session.query(func.count(Ward.id)).filter(Ward.floor_id == f.id).scalar() or 0
            # ✅ NEW count
            dept_count = 0
            try:
                dept_count = session.execute(
                    text("SELECT COUNT(*) FROM departments WHERE floor_id=:fid"),
                    {"fid": f.id}
                ).scalar() or 0
            except Exception:
                dept_count = 0

            out.append({
                "id": f.id,
                "name": f.name,
                "floorNumber": f.floor_number,
                "description": f.description,
                "status": f.status,
                "wards": int(wards_count),
                "departments": int(dept_count),
            })
        return {"floors": out}
    finally:
        session.close()

@app.get("/api/config/departments")
async def get_departments():
    session = get_db_session(engine)
    try:
        rows = session.execute(
            text("""
                SELECT d.id, d.name, d.floor_id, d.description, d.status
                FROM departments d
                ORDER BY d.id ASC
            """)
        ).mappings().all()

        out = []
        for r in rows:
            wing_count = 0
            try:
                wing_count = session.execute(
                    text("SELECT COUNT(*) FROM wings WHERE department_id=:did"),
                    {"did": r["id"]}
                ).scalar() or 0
            except Exception:
                wing_count = 0

            out.append({
                "id": r["id"],
                "name": r["name"],
                "floorId": r["floor_id"],
                "description": r["description"],
                "status": r["status"],
                "wings": int(wing_count),
            })

        return {"departments": out}
    finally:
        session.close()

@app.get("/api/config/wings")
async def get_wings():
    session = get_db_session(engine)
    try:
        rows = session.execute(
            text("""
                SELECT w.id, w.name, w.department_id, w.description, w.status
                FROM wings w
                ORDER BY w.id ASC
            """)
        ).mappings().all()

        out = []
        for r in rows:
            ward_count = 0
            try:
                ward_count = session.execute(
                    text("SELECT COUNT(*) FROM wards WHERE wing_id=:wid"),
                    {"wid": r["id"]}
                ).scalar() or 0
            except Exception:
                ward_count = 0

            out.append({
                "id": r["id"],
                "name": r["name"],
                "departmentId": r["department_id"],
                "description": r["description"],
                "status": r["status"],
                "wards": int(ward_count),
            })

        return {"wings": out}
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

            # ✅ Read optional columns if present (from DB), without changing ORM
            dep_id = None
            wing_id = None
            try:
                row = session.execute(
                    text("SELECT department_id, wing_id FROM wards WHERE id=:id"),
                    {"id": w.id}
                ).mappings().first()
                if row:
                    dep_id = row.get("department_id")
                    wing_id = row.get("wing_id")
            except Exception:
                pass

            out.append({
                "id": w.id,
                "name": w.name,
                "floorId": w.floor_id,
                "departmentId": dep_id,
                "wingId": wing_id,
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

# ----------------- Config CRUD endpoints -----------------

class FloorUpsert(BaseModel):
    name: Optional[str] = Field(default=None, alias="name")
    floor_number: Optional[int] = Field(default=None, alias="floor_number")
    description: Optional[str] = None
    status: Optional[str] = None

class DepartmentUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: Optional[str] = Field(default=None, alias="name")
    floor_id: Optional[int] = Field(default=None, alias="floor_id")
    description: Optional[str] = None
    status: Optional[str] = None

class WingUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: Optional[str] = Field(default=None, alias="name")
    department_id: Optional[int] = Field(default=None, alias="department_id")
    description: Optional[str] = None
    status: Optional[str] = None

class WardUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: Optional[str] = Field(default=None, alias="name")
    floor_id: Optional[int] = Field(default=None, alias="floor_id")
    # ✅ new optional mapping fields
    department_id: Optional[int] = Field(default=None, alias="department_id")
    wing_id: Optional[int] = Field(default=None, alias="wing_id")
    ward_type: Optional[str] = Field(default=None, alias="ward_type")
    description: Optional[str] = None
    status: Optional[str] = None

class RoomUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ward_id: Optional[int] = Field(default=None, alias="ward_id")
    room_name: Optional[str] = Field(default=None, alias="room_name")
    room_number: Optional[str] = Field(default=None, alias="room_number")
    system_ip: Optional[str] = Field(default=None, alias="system_ip")
    status: Optional[str] = None

class BedUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    room_id: Optional[int] = Field(default=None, alias="room_id")
    room: Optional[Any] = Field(default=None, alias="room")
    bed_name: Optional[str] = Field(default=None, alias="bed_name")
    bed_number: Optional[str] = Field(default=None, alias="bed_number")
    camera_id: Optional[int] = Field(default=None, alias="camera_id")
    status: Optional[str] = Field(default=None, alias="status")
    ip_address: Optional[str] = Field(default=None, alias="ip_address")

class ColorUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    event_type: Optional[str] = Field(default=None, alias="event_type")
    color: Optional[str] = Field(default=None, alias="color")

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

def _last_insert_id(session) -> int:
    try:
        return int(session.execute(text("SELECT LAST_INSERT_ID()")).scalar() or 0)
    except Exception:
        return 0

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

# ---------- Departments CRUD ----------
@app.post("/api/config/departments")
@app.put("/api/config/departments")
async def create_department(payload: DepartmentUpsert):
    session = get_db_session(engine)
    try:
        data = payload.model_dump(exclude_unset=True)
        name = _s(data.get("name"))
        floor_id = data.get("floor_id")

        if not name:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "name is required"}, status_code=422)
        if floor_id is None:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "floor_id is required"}, status_code=422)

        # validate floor exists
        f = session.query(Floor).filter(Floor.id == int(floor_id)).first()
        if not f:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "floor_id not found"}, status_code=404)

        session.execute(
            text("""
                INSERT INTO departments (name, floor_id, description, status, created_at)
                VALUES (:name, :floor_id, :description, :status, :created_at)
            """),
            {
                "name": name,
                "floor_id": int(floor_id),
                "description": data.get("description"),
                "status": data.get("status") or "active",
                "created_at": now_karachi_naive(),
            }
        )
        session.commit()
        new_id = _last_insert_id(session)

        row = session.execute(
            text("SELECT id, name, floor_id, description, status FROM departments WHERE id=:id"),
            {"id": new_id}
        ).mappings().first()

        return {"ok": True, "success": True, "status": "success", "message": "Department created", "department": {
            "id": row["id"], "name": row["name"], "floorId": row["floor_id"],
            "description": row["description"], "status": row["status"]
        }}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()

@app.put("/api/config/departments/{department_id}")
@app.patch("/api/config/departments/{department_id}")
async def update_department(department_id: int, payload: DepartmentUpsert):
    session = get_db_session(engine)
    try:
        exists = session.execute(
            text("SELECT id FROM departments WHERE id=:id"),
            {"id": department_id}
        ).scalar()

        if not exists:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Department not found"}, status_code=404)

        data = payload.model_dump(exclude_unset=True)

        if "floor_id" in data and data.get("floor_id") is not None:
            f = session.query(Floor).filter(Floor.id == int(data["floor_id"])).first()
            if not f:
                return JSONResponse({"ok": False, "success": False, "status": "error", "error": "floor_id not found"}, status_code=404)

        session.execute(
            text("""
                UPDATE departments
                SET
                  name = COALESCE(:name, name),
                  floor_id = COALESCE(:floor_id, floor_id),
                  description = COALESCE(:description, description),
                  status = COALESCE(:status, status),
                  updated_at = :updated_at
                WHERE id = :id
            """),
            {
                "id": department_id,
                "name": _s(data.get("name")) if "name" in data else None,
                "floor_id": int(data["floor_id"]) if "floor_id" in data and data.get("floor_id") is not None else None,
                "description": data.get("description") if "description" in data else None,
                "status": data.get("status") if "status" in data else None,
                "updated_at": now_karachi_naive(),
            }
        )
        session.commit()

        row = session.execute(
            text("SELECT id, name, floor_id, description, status FROM departments WHERE id=:id"),
            {"id": department_id}
        ).mappings().first()

        return {"ok": True, "success": True, "status": "success", "message": "Department saved", "department": {
            "id": row["id"], "name": row["name"], "floorId": row["floor_id"],
            "description": row["description"], "status": row["status"]
        }}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()

@app.delete("/api/config/departments/{department_id}")
async def delete_department(department_id: int):
    session = get_db_session(engine)
    try:
        exists = session.execute(
            text("SELECT id FROM departments WHERE id=:id"),
            {"id": department_id}
        ).scalar()
        if not exists:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Department not found"}, status_code=404)

        # Optional safety: prevent delete if wings exist
        wing_cnt = session.execute(
            text("SELECT COUNT(*) FROM wings WHERE department_id=:id"),
            {"id": department_id}
        ).scalar() or 0
        if int(wing_cnt) > 0:
            return JSONResponse(
                {"ok": False, "success": False, "status": "error", "error": "Cannot delete department: wings exist"},
                status_code=400
            )

        session.execute(text("DELETE FROM departments WHERE id=:id"), {"id": department_id})
        session.commit()
        return {"ok": True, "success": True, "status": "success", "message": "Department deleted"}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()

# ---------- Wings CRUD ----------
@app.post("/api/config/wings")
@app.put("/api/config/wings")
async def create_wing(payload: WingUpsert):
    session = get_db_session(engine)
    try:
        data = payload.model_dump(exclude_unset=True)
        name = _s(data.get("name"))
        department_id = data.get("department_id")

        if not name:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "name is required"}, status_code=422)
        if department_id is None:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "department_id is required"}, status_code=422)

        dep_exists = session.execute(
            text("SELECT id FROM departments WHERE id=:id"),
            {"id": int(department_id)}
        ).scalar()
        if not dep_exists:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "department_id not found"}, status_code=404)

        session.execute(
            text("""
                INSERT INTO wings (name, department_id, description, status, created_at)
                VALUES (:name, :department_id, :description, :status, :created_at)
            """),
            {
                "name": name,
                "department_id": int(department_id),
                "description": data.get("description"),
                "status": data.get("status") or "active",
                "created_at": now_karachi_naive(),
            }
        )
        session.commit()
        new_id = _last_insert_id(session)

        row = session.execute(
            text("SELECT id, name, department_id, description, status FROM wings WHERE id=:id"),
            {"id": new_id}
        ).mappings().first()

        return {"ok": True, "success": True, "status": "success", "message": "Wing created", "wing": {
            "id": row["id"], "name": row["name"], "departmentId": row["department_id"],
            "description": row["description"], "status": row["status"]
        }}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()

@app.put("/api/config/wings/{wing_id}")
@app.patch("/api/config/wings/{wing_id}")
async def update_wing(wing_id: int, payload: WingUpsert):
    session = get_db_session(engine)
    try:
        exists = session.execute(
            text("SELECT id FROM wings WHERE id=:id"),
            {"id": wing_id}
        ).scalar()
        if not exists:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Wing not found"}, status_code=404)

        data = payload.model_dump(exclude_unset=True)

        if "department_id" in data and data.get("department_id") is not None:
            dep_exists = session.execute(
                text("SELECT id FROM departments WHERE id=:id"),
                {"id": int(data["department_id"])}
            ).scalar()
            if not dep_exists:
                return JSONResponse({"ok": False, "success": False, "status": "error", "error": "department_id not found"}, status_code=404)

        session.execute(
            text("""
                UPDATE wings
                SET
                  name = COALESCE(:name, name),
                  department_id = COALESCE(:department_id, department_id),
                  description = COALESCE(:description, description),
                  status = COALESCE(:status, status),
                  updated_at = :updated_at
                WHERE id = :id
            """),
            {
                "id": wing_id,
                "name": _s(data.get("name")) if "name" in data else None,
                "department_id": int(data["department_id"]) if "department_id" in data and data.get("department_id") is not None else None,
                "description": data.get("description") if "description" in data else None,
                "status": data.get("status") if "status" in data else None,
                "updated_at": now_karachi_naive(),
            }
        )
        session.commit()

        row = session.execute(
            text("SELECT id, name, department_id, description, status FROM wings WHERE id=:id"),
            {"id": wing_id}
        ).mappings().first()

        return {"ok": True, "success": True, "status": "success", "message": "Wing saved", "wing": {
            "id": row["id"], "name": row["name"], "departmentId": row["department_id"],
            "description": row["description"], "status": row["status"]
        }}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()

@app.delete("/api/config/wings/{wing_id}")
async def delete_wing(wing_id: int):
    session = get_db_session(engine)
    try:
        exists = session.execute(
            text("SELECT id FROM wings WHERE id=:id"),
            {"id": wing_id}
        ).scalar()
        if not exists:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Wing not found"}, status_code=404)

        # Optional safety: prevent delete if wards exist
        ward_cnt = session.execute(
            text("SELECT COUNT(*) FROM wards WHERE wing_id=:id"),
            {"id": wing_id}
        ).scalar() or 0
        if int(ward_cnt) > 0:
            return JSONResponse(
                {"ok": False, "success": False, "status": "error", "error": "Cannot delete wing: wards exist"},
                status_code=400
            )

        session.execute(text("DELETE FROM wings WHERE id=:id"), {"id": wing_id})
        session.commit()
        return {"ok": True, "success": True, "status": "success", "message": "Wing deleted"}
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

        # ✅ save optional department_id / wing_id into DB columns (if they exist)
        try:
            session.execute(
                text("UPDATE wards SET department_id=:did, wing_id=:wid WHERE id=:id"),
                {
                    "id": w.id,
                    "did": int(data["department_id"]) if data.get("department_id") is not None else None,
                    "wid": int(data["wing_id"]) if data.get("wing_id") is not None else None,
                }
            )
            session.commit()
        except Exception:
            session.rollback()
            session.commit()

        return {"ok": True, "success": True, "status": "success", "message": "Ward created", "ward": {
            "id": w.id, "name": w.name, "floorId": w.floor_id,
            "departmentId": data.get("department_id"),
            "wingId": data.get("wing_id"),
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

        # ✅ update optional columns
        try:
            if "department_id" in data or "wing_id" in data:
                session.execute(
                    text("""
                        UPDATE wards
                        SET
                          department_id = COALESCE(:did, department_id),
                          wing_id = COALESCE(:wid, wing_id)
                        WHERE id = :id
                    """),
                    {
                        "id": ward_id,
                        "did": int(data["department_id"]) if "department_id" in data and data.get("department_id") is not None else None,
                        "wid": int(data["wing_id"]) if "wing_id" in data and data.get("wing_id") is not None else None,
                    }
                )
                session.commit()
        except Exception:
            session.rollback()
            session.commit()

        # read back optional
        dep_id = None
        wing_id = None
        try:
            row = session.execute(
                text("SELECT department_id, wing_id FROM wards WHERE id=:id"),
                {"id": ward_id}
            ).mappings().first()
            if row:
                dep_id = row.get("department_id")
                wing_id = row.get("wing_id")
        except Exception:
            pass

        return {"ok": True, "success": True, "status": "success", "message": "Ward saved", "ward": {
            "id": w.id, "name": w.name, "floorId": w.floor_id,
            "departmentId": dep_id,
            "wingId": wing_id,
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
            if ip:
                msg = _ensure_intercom_ip_unique(session, ip, ignore_bed_id=bed_id)
                if msg:
                    return JSONResponse({"ok": False, "success": False, "status": "error", "error": msg}, status_code=400)
            b.intercom_ip = ip

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
    print(f"⏰ {now_karachi_naive().strftime('%Y-%m-%d %H:%M:%S')}")

    while True:
        try:
            data, addr = await loop.run_in_executor(None, sock.recvfrom, 2048)
            incoming_ip = addr[0]

            decoded = decode_l7700_packet(data)
            if not decoded:
                continue

            session = get_db_session(engine)
            try:
                bed = resolve_bed_by_ip(session, incoming_ip)
                room = bed.room if bed else None

                if not room:
                    room = session.query(Room).filter(Room.system_ip == incoming_ip).first()

                if room and not bed:
                    bed = resolve_bed_in_room(session, room, decoded)

                    if bed and not _s(getattr(bed, "intercom_ip", None)):
                        msg = _ensure_intercom_ip_unique(session, incoming_ip, ignore_bed_id=bed.id)
                        if not msg:
                            bed.intercom_ip = incoming_ip
                            session.commit()

                system_time = now_karachi_naive()
                event_type = decoded.get("event") or "Unknown"

                room_id = room.id if room else (bed.room_id if bed else None)
                bed_id = bed.id if bed else None

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

                event = EventModel(
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
                session.refresh(event)

                payload = serialize_event_with_camera(session, event)
                message = json.dumps(payload)

                dead = []
                for cid, w in list(active_connections.items()):
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

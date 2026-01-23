# server.py

import os
import socket
import asyncio
import json
import time
import base64
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from urllib.parse import quote
from typing import Optional, Any, Dict

import uvicorn
import bcrypt
from fastapi import (
    FastAPI, WebSocket, WebSocketDisconnect, Request,
    Form, Depends
)
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from sqlalchemy import Column, Integer, String, func, text
from sqlalchemy.orm import Session, sessionmaker, declarative_base, joinedload

from pydantic import BaseModel, ConfigDict, Field

from decoder import decode_l7700_packet
from camera_stream import CameraManager
from config import Config
from models import (
    get_db_engine, get_db_session, init_database,
    Floor, Department, Wing, Ward, Room, Bed, Camera,
    Event as EventModel, ColorScheme, CallSession,
    get_karachi_time
)

# ----------------- Globals -----------------

SECRET_KEY = os.getenv("NCS_SECRET_KEY", "your-secret-key-change-this-in-production")

engine = get_db_engine(
    Config.DATABASE_URL,
    Config.DB_SOCKET if getattr(Config, "DB_USE_SOCKET", False) else None
)

UserSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Separate Base for auth users table (keeps your models.py Base untouched)
UserBase = declarative_base()


class User(UserBase):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    password_hash = Column(String)
    role = Column(String)


UserBase.metadata.create_all(bind=engine)

camera_manager = CameraManager(use_simulation=False)
active_connections: Dict[int, WebSocket] = {}

templates = Jinja2Templates(directory="templates")


# ----------------- Password helpers (bcrypt only) -----------------

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except Exception:
        return False


def is_bcrypt_hash(s: str) -> bool:
    return isinstance(s, str) and (s.startswith("$2a$") or s.startswith("$2b$") or s.startswith("$2y$"))


# ----------------- Helpers -----------------

def _s(v) -> str:
    return (v or "").strip()


def _room_key(name: str) -> str:
    return (name or "").strip()


def now_karachi_naive() -> datetime:
    try:
        return get_karachi_time().replace(tzinfo=None)
    except Exception:
        return datetime.now()


# ----------------- DB init (FIX for your startup error) -----------------

def init_database_tables():
    """
    FIX: your code called init_database_tables(), but it didn't exist.
    Your models.py has init_database(engine), so we wrap it here.
    """
    init_database(engine)


def ensure_default_admin():
    """
    Helps when you can't login because there are no users in DB.
    Creates a default admin only if users table is empty.

    You can change defaults with env:
      NCS_DEFAULT_ADMIN_USER, NCS_DEFAULT_ADMIN_PASS
    """
    default_user = os.getenv("NCS_DEFAULT_ADMIN_USER", "admin")
    default_pass = os.getenv("NCS_DEFAULT_ADMIN_PASS", "admin123")

    db = UserSessionLocal()
    try:
        any_user = db.query(User).first()
        if not any_user:
            admin = User(
                username=default_user,
                password_hash=hash_password(default_pass),
                role="admin"
            )
            db.add(admin)
            db.commit()
            print(f"✓ Created default admin: {default_user} / {default_pass}")
    finally:
        db.close()


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


def check_password_hash(stored_password: str, provided_password: str) -> bool:
    return verify_password(provided_password, stored_password or "")


def rehash_passwords():
    """
    Rehash ONLY if someone stored plaintext by mistake.
    Existing bcrypt hashes remain unchanged.
    """
    db = UserSessionLocal()
    try:
        users = db.query(User).all()
        changed = False
        for user in users:
            if user.password_hash and not is_bcrypt_hash(user.password_hash):
                print(f"Rehashing plaintext password for user {user.username}")
                user.password_hash = hash_password(user.password_hash)
                changed = True
        if changed:
            db.commit()
    finally:
        db.close()


# ----------------- CallSession / Bed resolve helpers -----------------

def resolve_bed_by_ip(session: Session, ip: str) -> Optional[Bed]:
    ip = _s(ip)
    if not ip:
        return None
    return session.query(Bed).options(joinedload(Bed.room)).filter(Bed.intercom_ip == ip).first()


def resolve_bed_in_room(session: Session, room: Room, decoded: dict) -> Optional[Bed]:
    if not room:
        return None

    # Try bed_number first
    bed_number = _s(
        decoded.get("bed_number")
        or decoded.get("bedNumber")
        or decoded.get("bed")
        or decoded.get("bed_no")
        or decoded.get("bedNo")
    )

    if bed_number:
        b = (
            session.query(Bed)
            .options(joinedload(Bed.room))
            .filter(Bed.room_id == room.id, func.lower(Bed.bed_number) == bed_number.lower())
            .first()
        )
        if b:
            return b

    # Then try bed_name
    bed_name = _s(decoded.get("bed_name") or decoded.get("bedName") or decoded.get("label"))
    if bed_name:
        b = (
            session.query(Bed)
            .options(joinedload(Bed.room))
            .filter(Bed.room_id == room.id, func.lower(Bed.bed_name) == bed_name.lower())
            .first()
        )
        if b:
            return b

    return None


def get_or_create_call_session(session: Session, bed_id: int, event_type: str) -> CallSession:
    """
    models.py CallSession fields:
      - bed_id
      - current_event_type
      - started_at
      - ended_at
      - status
      - created_at
    """
    cs = session.query(CallSession).filter(
        CallSession.bed_id == bed_id,
        CallSession.status == "active"
    ).first()

    if cs:
        # keep current_event_type updated
        cs.current_event_type = event_type or cs.current_event_type
        session.commit()
        session.refresh(cs)
        return cs

    cs = CallSession(
        bed_id=bed_id,
        current_event_type=event_type or "Unknown",
        started_at=now_karachi_naive(),
        status="active",
        created_at=now_karachi_naive(),
    )
    session.add(cs)
    session.commit()
    session.refresh(cs)
    return cs


# ----------------- Cameras / Serialization -----------------

def get_room_camera(session: Session, room: Room) -> Optional[Camera]:
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


def serialize_event_with_camera(session: Session, event: EventModel) -> dict:
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
    ensure_default_admin()

    # Start cameras for beds that have camera_id + active camera
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
        if user.role == "staff":
            return RedirectResponse(url="/calls", status_code=303)
        if user.role == "user":
            return RedirectResponse(url="/user_calls", status_code=303)

        return RedirectResponse(url="/login", status_code=303)

    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": "Invalid credentials"}
    )


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login")


# ----------------- Role-Specific Page Routes -----------------

@app.get("/dashboard", response_class=HTMLResponse)
async def admin_dashboard(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login")

    if user["role"] != "admin":
        if user["role"] == "staff":
            return RedirectResponse(url="/calls")
        if user["role"] == "user":
            return RedirectResponse(url="/user_calls")

    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "user": user, "show_sidebar": True}
    )


@app.get("/calls", response_class=HTMLResponse)
async def calls_page(request: Request):
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
async def user_calls_page(request: Request):
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
        if user["role"] == "user":
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
        if user["role"] == "user":
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


# ----------------- Camera Streams -----------------

@app.get("/camera/{key:path}")
async def camera_by_key(key: str):
    key = _room_key(key)

    session = get_db_session(engine)
    try:
        # Try bed by name/number first
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

        # Then try room
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
            dept_count = session.query(func.count(Department.id)).filter(Department.floor_id == f.id).scalar() or 0
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
        depts = session.query(Department).order_by(Department.id.asc()).all()
        out = []
        for d in depts:
            wing_count = session.query(func.count(Wing.id)).filter(Wing.department_id == d.id).scalar() or 0
            out.append({
                "id": d.id,
                "name": d.name,
                "floorId": d.floor_id,
                "description": d.description,
                "status": d.status,
                "wings": int(wing_count),
            })
        return {"departments": out}
    finally:
        session.close()


@app.get("/api/config/wings")
async def get_wings():
    session = get_db_session(engine)
    try:
        wings = session.query(Wing).order_by(Wing.id.asc()).all()
        out = []
        for w in wings:
            ward_count = session.query(func.count(Ward.id)).filter(Ward.wing_id == w.id).scalar() or 0
            out.append({
                "id": w.id,
                "name": w.name,
                "departmentId": w.department_id,
                "description": w.description,
                "status": w.status,
                "wards": int(ward_count),
            })
        return {"wings": out}
    finally:
        session.close()


@app.get("/api/config/wards")
async def get_wards():
    session = get_db_session(engine)
    try:
        wards = session.query(Ward).options(joinedload(Ward.wing)).order_by(Ward.id.asc()).all()
        out = []
        for w in wards:
            rooms_count = session.query(func.count(Room.id)).filter(Room.ward_id == w.id).scalar() or 0
            department_id = w.wing.department_id if w.wing else None
            out.append({
                "id": w.id,
                "name": w.name,
                "floorId": w.floor_id,
                "departmentId": department_id,
                "wingId": w.wing_id,
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
            "wardId": r.ward_id,
            "roomName": r.room_name,
            "roomNumber": r.room_number,
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
            "ipAddress": b.intercom_ip,
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


# ----------------- Config CRUD models -----------------

class FloorUpsert(BaseModel):
    name: Optional[str] = None
    floor_number: Optional[int] = None
    description: Optional[str] = None
    status: Optional[str] = None


class DepartmentUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: Optional[str] = None
    floor_id: Optional[int] = None
    description: Optional[str] = None
    status: Optional[str] = None


class WingUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: Optional[str] = None
    department_id: Optional[int] = None
    description: Optional[str] = None
    status: Optional[str] = None


class WardUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: Optional[str] = None
    floor_id: Optional[int] = None
    wing_id: Optional[int] = None
    ward_type: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None


class RoomUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ward_id: Optional[int] = None
    room_name: Optional[str] = None
    room_number: Optional[str] = None
    system_ip: Optional[str] = None
    status: Optional[str] = None


class BedUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    room_id: Optional[int] = None
    room: Optional[Any] = None
    bed_name: Optional[str] = None
    bed_number: Optional[str] = None
    camera_id: Optional[int] = None
    status: Optional[str] = None
    ip_address: Optional[str] = None


class ColorUpsert(BaseModel):
    model_config = ConfigDict(extra="ignore")
    event_type: Optional[str] = None
    color: Optional[str] = None


def _resolve_room_id_for_bed(session: Session, payload: BedUpsert) -> Optional[int]:
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


def _ensure_intercom_ip_unique(session: Session, ip: str, ignore_bed_id: Optional[int] = None) -> Optional[str]:
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


# ----------------- Config CRUD endpoints -----------------

# Floors
@app.post("/api/config/floors")
@app.put("/api/config/floors")
async def create_floor(payload: FloorUpsert):
    session = get_db_session(engine)
    try:
        if not _s(payload.name):
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "name is required"}, status_code=422)
        if payload.floor_number is None:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "floor_number is required"}, status_code=422)

        f = Floor(
            name=_s(payload.name),
            floor_number=int(payload.floor_number),
            description=payload.description,
            status=payload.status or "active",
            created_at=now_karachi_naive()
        )
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

        if payload.name is not None:
            f.name = _s(payload.name)
        if payload.floor_number is not None:
            f.floor_number = int(payload.floor_number)
        if payload.description is not None:
            f.description = payload.description
        if payload.status is not None:
            f.status = payload.status or f.status

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


# Departments
@app.post("/api/config/departments")
@app.put("/api/config/departments")
async def create_department(payload: DepartmentUpsert):
    session = get_db_session(engine)
    try:
        name = _s(payload.name)
        if not name:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "name is required"}, status_code=422)
        if payload.floor_id is None:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "floor_id is required"}, status_code=422)

        f = session.query(Floor).filter(Floor.id == int(payload.floor_id)).first()
        if not f:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "floor_id not found"}, status_code=404)

        d = Department(
            name=name,
            floor_id=int(payload.floor_id),
            description=payload.description,
            status=payload.status or "active",
            created_at=now_karachi_naive()
        )
        session.add(d)
        session.commit()
        session.refresh(d)

        return {"ok": True, "success": True, "status": "success", "message": "Department created", "department": {
            "id": d.id, "name": d.name, "floorId": d.floor_id,
            "description": d.description, "status": d.status
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
        d = session.query(Department).filter(Department.id == department_id).first()
        if not d:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Department not found"}, status_code=404)

        if payload.floor_id is not None:
            f = session.query(Floor).filter(Floor.id == int(payload.floor_id)).first()
            if not f:
                return JSONResponse({"ok": False, "success": False, "status": "error", "error": "floor_id not found"}, status_code=404)
            d.floor_id = int(payload.floor_id)

        if payload.name is not None:
            d.name = _s(payload.name)
        if payload.description is not None:
            d.description = payload.description
        if payload.status is not None:
            d.status = payload.status or d.status

        session.commit()
        session.refresh(d)

        return {"ok": True, "success": True, "status": "success", "message": "Department saved", "department": {
            "id": d.id, "name": d.name, "floorId": d.floor_id,
            "description": d.description, "status": d.status
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
        d = session.query(Department).filter(Department.id == department_id).first()
        if not d:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Department not found"}, status_code=404)

        wing_cnt = session.query(func.count(Wing.id)).filter(Wing.department_id == department_id).scalar() or 0
        if int(wing_cnt) > 0:
            return JSONResponse(
                {"ok": False, "success": False, "status": "error", "error": "Cannot delete department: wings exist"},
                status_code=400
            )

        session.delete(d)
        session.commit()
        return {"ok": True, "success": True, "status": "success", "message": "Department deleted"}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()


# Wings
@app.post("/api/config/wings")
@app.put("/api/config/wings")
async def create_wing(payload: WingUpsert):
    session = get_db_session(engine)
    try:
        name = _s(payload.name)
        if not name:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "name is required"}, status_code=422)
        if payload.department_id is None:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "department_id is required"}, status_code=422)

        dep = session.query(Department).filter(Department.id == int(payload.department_id)).first()
        if not dep:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "department_id not found"}, status_code=404)

        w = Wing(
            name=name,
            department_id=int(payload.department_id),
            description=payload.description,
            status=payload.status or "active",
            created_at=now_karachi_naive()
        )
        session.add(w)
        session.commit()
        session.refresh(w)

        return {"ok": True, "success": True, "status": "success", "message": "Wing created", "wing": {
            "id": w.id, "name": w.name, "departmentId": w.department_id,
            "description": w.description, "status": w.status
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
        w = session.query(Wing).filter(Wing.id == wing_id).first()
        if not w:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Wing not found"}, status_code=404)

        if payload.department_id is not None:
            dep = session.query(Department).filter(Department.id == int(payload.department_id)).first()
            if not dep:
                return JSONResponse({"ok": False, "success": False, "status": "error", "error": "department_id not found"}, status_code=404)
            w.department_id = int(payload.department_id)

        if payload.name is not None:
            w.name = _s(payload.name)
        if payload.description is not None:
            w.description = payload.description
        if payload.status is not None:
            w.status = payload.status or w.status

        session.commit()
        session.refresh(w)

        return {"ok": True, "success": True, "status": "success", "message": "Wing saved", "wing": {
            "id": w.id, "name": w.name, "departmentId": w.department_id,
            "description": w.description, "status": w.status
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
        w = session.query(Wing).filter(Wing.id == wing_id).first()
        if not w:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "Wing not found"}, status_code=404)

        ward_cnt = session.query(func.count(Ward.id)).filter(Ward.wing_id == wing_id).scalar() or 0
        if int(ward_cnt) > 0:
            return JSONResponse(
                {"ok": False, "success": False, "status": "error", "error": "Cannot delete wing: wards exist"},
                status_code=400
            )

        session.delete(w)
        session.commit()
        return {"ok": True, "success": True, "status": "success", "message": "Wing deleted"}
    except Exception as e:
        session.rollback()
        return JSONResponse({"ok": False, "success": False, "status": "error", "error": str(e)}, status_code=400)
    finally:
        session.close()


# Wards
@app.post("/api/config/wards")
@app.put("/api/config/wards")
async def create_ward(payload: WardUpsert):
    session = get_db_session(engine)
    try:
        if not _s(payload.name):
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "name is required"}, status_code=422)
        if payload.floor_id is None:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "floor_id is required"}, status_code=422)

        floor = session.query(Floor).filter(Floor.id == int(payload.floor_id)).first()
        if not floor:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "floor_id not found"}, status_code=404)

        if payload.wing_id is not None:
            wing = session.query(Wing).filter(Wing.id == int(payload.wing_id)).first()
            if not wing:
                return JSONResponse({"ok": False, "success": False, "status": "error", "error": "wing_id not found"}, status_code=404)

        w = Ward(
            name=_s(payload.name),
            floor_id=int(payload.floor_id),
            wing_id=int(payload.wing_id) if payload.wing_id is not None else None,
            ward_type=payload.ward_type,
            description=payload.description,
            status=payload.status or "active",
            created_at=now_karachi_naive()
        )
        session.add(w)
        session.commit()
        session.refresh(w)

        department_id = None
        if w.wing_id:
            wing = session.query(Wing).filter(Wing.id == w.wing_id).first()
            department_id = wing.department_id if wing else None

        return {"ok": True, "success": True, "status": "success", "message": "Ward created", "ward": {
            "id": w.id, "name": w.name, "floorId": w.floor_id,
            "departmentId": department_id,
            "wingId": w.wing_id,
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

        if payload.name is not None:
            w.name = _s(payload.name)
        if payload.floor_id is not None:
            floor = session.query(Floor).filter(Floor.id == int(payload.floor_id)).first()
            if not floor:
                return JSONResponse({"ok": False, "success": False, "status": "error", "error": "floor_id not found"}, status_code=404)
            w.floor_id = int(payload.floor_id)

        if payload.wing_id is not None:
            if payload.wing_id == 0:
                w.wing_id = None
            else:
                wing = session.query(Wing).filter(Wing.id == int(payload.wing_id)).first()
                if not wing:
                    return JSONResponse({"ok": False, "success": False, "status": "error", "error": "wing_id not found"}, status_code=404)
                w.wing_id = int(payload.wing_id)

        if payload.ward_type is not None:
            w.ward_type = payload.ward_type
        if payload.description is not None:
            w.description = payload.description
        if payload.status is not None:
            w.status = payload.status or w.status

        session.commit()
        session.refresh(w)

        department_id = None
        if w.wing_id:
            wing = session.query(Wing).filter(Wing.id == w.wing_id).first()
            department_id = wing.department_id if wing else None

        return {"ok": True, "success": True, "status": "success", "message": "Ward saved", "ward": {
            "id": w.id, "name": w.name, "floorId": w.floor_id,
            "departmentId": department_id,
            "wingId": w.wing_id,
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


# Rooms
@app.post("/api/config/rooms")
@app.put("/api/config/rooms")
async def create_room(payload: RoomUpsert):
    session = get_db_session(engine)
    try:
        if payload.ward_id is None:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "ward_id is required"}, status_code=422)
        if not _s(payload.room_number):
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "room_number is required"}, status_code=422)

        ward = session.query(Ward).filter(Ward.id == int(payload.ward_id)).first()
        if not ward:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "ward_id not found"}, status_code=404)

        if _s(payload.system_ip):
            exists = session.query(Room).filter(Room.system_ip == _s(payload.system_ip)).first()
            if exists:
                return JSONResponse({"ok": False, "success": False, "status": "error", "error": "system_ip already exists in another room"}, status_code=400)

        r = Room(
            ward_id=int(payload.ward_id),
            room_name=_s(payload.room_name) or None,
            room_number=_s(payload.room_number),
            system_ip=_s(payload.system_ip) or None,
            status=payload.status or "active",
            created_at=now_karachi_naive()
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

        if payload.ward_id is not None:
            ward = session.query(Ward).filter(Ward.id == int(payload.ward_id)).first()
            if not ward:
                return JSONResponse({"ok": False, "success": False, "status": "error", "error": "ward_id not found"}, status_code=404)
            r.ward_id = int(payload.ward_id)

        if payload.room_name is not None:
            r.room_name = _s(payload.room_name) or None
        if payload.room_number is not None:
            r.room_number = _s(payload.room_number)

        if payload.system_ip is not None:
            rip = _s(payload.system_ip)
            if rip:
                exists = session.query(Room).filter(Room.system_ip == rip, Room.id != r.id).first()
                if exists:
                    return JSONResponse({"ok": False, "success": False, "status": "error", "error": "system_ip already exists in another room"}, status_code=400)
                r.system_ip = rip
            else:
                r.system_ip = None

        if payload.status is not None:
            r.status = payload.status or r.status

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


# Beds
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

        ip = _s(payload.ip_address) or None
        if ip:
            msg = _ensure_intercom_ip_unique(session, ip)
            if msg:
                return JSONResponse({"ok": False, "success": False, "status": "error", "error": msg}, status_code=400)

        # camera_id can be null; if provided, ensure camera exists
        if payload.camera_id is not None:
            cam = session.query(Camera).filter(Camera.id == int(payload.camera_id)).first()
            if not cam:
                return JSONResponse({"ok": False, "success": False, "status": "error", "error": "camera_id not found"}, status_code=404)

        b = Bed(
            room_id=int(rid),
            bed_number=_s(payload.bed_number),
            bed_name=_s(payload.bed_name) or None,
            camera_id=int(payload.camera_id) if payload.camera_id is not None else None,
            status=payload.status or "available",
            intercom_ip=ip,
            created_at=now_karachi_naive()
        )
        session.add(b)
        session.commit()
        session.refresh(b)

        room = session.query(Room).filter(Room.id == b.room_id).first()
        return {"ok": True, "success": True, "status": "success", "message": "Bed created", "bed": {
            "id": b.id,
            "roomId": b.room_id,
            "roomName": room.room_name if room else None,
            "bedName": b.bed_name,
            "bedNumber": b.bed_number,
            "cameraId": b.camera_id,
            "status": b.status,
            "ipAddress": b.intercom_ip,
        }}
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
            b.room_id = int(rid)

        if payload.bed_number is not None:
            b.bed_number = _s(payload.bed_number)

        if payload.bed_name is not None:
            b.bed_name = _s(payload.bed_name) or None

        if payload.camera_id is not None:
            if payload.camera_id == 0:
                b.camera_id = None
            else:
                cam = session.query(Camera).filter(Camera.id == int(payload.camera_id)).first()
                if not cam:
                    return JSONResponse({"ok": False, "success": False, "status": "error", "error": "camera_id not found"}, status_code=404)
                b.camera_id = int(payload.camera_id)

        if payload.status is not None:
            b.status = payload.status or b.status

        if payload.ip_address is not None:
            ip = _s(payload.ip_address) or None
            if ip:
                msg = _ensure_intercom_ip_unique(session, ip, ignore_bed_id=bed_id)
                if msg:
                    return JSONResponse({"ok": False, "success": False, "status": "error", "error": msg}, status_code=400)
            b.intercom_ip = ip

        session.commit()
        session.refresh(b)

        room = session.query(Room).filter(Room.id == b.room_id).first()
        return {"ok": True, "success": True, "status": "success", "message": "Bed saved", "bed": {
            "id": b.id,
            "roomId": b.room_id,
            "roomName": room.room_name if room else None,
            "bedName": b.bed_name,
            "bedNumber": b.bed_number,
            "cameraId": b.camera_id,
            "status": b.status,
            "ipAddress": b.intercom_ip,
        }}
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


# Colors
@app.post("/api/config/colors")
@app.put("/api/config/colors")
async def create_color(payload: ColorUpsert):
    session = get_db_session(engine)
    try:
        et = _s(payload.event_type)
        col = _s(payload.color)
        if not et or not col:
            return JSONResponse({"ok": False, "success": False, "status": "error", "error": "eventType and color required"}, status_code=422)

        # upsert behavior: if exists, update
        existing = session.query(ColorScheme).filter(ColorScheme.event_type == et).first()
        if existing:
            existing.color = col
            session.commit()
            session.refresh(existing)
            return {"ok": True, "success": True, "status": "success", "message": "Color saved", "color": {
                "id": existing.id, "eventType": existing.event_type, "color": existing.color
            }}

        c = ColorScheme(event_type=et, color=col, created_at=now_karachi_naive(), updated_at=now_karachi_naive())
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

        if payload.event_type is not None and _s(payload.event_type):
            c.event_type = _s(payload.event_type)
        if payload.color is not None and _s(payload.color):
            c.color = _s(payload.color)

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

                    # Auto-learn intercom_ip for that bed (if empty)
                    if bed and not _s(bed.intercom_ip):
                        msg = _ensure_intercom_ip_unique(session, incoming_ip, ignore_bed_id=bed.id)
                        if not msg:
                            bed.intercom_ip = incoming_ip
                            session.commit()

                system_time = now_karachi_naive()
                event_type = decoded.get("event") or "Unknown"

                room_id = room.id if room else (bed.room_id if bed else None)
                bed_id = bed.id if bed else None

                # End active session on reset
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

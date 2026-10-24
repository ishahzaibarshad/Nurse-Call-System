import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def project_root() -> Path:
    """
    Returns project root (one level above this file) for normal python runs.
    If running as PyInstaller EXE, returns folder of the .exe.
    """
    if getattr(sys, "frozen", False) and hasattr(sys, "executable"):
        return Path(sys.executable).resolve().parent
    # config.py is usually inside project root; if yours is inside a subfolder,
    # change .parent to .parent.parent accordingly.
    return Path(__file__).resolve().parent


class Config:
    # ---------- DB (SQLite Offline) ----------
    # .env overrides:
    # SQLITE_FILENAME=hospital_monitor.db
    # SQLITE_PATH=/absolute/path/to/hospital_monitor.db
    SQLITE_FILENAME = os.getenv("SQLITE_FILENAME", "hospital_monitor.db")
    SQLITE_PATH = os.getenv("SQLITE_PATH", "").strip()

    if SQLITE_PATH:
        DB_PATH = Path(SQLITE_PATH).expanduser().resolve()
    else:
        # Put DB inside a dedicated writable folder (recommended)
        data_dir = (project_root() / "data").resolve()
        data_dir.mkdir(parents=True, exist_ok=True)
        DB_PATH = (data_dir / SQLITE_FILENAME).resolve()

    # SQLAlchemy SQLite URL
    # Use as_posix() for linux paths. Absolute path => sqlite:////abs/path
    DATABASE_URL = f"sqlite:///{DB_PATH.as_posix()}"

    # ---------- Optional debug print (helps find wrong DB path) ----------
    print(f"[Config] Using SQLite DB: {DB_PATH}")

    # Keep legacy MySQL settings for compatibility (not used in SQLite)
    DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
    DB_PORT = int(os.getenv("DB_PORT", "3306"))
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_NAME = os.getenv("DB_NAME", "hospital_monitor")
    DB_SOCKET = os.getenv("DB_SOCKET", "/opt/lampp/var/mysql/mysql.sock")
    DB_USE_SOCKET = False  # ✅ SQLite mode

    # ---------- UDP ----------
    UDP_IP = os.getenv("UDP_IP", "0.0.0.0")
    UDP_PORT = int(os.getenv("UDP_PORT", "6345"))

    # ---------- SERVER ----------
    SERVER_HOST = os.getenv("SERVER_HOST", "127.0.0.1")
    SERVER_PORT = int(os.getenv("SERVER_PORT", "9000"))

    # ---------- TIMEZONE ----------
    TIMEZONE = "Asia/Karachi"

    # ---------- BED -> INTERCOM DEFAULT MAPPING ----------
    DEFAULT_BED_INTERCOM_SETUP = [
        {"room_name": "G1 RES RN2", "bed_name": "G1 RES RN 02", "intercom_ip": "192.168.1.11"},
        {"room_name": "VIP ROOM 1", "bed_name": "VIP ROOM 01", "intercom_ip": "192.168.1.12"},
    ]

    # ---------- Room/system IP -> Intercom IP mapping ----------
    INTERCOM_IP_BY_ROOM_SYSTEM_IP = {
        "192.168.1.11": "192.168.1.11",
        "192.168.1.12": "192.168.1.12",
    }

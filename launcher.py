import os
import sys
import shutil
import time
import threading
import webbrowser
from pathlib import Path

def resource_path(rel_path: str) -> Path:
    """
    When bundled by PyInstaller, files are unpacked into sys._MEIPASS.
    When running normally, use project directory.
    """
    base = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent))
    return base / rel_path

def get_writable_app_dir() -> Path:
    """
    Windows: %APPDATA%\\NurseCall
    Linux/mac: ~/.nursecall
    """
    if os.name == "nt":
        appdata = os.environ.get("APPDATA") or str(Path.home() / "AppData" / "Roaming")
        return Path(appdata) / "NurseCall"
    return Path.home() / ".nursecall"

def ensure_writable_db() -> Path:
    appdir = get_writable_app_dir()
    datadir = appdir / "data"
    datadir.mkdir(parents=True, exist_ok=True)

    src_db = resource_path("data/hospital_monitor.db")
    dst_db = datadir / "hospital_monitor.db"

    # Copy on first run, or if destination missing
    if not dst_db.exists():
        shutil.copy2(src_db, dst_db)

    return dst_db

def open_browser_later(url: str, delay: float = 1.5):
    def _go():
        time.sleep(delay)
        webbrowser.open(url)
    threading.Thread(target=_go, daemon=True).start()

def main():
    # Ensure DB exists in writable location
    db_path = ensure_writable_db()

    # Make server use this DB
    # Your config.py reads DATABASE_URL; we override it here for Windows EXE runtime.
    os.environ["DATABASE_URL"] = f"sqlite:///{db_path.as_posix()}"

    # (Optional) also ensure templates/static are found (your server likely uses relative paths)
    # If your server uses Jinja2 DirectoryLoader, point it here:
    os.environ["TEMPLATES_DIR"] = str(resource_path("templates"))
    os.environ["STATIC_DIR"] = str(resource_path("static"))

    # Start server (imports AFTER env set)
    from server import app
    import uvicorn

    # Auto-open login page
    open_browser_later("http://127.0.0.1:9000/login")

    uvicorn.run(app, host="127.0.0.1", port=9000, log_level="info")

if __name__ == "__main__":
    main()

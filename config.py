import os
from dotenv import load_dotenv
from urllib.parse import quote_plus

load_dotenv()

class Config:
    # ---------- DB ----------
    DB_HOST = os.getenv("DB_HOST", "127.0.0.1")   # use 127.0.0.1 for TCP
    DB_PORT = int(os.getenv("DB_PORT", "3306"))
    DB_USER = os.getenv("DB_USER", "root")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_NAME = os.getenv("DB_NAME", "hospital_monitor")

    # XAMPP socket
    DB_SOCKET = os.getenv("DB_SOCKET", "/opt/lampp/var/mysql/mysql.sock")

    # 1 = use socket (best for XAMPP), 0 = use TCP
    DB_USE_SOCKET = os.getenv("DB_USE_SOCKET", "1") == "1"

    _pwd = quote_plus(DB_PASSWORD or "")
    if DB_USE_SOCKET:
        _sock = quote_plus(DB_SOCKET)
        DATABASE_URL = f"mysql+pymysql://{DB_USER}:{_pwd}@localhost/{DB_NAME}?unix_socket={_sock}&charset=utf8mb4"
    else:
        DATABASE_URL = f"mysql+pymysql://{DB_USER}:{_pwd}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"

    # ---------- UDP ----------
    UDP_IP = os.getenv("UDP_IP", "0.0.0.0")
    UDP_PORT = int(os.getenv("UDP_PORT", "6345"))  # set default to what your server log shows

    # ---------- SERVER ----------
    SERVER_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
    SERVER_PORT = int(os.getenv("SERVER_PORT", "9000"))

    # ---------- TIMEZONE ----------
    TIMEZONE = "Asia/Karachi"
        # ---------- BED -> INTERCOM DEFAULT MAPPING ----------
    # room_name (DB) ke basis par bed_name + intercom_ip auto set ho jayega
    DEFAULT_BED_INTERCOM_SETUP = [
        {
            "room_name": "G1 RES RN2",
            "bed_name": "G1 RES RN 02",
            "intercom_ip": "192.168.1.11",
        },
        {
            "room_name": "VIP ROOM 1",
            "bed_name": "VIP ROOM 01",
            "intercom_ip": "192.168.1.12",
        },
    ]
        # Room/system IP -> Intercom IP mapping
    # (Room name kuch bhi ho, system_ip stable hota hai)
    INTERCOM_IP_BY_ROOM_SYSTEM_IP = {
        "192.168.1.11": "192.168.1.11",
        "192.168.1.12": "192.168.1.12",
    }



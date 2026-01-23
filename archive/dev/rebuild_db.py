# rebuild_db.py
import sys
import sqlite3
import re
from pathlib import Path

from config import Config
from models import get_db_engine, init_database

DUMP_DEFAULT = "hospital_monitor.sql"
OUT_DEFAULT = str(Config.DB_PATH)

INSERT_RE = re.compile(r"^INSERT INTO\s+`?([a-zA-Z0-9_]+)`?\s+VALUES\s+", re.IGNORECASE)


def parse_values_block(s: str):
    s = s.strip()
    if s.endswith(";"):
        s = s[:-1]

    rows = []
    i, n = 0, len(s)

    def skip_ws():
        nonlocal i
        while i < n and s[i].isspace():
            i += 1

    def parse_string():
        nonlocal i
        i += 1  # skip opening quote
        out = []
        while i < n:
            ch = s[i]
            if ch == "\\" and i + 1 < n:
                out.append(s[i + 1])
                i += 2
                continue
            if ch == "'":
                i += 1
                return "".join(out)
            out.append(ch)
            i += 1
        raise ValueError("Unterminated string")

    def parse_token():
        nonlocal i
        skip_ws()
        if i >= n:
            return None
        if s[i] == "'":
            return parse_string()
        if s[i:i + 4].upper() == "NULL":
            i += 4
            return None

        start = i
        while i < n and s[i] not in [",", ")", "("] and not s[i].isspace():
            i += 1
        token = s[start:i].strip()
        if token == "":
            return None
        if re.fullmatch(r"-?\d+", token):
            return int(token)
        if re.fullmatch(r"-?\d+\.\d+", token):
            return float(token)
        return token

    while i < n:
        skip_ws()
        if i >= n:
            break
        if s[i] != "(":
            i += 1
            continue
        i += 1  # '('
        row = []
        while True:
            skip_ws()
            if i < n and s[i] == ")":
                i += 1
                break
            val = parse_token()
            row.append(val)
            skip_ws()
            if i < n and s[i] == ",":
                i += 1
                continue
            if i < n and s[i] == ")":
                i += 1
                break
        rows.append(tuple(row))
        skip_ws()
        if i < n and s[i] == ",":
            i += 1
            continue
    return rows


def _sqlite_url_for_path(db_path: str) -> str:
    p = Path(db_path).expanduser().resolve()
    return f"sqlite:///{p.as_posix()}"


def _read_insert_statements(path: str):
    buf = []
    capturing = False

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue

            if not capturing:
                if line.upper().startswith("INSERT INTO"):
                    capturing = True
                    buf = [line]
                    if line.endswith(";"):
                        yield " ".join(buf)
                        capturing = False
                        buf = []
                continue

            buf.append(line)
            if line.endswith(";"):
                yield " ".join(buf)
                capturing = False
                buf = []

    return


def main():
    dump_path = sys.argv[1] if len(sys.argv) > 1 else DUMP_DEFAULT
    out_db = sys.argv[2] if len(sys.argv) > 2 else OUT_DEFAULT

    out_db_path = Path(out_db).expanduser().resolve()
    out_db_path.parent.mkdir(parents=True, exist_ok=True)

    if out_db_path.exists():
        backup = out_db_path.with_suffix(out_db_path.suffix + ".backup")
        try:
            backup.write_bytes(out_db_path.read_bytes())
            print(f"📦 Backup saved: {backup}")
        except Exception as e:
            print(f"⚠️ Could not create backup: {e}")

    # Create schema in the OUTPUT DB
    out_url = _sqlite_url_for_path(str(out_db_path))
    engine = get_db_engine(out_url)
    init_database(engine)

    # Autocommit mode
    con = sqlite3.connect(str(out_db_path), isolation_level=None)
    cur = con.cursor()

    # Disable FK during import
    cur.execute("PRAGMA foreign_keys=OFF;")
    print("foreign_keys =", cur.execute("PRAGMA foreign_keys;").fetchone()[0])

    # Ensure users table exists (server.py uses its own Base)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE,
            password_hash TEXT,
            role TEXT
        );
        """
    )

    # Clear existing data (NO manual BEGIN/COMMIT here; executescript handles it)
    cur.executescript(
        """
        DELETE FROM events;
        DELETE FROM call_sessions;
        DELETE FROM beds;
        DELETE FROM cameras;
        DELETE FROM rooms;
        DELETE FROM wards;
        DELETE FROM wings;
        DELETE FROM departments;
        DELETE FROM floors;
        DELETE FROM color_schemes;
        DELETE FROM users;
        """
    )

    mappings = {
        "floors": {
            "cols": ["id", "name", "floor_number", "description", "status", "created_at"],
            "pick": lambda r: (r[0], r[1], r[2], r[3], r[4], r[5]),
        },
        "departments": {
            "cols": ["id", "floor_id", "name", "description", "status", "created_at"],
            "pick": lambda r: (r[0], r[1], r[2], r[3], r[4], r[5]),
        },
        "wings": {
            "cols": ["id", "department_id", "name", "description", "status", "created_at"],
            "pick": lambda r: (r[0], r[1], r[2], r[3], r[4], r[5]),
        },
        "wards": {
            "cols": ["id", "floor_id", "wing_id", "name", "ward_type", "description", "status", "created_at"],
            "pick": lambda r: (r[0], r[1], (r[8] if len(r) > 8 else None), r[2], r[3], r[4], r[5], r[6]),
        },
        "rooms": {
            "cols": ["id", "ward_id", "room_number", "room_name", "room_type", "capacity", "status",
                     "created_at", "system_device_id", "system_ip", "system_mac"],
            "pick": lambda r: (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10]),
        },
        "cameras": {
            "cols": ["id", "room_id", "camera_name", "rtsp_url", "ip_address", "username", "password",
                     "port", "status", "created_at"],
            "pick": lambda r: (r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]),
        },
        "beds": {
            "cols": ["id", "room_id", "intercom_ip", "bed_number", "bed_name", "status", "camera_id", "created_at"],
            "pick": lambda r: (r[0], r[1], r[7], r[2], r[3], r[4], r[5], r[6]),
        },
        "call_sessions": {
            "cols": ["id", "bed_id", "current_event_type", "started_at", "ended_at", "status", "created_at"],
            "pick": lambda r: (r[0], r[1], r[2], r[3], r[4], r[5], r[6]),
        },
        "events": {
            "cols": ["id", "room_id", "bed_id", "call_session_id", "device_timestamp", "system_timestamp",
                     "room_identifier", "device_type", "event_type", "status",
                     "acknowledged_at", "cleared_at", "raw_hex", "created_at"],
            "pick": lambda r: (
                r[0],
                (r[13] if len(r) > 13 else None),
                r[1],
                r[2],
                r[3],
                r[4],
                r[5],
                r[6],
                r[7],
                r[8],
                r[9],
                r[10],
                r[11],
                r[12],
            ),
        },
        "color_schemes": {
            "cols": ["id", "event_type", "color", "created_at", "updated_at"],
            "pick": lambda r: (r[0], r[1], r[2], r[3], r[4]),
        },
        "users": {
            "cols": ["id", "username", "password_hash", "role"],
            "pick": lambda r: (r[0], r[1], r[2], r[3]),
        },
    }

    inserted_counts = {k: 0 for k in mappings.keys()}

    # Import in a single explicit transaction (fast)
    cur.execute("BEGIN;")
    try:
        for stmt in _read_insert_statements(dump_path):
            m = INSERT_RE.match(stmt)
            if not m:
                continue

            table = m.group(1)
            if table not in mappings:
                continue

            values_part = re.sub(
                rf"^INSERT INTO\s+`?{re.escape(table)}`?\s+VALUES\s+",
                "",
                stmt,
                flags=re.IGNORECASE,
            )

            rows = parse_values_block(values_part)
            if not rows:
                continue

            cols = mappings[table]["cols"]
            pick = mappings[table]["pick"]
            data = [pick(r) for r in rows]

            placeholders = ",".join(["?"] * len(cols))
            col_sql = ",".join(cols)
            sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"

            cur.executemany(sql, data)
            inserted_counts[table] += len(data)

        cur.execute("COMMIT;")
    except Exception:
        cur.execute("ROLLBACK;")
        con.close()
        raise

    # Enable FK and check
    cur.execute("PRAGMA foreign_keys=ON;")
    print("foreign_keys =", cur.execute("PRAGMA foreign_keys;").fetchone()[0])

    bad = cur.execute("PRAGMA foreign_key_check;").fetchall()
    if bad:
        print("⚠️ foreign_key_check issues (table, rowid, parent, fkid):")
        for row in bad[:100]:
            print(" ", row)
    else:
        print("✅ foreign_key_check: OK")

    con.close()

    print("✅ Rebuild complete. Inserted rows:")
    for t, c in inserted_counts.items():
        if c:
            print(f"  {t}: {c}")

    # quick counts
    con = sqlite3.connect(str(out_db_path), isolation_level=None)
    cur = con.cursor()
    for t in ["floors", "departments", "wings", "wards", "rooms", "beds", "cameras", "call_sessions", "events", "users"]:
        try:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"{t} -> {cur.fetchone()[0]}")
        except Exception as e:
            print(f"{t} -> ERROR: {e}")
    con.close()


if __name__ == "__main__":
    main()

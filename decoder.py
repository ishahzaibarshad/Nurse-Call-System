import re
from typing import Optional, Dict

def _parse_timestamp(ts_bytes: bytes) -> str:
    if len(ts_bytes) < 6:
        return "0000-00-00 00:00:00"
    yy, mm, dd, hh, mi, ss = ts_bytes[0], ts_bytes[1], ts_bytes[2], ts_bytes[3], ts_bytes[4], ts_bytes[5]
    year = 2000 + yy if yy < 100 else yy
    return f"{year:04}-{mm:02}-{dd:02} {hh:02}-{mi:02}-{ss:02}".replace("-", ":" , 2).replace("-", " ", 1)

def _cleanup_ascii_block(s: str) -> str:
    return s.replace("\x00", "").strip()

def _extract_room_number(ascii_payload: str) -> Optional[str]:
    """
    Extract a numeric room identifier from patterns like:
    - ROOM 12
    - RM 02
    - RM02
    Returns digits string (keeps leading zeros like '02').
    """
    m = re.search(r"\b(?:ROOM|RM)\s*[-:#]?\s*(\d{1,4})\b", ascii_payload, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    return None

def _extract_bed(ascii_payload: str) -> str:
    """
    Extract bed label. Supports:
    - 'Bed No 1', 'Bed No. 1', 'Bed#1'
    - 'BED 1', 'Bed-1', 'BED1'
    If no bed found, fallback: try to use ROOM/RM number as Bed No <num>
    """
    # Bed No 1 style
    m = re.search(r"\bBED\s*(No\.?|NO\.?|#)?\s*[-:]?\s*(\d{1,4})\b", ascii_payload, flags=re.IGNORECASE)
    if m:
        num = m.group(2)
        return f"Bed No {num}"

    # BED 12 style with possible extra text
    m2 = re.search(r"\b([A-Za-z0-9\s\-]{1,40}BED\s*[-:]?\s*\d{1,4})\b", ascii_payload, flags=re.IGNORECASE)
    if m2:
        return _cleanup_ascii_block(m2.group(1))

    # ✅ Fallback: if it's a "room call" style payload, map RM/ROOM number to a bed
    rn = _extract_room_number(ascii_payload)
    if rn:
        # keep leading zeros (e.g. 02)
        return f"Bed No {rn}"

    return "UNKNOWN"

def _extract_room(ascii_payload: str) -> str:
    """
    Extract room label. Supports:
    - 'ROOM 12'
    - 'RM 02'
    - fallback to first readable block
    """
    room_match = re.search(r"\b([A-Za-z0-9\s\-]{1,40}(ROOM|RM)\s*\d{0,4})\b", ascii_payload, flags=re.IGNORECASE)
    if room_match:
        return _cleanup_ascii_block(room_match.group(1))

    alt_match = re.search(r"([A-Za-z0-9\-\s]{5,40})", ascii_payload)
    if alt_match:
        return _cleanup_ascii_block(alt_match.group(1))

    return "UNKNOWN"

def decode_l7700_packet(data: bytes) -> Optional[Dict]:
    try:
        if not data or data[0] != 0x02 or data[-1] != 0x03:
            return None

        payload = data[1:-1]
        if len(payload) < 24:
            return None

        ts_block = payload[16:24]
        timestamp = _parse_timestamp(ts_block)

        ascii_payload = payload.decode("latin1", "ignore")

        room = _extract_room(ascii_payload)
        bed = _extract_bed(ascii_payload)  # ✅ now room-call gets Bed No <RM>

        # Device type
        device = "UNKNOWN"
        dev_idx = ascii_payload.find("INTERCALL-IP")
        if dev_idx != -1:
            device = "INTERCALL-IP"
        else:
            dev_alt = re.search(r"([A-Z0-9\-\_]{6,20})", ascii_payload)
            if dev_alt:
                device = _cleanup_ascii_block(dev_alt.group(1))

        # Event
        event = "UNKNOWN"
        if dev_idx != -1:
            start = dev_idx + len("INTERCALL-IP")
            event_block = ascii_payload[start:start + 32]
            event = _cleanup_ascii_block(event_block)
            if event:
                event = event.split()[0]
        else:
            for candidate in (
                "Call", "Accept", "Cancel", "Presence", "Reset", "ROOM SERVICE", "Doctor Present",
                "Present", "CATERING SERVICE", "Alarm", "Assistance", "LowBattery", "Emergency", "Isolate"
            ):
                if candidate.lower() in ascii_payload.lower():
                    event = candidate
                    break

        return {
            "timestamp": timestamp,

            # keep semantic meaning consistent for server.py
            "room": room.strip(),
            "bed": bed.strip(),

            # keep originals too
            "bed_name": bed.strip(),
            "room_name": room.strip(),

            "device": device.strip(),
            "event": event.strip(),
            "raw_hex": data.hex(),
        }
    except Exception:
        return None

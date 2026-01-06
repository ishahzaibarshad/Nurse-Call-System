from sqlalchemy import create_engine, Column, Integer, String, DateTime, ForeignKey, Text
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from datetime import datetime
import pytz

Base = declarative_base()

# Asia/Karachi timezone
KARACHI_TZ = pytz.timezone('Asia/Karachi')

def get_karachi_time():
    return datetime.now(KARACHI_TZ)

class Floor(Base):
    __tablename__ = 'floors'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False)
    floor_number = Column(Integer, nullable=False)
    description = Column(Text)
    status = Column(String(20), default='active')
    created_at = Column(DateTime, default=get_karachi_time)

    wards = relationship("Ward", back_populates="floor", cascade="all, delete-orphan")

class Ward(Base):
    __tablename__ = 'wards'

    id = Column(Integer, primary_key=True, autoincrement=True)
    floor_id = Column(Integer, ForeignKey('floors.id', ondelete='CASCADE'), nullable=False)
    name = Column(String(100), nullable=False)
    ward_type = Column(String(50))
    description = Column(Text)
    status = Column(String(20), default='active')
    created_at = Column(DateTime, default=get_karachi_time)

    floor = relationship("Floor", back_populates="wards")
    rooms = relationship("Room", back_populates="ward", cascade="all, delete-orphan")

class Room(Base):
    __tablename__ = 'rooms'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ward_id = Column(Integer, ForeignKey('wards.id', ondelete='CASCADE'), nullable=False)

    room_number = Column(String(50), nullable=False)
    room_name = Column(String(100))
    room_type = Column(String(50))
    capacity = Column(Integer, default=1)
    status = Column(String(20), default='active')
    created_at = Column(DateTime, default=get_karachi_time)

    # system/intercom identity belongs to ROOM
    system_device_id = Column(String(100), unique=True, nullable=True)
    system_ip = Column(String(50), unique=True, index=True, nullable=True)
    system_mac = Column(String(100), unique=True, nullable=True)

    ward = relationship("Ward", back_populates="rooms")
    beds = relationship("Bed", back_populates="room", cascade="all, delete-orphan")
    cameras = relationship("Camera", back_populates="room", cascade="all, delete-orphan")
    events = relationship("Event", back_populates="room")

class Bed(Base):
    __tablename__ = 'beds'

    id = Column(Integer, primary_key=True, autoincrement=True)
    room_id = Column(Integer, ForeignKey('rooms.id', ondelete='SET NULL'), nullable=True)
    intercom_ip = Column(String(50), nullable=True)
    bed_number = Column(String(50), nullable=False)
    bed_name = Column(String(100))
    status = Column(String(20), default='available')
    camera_id = Column(Integer, ForeignKey('cameras.id', ondelete='SET NULL'), nullable=True)
    created_at = Column(DateTime, default=get_karachi_time)

    # ✅ ONLY this (user wants only IP)
    intercom_ip = Column(String(50), nullable=True)

    room = relationship("Room", back_populates="beds")
    camera = relationship("Camera", foreign_keys=[camera_id])
    events = relationship("Event", back_populates="bed", cascade="all, delete-orphan")


class Camera(Base):
    __tablename__ = 'cameras'

    id = Column(Integer, primary_key=True, autoincrement=True)
    room_id = Column(Integer, ForeignKey('rooms.id', ondelete='CASCADE'), nullable=False)
    camera_name = Column(String(100), nullable=False)
    rtsp_url = Column(String(500), nullable=False)
    ip_address = Column(String(50))
    username = Column(String(100))
    password = Column(String(100))
    port = Column(Integer, default=554)
    status = Column(String(20), default='active')
    created_at = Column(DateTime, default=get_karachi_time)

    room = relationship("Room", back_populates="cameras")

class CallSession(Base):
    __tablename__ = 'call_sessions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    bed_id = Column(Integer, ForeignKey('beds.id', ondelete='CASCADE'), nullable=False)
    current_event_type = Column(String(50), nullable=False)
    started_at = Column(DateTime, default=get_karachi_time, nullable=False)
    ended_at = Column(DateTime, nullable=True)
    status = Column(String(20), default='active')
    created_at = Column(DateTime, default=get_karachi_time)

    bed = relationship("Bed", foreign_keys=[bed_id])

class Event(Base):
    __tablename__ = 'events'

    id = Column(Integer, primary_key=True, autoincrement=True)

    room_id = Column(Integer, ForeignKey('rooms.id', ondelete='SET NULL'), nullable=True)

    bed_id = Column(Integer, ForeignKey('beds.id', ondelete='CASCADE'), nullable=True)
    call_session_id = Column(Integer, ForeignKey('call_sessions.id', ondelete='SET NULL'), nullable=True)

    device_timestamp = Column(String(50))
    system_timestamp = Column(DateTime, default=get_karachi_time, nullable=False)
    room_identifier = Column(String(200))
    device_type = Column(String(100))
    event_type = Column(String(50), nullable=False)
    status = Column(String(20), default='active')
    acknowledged_at = Column(DateTime)
    cleared_at = Column(DateTime)
    raw_hex = Column(Text)
    created_at = Column(DateTime, default=get_karachi_time)

    room = relationship("Room", back_populates="events")
    bed = relationship("Bed", back_populates="events")
    call_session = relationship("CallSession", foreign_keys=[call_session_id])

class ColorScheme(Base):
    __tablename__ = 'color_schemes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_type = Column(String(50), unique=True, nullable=False)
    color = Column(String(7), nullable=False)
    created_at = Column(DateTime, default=get_karachi_time)
    updated_at = Column(DateTime, default=get_karachi_time, onupdate=get_karachi_time)

    def to_dict(self):
        return {
            'id': self.id,
            'event_type': self.event_type,
            'color': self.color,
            'created_at': str(self.created_at)
        }

def get_db_engine(db_url: str, socket_path: str | None = None):
    """
    If socket_path is provided -> connect using unix_socket (XAMPP).
    If socket_path is None -> normal TCP connection (host/port) from db_url.
    """
    connect_args = {}
    if socket_path:
        connect_args["unix_socket"] = socket_path

    return create_engine(
        db_url,
        echo=False,
        pool_pre_ping=True,
        connect_args=connect_args if connect_args else None,
    )

def get_db_session(engine):
    Session = sessionmaker(bind=engine)
    return Session()

def init_database(engine):
    Base.metadata.create_all(engine)

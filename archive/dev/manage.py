#!/usr/bin/env python
import sys
from models import get_db_engine, init_database, Base

DB_URL = "mysql+pymysql://root:@localhost/hospital_monitor"

def create_tables():
    engine = get_db_engine(DB_URL)
    init_database(engine)
    print("✅ Tables created (if not already present)")

def drop_tables():
    engine = get_db_engine(DB_URL)
    Base.metadata.drop_all(engine)
    print("⚠️ All tables dropped")

def recreate_tables():
    engine = get_db_engine(DB_URL)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    print("♻️ Database recreated")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python manage.py create")
        print("  python manage.py drop")
        print("  python manage.py recreate")
        sys.exit(1)

    command = sys.argv[1]

    if command == "create":
        create_tables()
    elif command == "drop":
        drop_tables()
    elif command == "recreate":
        recreate_tables()
    else:
        print(f"Unknown command: {command}")

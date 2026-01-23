from alembic import op
import sqlalchemy as sa

revision = "264110f95dd8"
down_revision = None
branch_labels = None
depends_on = None

def _column_exists(conn, table, column):
    res = conn.execute(sa.text("""
        SELECT COUNT(*) AS c
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = :t
          AND COLUMN_NAME = :c
    """), {"t": table, "c": column}).scalar()
    return int(res or 0) > 0

def upgrade():
    conn = op.get_bind()

    if not _column_exists(conn, "events", "room_id"):
        op.add_column("events", sa.Column("room_id", sa.Integer(), nullable=True))
        op.create_index("ix_events_room_id", "events", ["room_id"])
        op.create_foreign_key(
            "fk_events_room_id_rooms",
            "events", "rooms",
            ["room_id"], ["id"],
            ondelete="SET NULL"
        )

def downgrade():
    conn = op.get_bind()
    if _column_exists(conn, "events", "room_id"):
        op.drop_constraint("fk_events_room_id_rooms", "events", type_="foreignkey")
        op.drop_index("ix_events_room_id", table_name="events")
        op.drop_column("events", "room_id")

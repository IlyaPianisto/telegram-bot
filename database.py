import os
import datetime
import sqlite3
from dotenv import load_dotenv

load_dotenv()

DB_FILE = os.getenv('DB_FILE')

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")

    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS users (
                   chat_id TEXT PRIMARY KEY,
                   wind_zero REAL NOT NULL DEFAULT '0.0',
                   light_ref REAL NOT NULL DEFAULT '0.0',
                   temp_min REAL NOT NULL DEFAULT '0.0',
                   wind_max REAL NOT NULL DEFAULT '0.0',
                   humidity_max REAL NOT NULL DEFAULT '0.0',
                   light_min REAL NOT NULL DEFAULT '0.0',
                   created_at TEXT NOT NULL
                   )
                   """)

    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS systems (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        chat_id TEXT NOT NULL REFERENCES users (chat_id),
                        sys_id INTEGER NOT NULL,
                        name TEXT NOT NULL DEFAULT 'СИСТЕМА',
                        created_at TEXT NOT NULL,
                        UNIQUE (chat_id, sys_id)
                        )
                        """)



    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS tree_types (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   name TEXT NOT NULL UNIQUE,
                   created_at TEXT NOT NULL
                   )
                   """)
    cursor.execute("""
                   INSERT OR IGNORE INTO tree_types (name, created_at) VALUES
                   ('Яблоня', ?),
                   ('Груша', ?),
                   ('Вишня', ?), 
                   ('Слива', ?)
                   """, (
                   datetime.datetime.now().isoformat(),
                   datetime.datetime.now().isoformat(),
                   datetime.datetime.now().isoformat(),
                   datetime.datetime.now().isoformat()
    ))

    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS pump_assignments (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   system_id TEXT NOT NULL REFERENCES systems (systems_id),
                   pump_number INTEGER NOT NULL CHECK(pump_number BETWEEN 1 AND 8),
                   tree_type_id INTEGER NOT NULL REFERENCES tree_types (id),
                   assigned_at TEXT NOT NULL,
                   UNIQUE (system_id, pump_number)
                   )
                   """)

    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS sensor_cache (
                   chat_id TEXT PRIMARY KEY REFERENCES users (chat_id),
                   wind REAL,
                   light REAL,
                   temp REAL,
                   humidity REAL,
                   updated_at TEXT NOT NULL
                   )
                   """)

    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS scheduled_tasks (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   chat_id TEXT NOT NULL REFERENCES users (chat_id),
                    system_id INTEGER NOT NULL REFERENCES systems (systems_id),
                   pump_assignment_id INTEGER NOT NULL REFERENCES pump_assignments (id), 
                   month_name TEXT NOT NULL,
                   stage_name TEXT NOT NULL,
                   scheduled_time TEXT NOT NULL DEFAULT '22:00',
                   status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN( 'pending', 'checking', 'running', 'done', 'skipped', 'cancelled' )),
                   created_at TEXT NOT NULL
                   )
                    """)
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS treatment_log (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   chat_id TEXT NOT NULL REFERENCES users (chat_id),
                    system_id INTEGER NOT NULL REFERENCES systems (systems_id),
                   pump_assignment_id INTEGER NOT NULL REFERENCES pump_assignments (id), 
                   month_name TEXT NOT NULL,
                   stage_name TEXT NOT NULL,
                   result TEXT NOT NULL CHECK(result IN ( 'success', 'skipped', 'failed' )),
                   sensor_snapshot TEXT,
                   completed_at TEXT NOT NULL
                   )
                """)
    conn.commit()
    conn.close()

def get_or_create_user(chat_id: srt) -> dict:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM users WHERE chat_id = ?", (chat_id,))
    row = cursor.fetchone()

    if row is None:
        now = datetime.datetime.now().isoformat()
        cursor.execute("""
                        INSERT INTO users (chat_id, created_at) VALUES (?, ?) """ , (chat_id, now))

        conn.commit()

        cursor.execute("SELECT * FROM users WHERE chat_id = ?", (chat_id,))
        row = cursor.fetchone()

    conn.close()
    return dict(row)
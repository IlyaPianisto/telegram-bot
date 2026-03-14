import os
import datetime
import sqlite3
from dotenv import load_dotenv
from mmsystem import SELECTDIB

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

def get_or_create_user(chat_id: str) -> dict:
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

#обновление значений всех
def update_user_settings(chat_id: str, field: str, value:str) -> None:
    ALLOWED_FIELDS = {
        'wind_zero',
        'light_ref',
        'temp_min',
        'wind_max',
        'light_min',
        'humidity_max'
    }
    if field not in ALLOWED_FIELDS:
        print(f"ERROR: НЕДОПУСТИМОЕ ПОЛЕ: {field}")
        return

    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    cursor.execute(f"""
                    UPDATE users SET {field} = ? WHERE chat_id = ?""", (value, chat_id))

    conn.commit()
    conn.close()


def get_tree_types() -> list:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        "SELECT * FROM tree_types ORDER BY name"
    )
    rows = cursor.fetchall()

    conn.close()
    return [dict(row) for row in rows]


def add_tree_type(name: str) -> dict | None:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        now = datetime.datetime.now().isoformat()
        cursor.execute("""
                       INSERT INTO tree_types (name, created_at) VALUES (?, ?) """ , (name, now))
        conn.commit()

        new_id = cursor.lastrowid
        cursor.execute("SELECT * FROM tree_types WHERE id = ?", (new_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row)

    except sqlite3.IntegrityError:
        conn.close()
        print(f"ERROR: дерево {name} уже есть, уже существует!")
        return None

def add_system(chat_id: str, sys_id: str, name: str = "Система") -> dict | None:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        now = datetime.datetime.now().isoformat()
        cursor.execute("""
                       INSERT INTO systems (chat_id, sys_id, name, created_at) VALUES (?, ?, ?, ?) """ , (chat_id, sys_id, name, now))
        conn.commit()

        new_id = cursor.lastrowid
        cursor.execute("SELECT * FROM systems WHERE id = ?", (new_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row)

    except sqlite3.IntegrityError:
        conn.close()
        print(f'ERROR: система {sys_id} уже добавлена для этого пользователя!')
        return None

def get_user_systems(chat_id: str) -> list:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """SELECT * FROM systems WHERE chat_id = ? ORDER BY created_at ASC""", (chat_id,))

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]

def get_system(system_id: int ) -> dict | None:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.rom_row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """SELECT * FROM systems WHERE id = ?
        """ , (system_id,))
    row = cursor.fetchone()
    conn.close()

    if row is None:
        return None
    return dict(row)

def delete_system(system_id: int, chat_id: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    cursor.execute(
        """DELETE FROM systems WHERE id = ? and chat_id = ?""", (system_id, chat_id)
    )

    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

def assign_pump (system_id: int, pump_number: int, tree_type_id: int) -> dict | None:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        now = datetime.datetime.now().isoformat()
        cursor.execute("""
        INSERT INTO pump_assignments
        (system_id, pump_number, tree_type_id, assigned_at) VALUES (?, ?, ?, ?)
        ON CONFLICT (system_id, pump_number) DO UPDATE
        SET tree_type_id = excluded.tree_type_id,
        assigned_at = excluded.assigned_at
                       """, (system_id, pump_number, tree_type_id, now))
        conn.commit()

        cursor.execute("""
        SELECT
        pa.id,
        pa.system_id, 
        pa.pump_number,
        pa.assigned_at,
        tt.name AS tree_name
        FROM pump_assignments pa
        JOIN tree_types tt ON tt.id = pa.tree_type_id
        WHERE pa.tree_type_id = ? AND pa.pump_number = ?
        """, (system_id, pump_number))

        row = cursor.fetchone()
        conn.close()
        return dict(row)

    except sqlite3.IntegrityError as e:
        conn.close()
        print(f'Ошибка привязки насоса: {e}')
        return None

def get_system_pumps(system_id: int) -> list:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
                    SELECT
                    pa.id,
                    pa.pump_number,
                    pa.assigned_at,
                    tt.id AS tree_type_id,
                    tt.name AS tree_name
                FROM pump_assignments pa
                JOIN tree_types tt ON tt.id = pa.tree_type_id
                WHERE pa.system_id = ?
                ORDER BY pa.pump_number ASC""", (system_id,)
    )

    rows = cursor.fetchall()
    conn.close()
    return [dict(row)  for row in rows]

def get_pump_assignment(system_id: int, pump_number: int) -> dict | None:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
                    SELECT
                    pa.id,
                    pa.pump_number,
                    pa.assigned_at,
                    tt.id AS tree_type_id,
                    tt.name AS tree_name
                FROM pump_assignments pa
                JOIN tree_types tt ON tt.id = pa.tree_type_id
                WHERE pa.tree_type_id = ? AND pa.pump_number = ?""", (system_id, pump_number))

    row = cursor.fetchall()
    conn.close()
    if row is None:
        return None
    return dict(row)

def remove_pump_assignment(system_id: int, pump_number: int) -> bool:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    cursor.execute("""
                   DELETE FROM pump_assignments WHERE system_id = ? and pump_number = ?""", (system_id, pump_number))

    deleted = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return deleted

def update_sensor_cash(chat_id: str, field: str, value: float) -> None:
    ALLOWED_FIELDS = {"wind", "light", "temp", "humidity"}
    if field not in ALLOWED_FIELDS:
        print(f"ERROR: недопустимое поле датчика! {field}")
        return
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    now = datetime.datetime.now().isoformat()

    cursor.execute("""
    INSERT INTO sensor_cash(chat_id, {field}, updated_at) VALUES (?, ?, ?)
    ON CONFLICT (chat_id) DO UPDATE
    SET {field} = excluded.{field}, 
    updated_at = excluded.updated_at
    """, (chat_id, value, now))

    conn.commit()
    conn.close()


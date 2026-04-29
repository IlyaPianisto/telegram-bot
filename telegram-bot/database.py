import os
import datetime
import sqlite3
from dotenv import load_dotenv

load_dotenv()

DB_FILE = os.getenv('DB_FILE')

DEFAULTS = {
    "light_day": 0.0,
    "light_night": 0.0,
    "wind_max": 0.0,
    "humidity_max": 0.0,
    'temp_min': 1.0,
    "bottle_volume_l": 5.0,
    "pump_flow_rate": 10.0,
}

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON")

    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS users (
                    chat_id TEXT PRIMARY KEY,
                    light_day REAL NOT NULL DEFAULT '0.0',
                    light_night REAL NOT NULL DEFAULT '0.0',
                    wind_max REAL NOT NULL DEFAULT '0.0',
                    humidity_max REAL NOT NULL DEFAULT '0.0',
                    bottle_volume_l REAL NOT NULL DEFAULT '5.0',
                    temp_min REAL NOT NULL DEFAULT '1.0',
                    pump_flow_rate REAL NOT NULL DEFAULT '0.0',
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
                   system_id INTEGER NOT NULL REFERENCES systems (id),
                   pump_number INTEGER NOT NULL CHECK(pump_number BETWEEN 1 AND 8),
                   tree_type_id INTEGER NOT NULL REFERENCES tree_types (id),
                   assigned_at TEXT NOT NULL,
                   UNIQUE (system_id, pump_number)
                   )
                   """)

    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS sensor_cash (
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
                    system_id INTEGER NOT NULL REFERENCES systems (id),
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
                    system_id INTEGER NOT NULL REFERENCES systems (id),
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
def update_user_settings(chat_id: str, field: str, value) -> None:
    ALLOWED_FIELDS = {
        'light_day', 'light_night', 'temp_min',
        'wind_max', 'humidity_max', 'bottle_volume_l', 'pump_flow_rate',
    }
    if field not in ALLOWED_FIELDS:
        print(f"ERROR: НЕДОПУСТИМОЕ ПОЛЕ: {field}")
        return

    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()
    cursor.execute(f"UPDATE users SET {field} = ? WHERE chat_id = ?", (float(value), chat_id))
    conn.commit()
    conn.close()

def reset_user_settings_to_default(chat_id: str) -> None:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    cursor.execute("""UPDATE users SET light_day = ?, light_night = ?, wind_max = ?, humidity_max = ?, pump_flow_rate = ?, bottle_volume_l = ?, temp_min = ? WHERE chat_id = ?""",
                   (DEFAULTS['light_day'], DEFAULTS['light_night'], DEFAULTS['wind_max'], DEFAULTS['humidity_max'], DEFAULTS['pump_flow_rate'], DEFAULTS['bottle_volume_l'], DEFAULTS['temp_min'], chat_id))

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

def delete_tree_type(tree_type_name: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM tree_types WHERE name = ?", (tree_type_name,))
        conn.commit()
        conn.close()
        return True

    except Exception as e:
        conn.close()
        print(e)
        return False

def add_system(chat_id: str, sys_id: int, name: str = "Система") -> dict | None:
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
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """SELECT * FROM systems WHERE id = ?
        """ , (system_id,))
    row = cursor.fetchone()
    conn.close()

    if row is None:
        return None
    return dict(row)

def rename_system(system_id: int, chat_id: str, new_name: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    cursor.execute(
        """UPDATE systems SET name = ? WHERE id = ? AND chat_id = ?""", (new_name, system_id, chat_id))
    updated = cursor.rowcount > 0
    conn.commit()
    conn.close()
    return updated


def delete_system(system_id: int, chat_id: str) -> bool:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    try:
        cursor.execute("""
        DELETE FROM treatment_log WHERE pump_assignment_id IN ( SELECT id FROM pump_assignments WHERE system_id = ?)""", (system_id,))

        cursor.execute("DELETE FROM scheduled_tasks WHERE pump_assignment_id IN ( SELECT id FROM pump_assignments WHERE system_id = ?)", (system_id,))

        cursor.execute("DELETE FROM pump_assignments WHERE system_id = ?", (system_id,))

        cursor.execute(
            """DELETE FROM systems WHERE id = ? and chat_id = ?""", (system_id, chat_id)
        )
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return deleted
    except Exception as e:
        conn.close()
        print(f"Ошибка удаления системы: {e}")
        return False

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
        tt.id AS tree_type_id,
        tt.name AS tree_name
        FROM pump_assignments pa
        JOIN tree_types tt ON tt.id = pa.tree_type_id
        WHERE pa.system_id = ? AND pa.pump_number = ?
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
                WHERE pa.system_id = ? AND pa.pump_number = ?""", (system_id, pump_number))

    row = cursor.fetchone()
    conn.close()
    if row is None:
        return None
    return dict(row)

def remove_pump_assignment(system_id: int, pump_number: int) -> bool:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    try:
        cursor.execute("""
        UPDATE scheduled_tasks SET status = 'cancelled' 
        WHERE pump_assignment_id IN (
        SELECT id FROM pump_assignments WHERE system_id = ? AND pump_number = ?)
        AND status = 'pending'""", (system_id, pump_number))

        cursor.execute("""
        DELETE FROM treatment_log WHERE pump_assignment_id IN (
        SELECT id FROM pump_assignments WHERE system_id = ? AND pump_number = ?)""", (system_id, pump_number))

        cursor.execute("""
        DELETE FROM scheduled_tasks WHERE pump_assignment_id IN (
        SELECT id FROM pump_assignments WHERE system_id = ? AND pump_number = ?)
        """, (system_id, pump_number))

        cursor.execute("""
                       DELETE FROM pump_assignments WHERE system_id = ? and pump_number = ?""", (system_id, pump_number))

        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return deleted

    except Exception as e:
        conn.close()
        print(f"Ошибка удаления насоса! {e}")
        return False

def update_sensor_cash(chat_id: str, field: str, value: float) -> None:
    ALLOWED_FIELDS = {"wind", "light", "temp", "humidity"}
    if field not in ALLOWED_FIELDS:
        print(f"ERROR: недопустимое поле датчика! {field}")
        return
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    now = datetime.datetime.now().isoformat()

    cursor.execute(f"""
    INSERT INTO sensor_cash(chat_id, {field}, updated_at) VALUES (?, ?, ?)
    ON CONFLICT (chat_id) DO UPDATE
    SET {field} = excluded.{field}, 
    updated_at = excluded.updated_at
    """, (chat_id, value, now))

    conn.commit()
    conn.close()

def get_sensor_cash(chat_id: str) -> dict:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM sensor_cash WHERE chat_id = ?", (chat_id,))
    row = cursor.fetchone()
    conn.close()

    if row is None:
        return {}
    result = dict(row)

    try:
        updated = datetime.datetime.now().fromisoformat(result['updated_at'])
        age_seconds = (datetime.datetime.now() - updated).total_seconds()
        result['age_minutes'] = round(age_seconds / 60, 1)
    except Exception:
        result['age_minutes'] = None

    return result

def clear_sensor_cash(chat_id: str) -> None:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    cursor.execute("DELETE FROM sensor_cash WHERE chat_id = ?", (chat_id,))
    conn.commit()
    conn.close()

def add_schedule_task(chat_id: str, system_id:int, pump_assignment_id: int, month_name: str, stage_name: str, scheduled_time = '22:00') -> dict | None:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        now = datetime.datetime.now().isoformat()
        cursor.execute("""
                        INSERT INTO scheduled_tasks (chat_id, system_id, pump_assignment_id, month_name, stage_name, scheduled_time, created_at)  VALUES (?, ?, ?, ?, ?, ?, ?)""", (chat_id, system_id, pump_assignment_id, month_name, stage_name, scheduled_time, now))
        conn.commit()

        new_id = cursor.lastrowid
        cursor.execute("SELECT * FROM scheduled_tasks WHERE id = ?", (new_id,))
        row = cursor.fetchone()
        conn.close()
        return dict(row)

    except sqlite3.IntegrityError as e:
        conn.close()
        print(f'Error! Ошибка создания задачи: {e}')
        return None

def get_pending_tasks() -> list:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    current_time = datetime.datetime.now().strftime("%H:%M")

    cursor.execute("""
                    SELECT
                    st.*,
                    s.sys_id,
                    s.name AS system_name,
                    pa.pump_number,
                    tt.name AS tree_name
                    FROM scheduled_tasks st
                    JOIN systems s ON s.id = st.system_id
                    JOIN pump_assignments pa ON pa.id = st.pump_assignment_id
                    JOIN tree_types tt ON tt.id = pa.tree_type_id
                    WHERE st.status = 'pending' AND st.scheduled_time <= ? ORDER BY st.created_at ASC """, (current_time,))

    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def update_task_status(task_id: int, new_status: str) -> None:
    ALLOWED_STATUSES = {'pending', 'checking', 'running', 'done', 'skipped', 'cancelled'}
    if new_status not in ALLOWED_STATUSES:
        print(f"ERROR! Недопустимый статус!! '{new_status}'")
        return

    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    cursor.execute("""
                    UPDATE scheduled_tasks SET status = ? WHERE id = ?""", (new_status, task_id))

    conn.commit()
    conn.close()

def cancel_pending_tasks(chat_id: str, system_id: int, stage_name: str) -> int:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    cursor.execute("""
                   UPDATE scheduled_tasks SET status = 'cancelled' WHERE chat_id = ? and system_id = ? and stage_name = ? and status = 'pending'""", (chat_id, system_id, stage_name))

    cancelled_count = cursor.rowcount
    conn.commit()
    conn.close()
    return cancelled_count

def get_user_tasks (chat_id: str, status: str = None) -> list:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    if status:
        cursor.execute("""
                       SELECT 
                       st.*,
                       s.sys_id,
                       s.name AS system_name,
                       pa.pump_number,
                       tt.name AS tree_name
                       FROM scheduled_tasks st
                       JOIN systems s ON s.id = st.system_id
                       JOIN pump_assignments pa ON pa.id = st.pump_assignment_id
                       JOIN tree_types tt ON tt.id = pa.tree_type_id
                       WHERE st.chat_id = ?
                       AND st.status = ?
                       ORDER BY st.created_at DESC """, (chat_id, status,))
    else:
        cursor.execute("""
                       SELECT
                       st.*,
                       s.sys_id,
                       s.name AS system_name,
                       pa.pump_number,
                       tt.name AS tree_name
                       FROM scheduled_tasks st
                       JOIN systems s ON s.id = st.system_id
                       JOIN pump_assignments pa ON pa.id = st.pump_assignment_id
                       JOIN tree_types tt ON tt.id = pa.tree_type_id
                       WHERE st.chat_id = ?
                       ORDER BY st.created_at DESC """, (chat_id,))

    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]


def log_treatment(chat_id: str, system_id: int, pump_assignment_id: int, month_name: str, stage_name: str, result: str, sensor_snapshot: str = None) -> dict | None:
    ALLOWED_RESULTS = {"success", "skipped", "failed"}
    if result not in ALLOWED_RESULTS:
        print(f"ERROR! Недопустимый результат!! '{result}'")
        return None
    
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try: 
        now = datetime.datetime.now().isoformat()
        cursor.execute("""
                       INSERT INTO treatment_log (chat_id, system_id, pump_assignment_id, month_name, stage_name, result, sensor_snapshot, completed_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", (chat_id, system_id, pump_assignment_id, month_name, stage_name, result, sensor_snapshot, now))
        conn.commit()
        
        new_id = cursor.lastrowid
        cursor.execute("SELECT * FROM treatment_log WHERE id = ?", (new_id,))
        
        row = cursor.fetchone()
        conn.close()
        return dict(row)
    
    except sqlite3.IntegrityError as e:
        conn.close()
        print(f"ERROR! Ошибка записи в лог: {e}")
        return None
    
def get_last_treatment_date(chat_id: str, stage_name: str) -> datetime.datetime | None:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT MAX (completed_at) FROM treatment_log WHERE chat_id = ? and stage_name = ? and result = 'success'""", (chat_id, stage_name))

    row = cursor.fetchone()
    conn.close()

    if row is None or row[0] is None:
        return None

    return datetime.datetime.fromisoformat(row[0])

def get_treatment_history(chat_id: str, limit: int = 10 ) -> list:
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT 
                   tl.*,
                   s.sys_id,
                   s.name AS system_name,
                   pa.pump_number,
                   tt.name AS tree_name
                   FROM treatment_log tl
                   JOIN systems s ON s.id = tl.system_id
                   JOIN pump_assignments pa ON pa.id = tl.pump_assignment_id
                   JOIN tree_types tt ON tt.id = pa.tree_type_id
                   WHERE tl.chat_id = ?
                   ORDER BY tl.completed DESC 
                   LIMIT ? """, (chat_id, limit))

    rows = cursor.fetchall()
    conn.close()
    return [dict(row) for row in rows]

def check_sensor_ok(chat_id: str, temp_min_override: float | None = None) -> dict:
    user = get_or_create_user(chat_id)
    cash = get_sensor_cash(chat_id)
    reasons = []

    light_night = float(user['light_night'])
    wind_max = float(user['wind_max'])
    humidity_max = float(user['humidity_max'])
    temp_min = float(user['temp_min'])

    dark = False
    if cash.get("light") is not None:
        dark = float(cash['light']) <= light_night
    if not dark:
        reasons.append('Ещё светло (освещённость выше ночного порога)')

    wind_ok = False
    if cash.get('wind') is not None :
        wind_ok = float(cash['wind']) <= wind_max
    if not wind_ok:
        reasons.append(f'Сильный ветер: {cash.get("wind")} > {wind_max}')

    humidity_ok = False
    if cash.get('humidity') is not None:
        humidity_ok = float(cash['humidity']) <= humidity_max
    if not humidity_ok:
        reasons.append(f'Высокая влажность: {cash.get("humidity")} > {humidity_max}')

    effective_temp_min = float(temp_min_override) if temp_min_override is not None else temp_min
    temp_ok = False
    if cash.get('temp') is not None:
        temp_ok = float(cash['temp']) >= effective_temp_min
    if not temp_ok:
        reasons.append(f'Низкая температура: {cash.get("temp")} < {effective_temp_min}')


    return {
        "ok": dark and wind_ok and humidity_ok and temp_ok,
        "dark": dark,
        "wind_ok": wind_ok,
        "humidity_ok": humidity_ok,
        "temperature_ok": temp_ok,
        "reasons": reasons,
        "cash": cash,
    }
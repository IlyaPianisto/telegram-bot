import database
import sqlite3
import os

database.init_db()
print("База создана")

conn = sqlite3.connect(os.getenv("DB_FILE", "garden_bot.db"))

print("\nТаблицы")
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
for table in tables:
    print(table[0])

print("\nДеревья")
rows = conn.execute("SELECT id, name FROM tree_types").fetchall()
for row in rows:
    print(f"{row[0]} - {row[1]}")

conn.close()
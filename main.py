import logging
import asyncio
import json
import os
from dotenv import load_dotenv
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
import paho.mqtt.client as mqtt
from typer.cli import callback

import database as db

# Настройки
load_dotenv()
TOKEN = os.getenv("TOKEN")
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_TOPIC_SUBSCRIBE = os.getenv("MQTT_TOPIC_SUBSCRIBE")

TREATMENTS_FILE = "treatments.json"

TREES_PER_PAGE = 5
SYSTEMS_PER_PAGE = 5

# Логирование
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Глобальные переменные
mqtt_client = None

def load_treatments() -> dict:
    if not os.path.exists(TREATMENTS_FILE):
        logger.warning(f"ФАЙЛ ОБРАБОТОК ({TREATMENTS_FILE}) НЕ НАЙДЕН!!!")
        return {}
    try:
        with open(TREATMENTS_FILE, "r", encoding='utf8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"ERROR!! Ошибка загрузки {TREATMENTS_FILE}: {e}")
        return {}

treatments = load_treatments()

# Маппинг месяцев
MONTHS_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель", 5: "Май", 6: "Июнь",
    7: "Июль", 8: "Август", 9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
}

user_states: dict = {}

# ВСЕ НЕОБХОДИМЫЕ ФУНКЦИИ

def init_user (chat_id) -> str:
    str_id = str(chat_id)
    db.get_or_create_user(str_id)
    if str_id not in user_states:
        user_states[str_id] = {
            'active_system_id' : None,
            'pump_states' : {i : False for i in range(1, 9)},
            'calibrating' : None,
            'awaiting_input' : None,
            'pending_treatment' : None,
        }

    return str_id

def get_active_system(str_id: str) -> dict | None:
    system_id = user_states[str_id].get('active_system_id')
    if system_id is None:
        return None

    return db.get_system(system_id)

def format_sensor_snapshot(cash: dict) -> str:
    return json.dumps({
        "wind": cash.get("wind"),
        "light": cash.get("light"),
        "temp": cash.get("temp"),
        "humidity": cash.get("humidity"),
    }, ensure_ascii=False)

def publish_command(chat_id, sys_id, command):
    topic = f"app/{chat_id}/{sys_id}/control"
    if mqtt_client:
        mqtt_client.publish(topic, command)
        logger.info(f"MQTT OUT {topic}: {command}")

def on_connect(client):
    logger.info("Connected to MQTT")
    client.subscribe(MQTT_TOPIC_SUBSCRIBE)

def on_message(msg):
    try:
        payload = msg.payload.decode()
        parts = msg.topic.split('/')
        owner_id = parts[1]

        if owner_id not in user_states:
            return

        state = user_states[owner_id]

        if payload.startswith("VETER:"):
            val = float(payload.split(":")[1])
            db.update_sensor_cash(owner_id, "wind", val)

        elif payload.startswith("POT:"):
            val = float(payload.split(":")[1])
            db.update_sensor_cash(owner_id, "light", val)

        elif payload.startswith("BME:"):
            bme_parts = payload[4].split("|")
            for part in bme_parts:
                if part.startswith("T:"):
                    db.update_sensor_cash(owner_id, "temp", float(part[2:]))
                elif part.startswith("H:"):
                    db.update_sensor_cash(owner_id, "humidity", float(part[2:]))

        elif payload.startswith("NASOS:"):
            parts = payload.split(":")
        if len(parts) >= 3:
            p_num = int(parts[1])
            state = (parts[2] == "ON")
            user_states[owner_id]["pump_states"][p_num] = state

    except Exception as e:
        logger.error(f"MQTT Parse error: {e}")

# Кнопушки навигация

def kb_main_menu():
    kb = [
        [InlineKeyboardButton("Обработка деревьев", callback_data="menu:treatment_auto")], # проверять!
        [InlineKeyboardButton("Настройки", callback_data="menu:settings")]
    ]
    return InlineKeyboardMarkup(kb)

def kb_settings_menu():
    kb = [
        [InlineKeyboardButton("Привязать/Отвязать систему", callback_data="menu:system_menu")],
        [InlineKeyboardButton("Калибровка", callback_data="menu:calibration")],
        [InlineKeyboardButton("Отладка", callback_data="menu:debug")],
        [InlineKeyboardButton("Конфигурация деревьев", callback_data="menu:tree_type_config:0")],
        [InlineKeyboardButton("<- Назад", callback_data="menu:main")]
    ]
    return InlineKeyboardMarkup(kb)

def kb_system_menu(str_id: str, page: int = 0) -> InlineKeyboardMarkup:
    systems = db.get_user_systems(str_id)
    sys_map = {s['sys_id']: s for s in systems} #мапинг

    max_slot = max(max(sys_map.keys(), default=0) + 1,1)
    slots = list(range(1, max_slot + 1))

    start = page * SYSTEMS_PER_PAGE
    end = start + SYSTEMS_PER_PAGE
    page_slots = slots[start:end]

    rows = []
    for slot in page_slots:
        if slot in sys_map:
            s = sys_map[slot]
            label = f"{slot}. {s['name']}"
            cb= f"sys:select:{s['id']}:{page}"
        else:
            label= f"{slot}. (Пусто)"
            cb = f"sys:empty:{slot}:{page}"
        rows.append([InlineKeyboardButton(label, callback_data=cb)])

    nav = []

    if page > 0:
        nav.append(InlineKeyboardButton("<<", callback_data=f"sys:page:{page-1}"))
    if end < len(slots):
        nav.append(InlineKeyboardButton(">>", callback_data=f"sys:page:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("<- Назад", callback_data="menu:settings")])

    return InlineKeyboardMarkup(rows)

def kb_chosen_system(system_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Переименовать", callback_data=f"sys:rename:{system_id}:{page}")],
        [InlineKeyboardButton("Удалить", callback_data=f"sys:delete_confirm:{system_id}:{page}")],
        [InlineKeyboardButton("<- Назад", callback_data=f"sys:page:{page}")],
    ])

def kb_chosen_empty_system(slot: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Добавить", callback_data=f"sys:add:{slot}:{page}")],
        [InlineKeyboardButton("<- Назад", callback_data=f"sys:page:{page}")],
    ])

def kb_add_new_system(slot: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Далее ->", callback_data=f"sys:add_name:{slot}:{page}")],
        [InlineKeyboardButton("<- Назад", callback_data=f"sys:empty:{slot}:{page}")],
    ])

# Калибровка

def kb_calibration_menu(str_id: str) -> InlineKeyboardMarkup:
    user = db.get_or_create_user(str_id)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Размер ёмкостей...", callback_data="clib:bottle")],
        [InlineKeyboardButton(f"Время опрыскивания (c): {user['pump_flow_rate']}", callback_data="calib:field:pump_flow_rate")],
        [InlineKeyboardButton(f"Освещённость (Ночь): {user['light_night']}", callback_data="calib:field:light_night")],
        [InlineKeyboardButton(f"Максимальная скорость ветра (м/с): {user['wind_max']}", callback_data="calib:field:wind_max")],
        [InlineKeyboardButton(f"Максимальная влажность: {user['humidity_max']}", callback_data="calib:field:humidity_max")],
        [InlineKeyboardButton("<- Назад", callback_data="menu:system_menu")],
])

def kb_chose_pump (system_id:int, purpose: str, page: int) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i in range(1, 9):
        rows.append([InlineKeyboardButton(f"Насос {i}", callback_data=f"pump:{purpose}:{system_id}:{i}")])
        if len(row) == 2:
            row.append(row)
            row = []

    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("<- Назад", callback_data=f"sys:select:{system_id}:{page}")])

    return InlineKeyboardMarkup(rows)

def kb_calib_value(field: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Считать текущее", callback_data="calib:read_sensor")],
        [InlineKeyboardButton("Ввести вручную", callback_data=f"calib:manual:{field}")],
        [InlineKeyboardButton("Вернуть к стандартному", callback_data=f"calib:default:{field}")],
        [InlineKeyboardButton("<- Назад", callback_data="menu:calibration")],
    ])

def kb_set_calibrate_value():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Записать текущее", callback_data="")],
        [InlineKeyboardButton("<- Назад", callback_data="")],
    ])

def kb_reset_calibrate_value():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Вернуть к стандартным", callback_data="")],
        [InlineKeyboardButton("<- Назад", callback_data="")],
    ])

def kb_debug() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Вкл/Выкл насос", callback_data="debug:pumps_sys:0")],
        [InlineKeyboardButton("Показания всех датчиков", callback_data="debug:sensors")],
        [InlineKeyboardButton("<- Назад", callback_data="menu:settings")],
    ])

def kb_pumps_menu(str_id: str, system_id: int) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i in range(1, 9):
        state = user_states[str_id]["pump_states"][i]
        icon = "🟢" if state else "🔴"
        row.append(InlineKeyboardButton(f"Насос {i} {icon}", callback_data=f"pumps:toggle:{system_id}:{i}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row: rows.append(row)
    rows.append([InlineKeyboardButton("<- Назад", callback_data="debug:pumps_sys:0")])
    return InlineKeyboardMarkup(rows)

def kb_choose_system_for(str_id: str, purpose:str, page: int = 0) -> InlineKeyboardMarkup:
    systems = db.get_user_systems(str_id)
    start = page + SYSTEMS_PER_PAGE
    end = page + SYSTEMS_PER_PAGE
    page_systems = systems[start:end]

    rows = []
    for s in page_systems:
        rows.append([InlineKeyboardButton(
            f"{s['sys_id']}. {s['name']}",
            callback_data = f'{purpose}:{s["id"]}:{page}'
        )])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("<<", callback_data=f"sys:page:{page - 1}"))
    if end < len(systems):
        nav.append(InlineKeyboardButton(">>", callback_data=f"sys:page:{page + 1}"))
    if nav:
        rows.append(nav)

    back_map = {
        "pumps" : "menu_debug",
        "tree_config": "menu_settings",
        "treatment" : "menu_treatment",
    }
    rows.append([InlineKeyboardButton("<- Назад", callback_data=back_map.get(purpose, 'menu:main'))])
    return InlineKeyboardMarkup(rows)

def delete_confirm (system_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Удалить", callback_data=f"sys:delete:{system_id}:{page}")],
        [InlineKeyboardButton( "<- Назад", callback_data=f"sys:select:{system_id}:{page}")],
    ])

def kb_choose_tree_type(system_id: int, pump_num: int,page: int) -> InlineKeyboardMarkup:
    tree_types = db.get_tree_types()
    start = page + TREES_PER_PAGE
    end = page + TREES_PER_PAGE
    page_trees = tree_types[start:end]

    rows = []

    for t in page_trees:
        rows.append([InlineKeyboardButton(
            t['name'], callback_data=f"tree_config:assign:{system_id}:{pump_num}:{t['id']}"
        )])

    nav = []

    if page > 0:
        nav.append(InlineKeyboardButton("<<", callback_data=f"tree_config:tree_page:{system_id}:{pump_num}:{page - 1}"))
    if end < len(tree_types):
        nav.append(InlineKeyboardButton(">>", callback_data=f"tree_config:tree_page:{system_id}:{pump_num}:{page + 1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton("<- Назад", callback_data=f"tree_config:back_pumps:{system_id}:{page}")])


async def cmd_start(update: Update):
    init_user(update.effective_chat.id)
    await update.message.reply_text("Система управления садом.", reply_markup=kb_main_menu())

async def text_input_handler(update: Update):
        str_id = str(update.effective_chat.id)
        state = user_states[str_id]
        text = update.message.text.strip()
        waiting = state.get('awaiting_input')

        if waiting == "sys_rename":
            pending = state.get("pending_sys", {})
            system_id = pending.get("system_id")
            page = pending.get("page", 0)
            state['awaiting_input'] = None
            state['pending_sys'] = None

            if system_id:
                db.rename_system(system_id, str_id, page)
                await update.message.reply_text(f"Система переименована в: '{text}'", reply_markup=kb_chosen_system(system_id, page))

            else:
                await update.message.reply_text("ERROR!", reply_markup=kb_chosen_system(system_id, page))

        elif waiting == "sys:add_name":
            pending = state.get("pending_sys", {})
            system_id = pending.get("system_id")
            slot = pending.get("slot")
            page = pending.get("page", 0)
            state['pending_sys'] = None
            state['awaiting_input'] = None

            if slot is not None:
                result = db.add_system(str_id, slot, name=text)
                if result:
                    await update.message.reply_text(f"Система №{slot} '{text} добавлена!", reply_markup=kb_system_menu(str_id, page))



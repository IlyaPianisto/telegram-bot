import logging
import asyncio
import json
import os
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters, CallbackContext
import paho.mqtt.client as mqtt
import database as db
from datetime import datetime

# Настройки
load_dotenv()
TOKEN = os.getenv("TOKEN")
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_TOPIC_SENSORS = os.getenv("MQTT_TOPIC_SENSORS")
MQTT_TOPIC_NASOS = os.getenv("MQTT_TOPIC_NASOS")
ADMIN_ID = int(os.getenv("ADMIN_ID"))

TREATMENTS_FILE = "treatments.json"

TREES_PER_PAGE = 5
SYSTEMS_PER_PAGE = 5
TREATMENTS_PER_PAGE = 5

# Логирование
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# Глобальные переменные
processed_callbacks: set = set()
mqtt_client: mqtt.Client | None = None

async def delete_photo_and_show(query, state, str_id, text, kb):
    photo_msg_id = state.get("photo_message_id")
    if photo_msg_id:
        try:
            await query.message.chat.delete_message(photo_msg_id)
        except Exception:
            pass
        state["photo_message_id"] = None
        await query.message.reply_text(text, reply_markup=kb)
    else:
        try:
            await query.edit_message_text(text, reply_markup=kb)
        except Exception:
            await query.message.reply_text(text, reply_markup=kb)

async def cmd_get_file_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.photo:
        file_id = update.message.photo[-1].file_id
        await update.message.reply_text(file_id)

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

treatments_db = load_treatments()

CALIB_LABELS = {
    "light_night": ("Освещенность", " "),
    "wind_max":  ("Максимальная скорость ветра", "м/с"),
    "humidity_max": ("Максимальная влажность", " "),
    "pump_flow_rate": ("Время опрыскивания (c)", "c"),
}

CALIB_SENSOR_CMD = {
    "light_night": "GET_POT",
    "wind_max": "GET_VETER",
    "humidity_max": "GET_ALL",
    "temp_min": "GET_ALL",
    "pump_flow_rate": None,
}

CALIB_CASH_FIELD = {
    "light_night": "light",
    "wind_max": "wind",
    "humidity_max": "humidity",
    "temp_max": "temp",
}

# Маппинг месяцев
MONTHS_LIST = ["Январь", "Февраль", "Март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]

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
            'photo_message_id': None,
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

def cale_pump_duration_sec(chat_id: str) -> int:
    user = db.get_or_create_user(chat_id)
    volume = user.get('bottle_volume_l', db.DEFAULTS['bottle_volume_l'])
    flow_rate = user.get('pump_flow_rate', db.DEFAULTS['pump_flow_rate'])
    return max(1, round(volume * flow_rate))

def publish_command(chat_id, sys_id, command):
    topic = MQTT_TOPIC_NASOS
    if mqtt_client:
        mqtt_client.publish(topic, command)
        logger.info(f"MQTT OUT {topic}: {command}")

def on_connect(client, userdata, flags, rc, properties = None):
    logger.info("Connected to MQTT")
    client.subscribe(MQTT_TOPIC_SENSORS)

def on_message(client, userdata, msg):
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
            bme_parts = payload[4:].split("|")
            for part in bme_parts:
                if part.startswith("T:"):
                    db.update_sensor_cash(owner_id, "temp", float(part[2:]))
                elif part.startswith("H:"):
                    db.update_sensor_cash(owner_id, "humidity", float(part[2:]))

        elif payload.startswith("NASOS:"):
            nasos_parts = payload.split(":")
            if len(nasos_parts) >= 3:
                p_num = int(nasos_parts[1])
                state = (nasos_parts[2] == "ON")
                user_states[owner_id]["pump_states"][p_num] = state

    except Exception as e:
        logger.error(f"MQTT Parse error: {e}")

# Кнопушки навигация

def kb_main_menu():
    kb = [
        [InlineKeyboardButton("Обработка деревьев", callback_data="menu:treatment")],
        [InlineKeyboardButton("Настройки", callback_data="menu:settings")]
    ]
    return InlineKeyboardMarkup(kb)

def kb_settings_menu():
    kb = [
        [InlineKeyboardButton("Привязать/Отвязать систему", callback_data="menu:system_menu")],
        [InlineKeyboardButton("Калибровка", callback_data="menu:calibration")],
        [InlineKeyboardButton("Отладка", callback_data="menu:debug")],
        [InlineKeyboardButton("Конфигурация деревьев", callback_data="menu:tree_config:0")],
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
        [InlineKeyboardButton("Размер ёмкостей...", callback_data="calib:bottle")],
        [InlineKeyboardButton(f"Время опрыскивания (c): {user['pump_flow_rate']}", callback_data="calib:field:pump_flow_rate")],
        [InlineKeyboardButton(f"Освещённость (Ночь): {user['light_night']}", callback_data="calib:field:light_night")],
        [InlineKeyboardButton(f"Максимальная скорость ветра (м/с): {user['wind_max']}", callback_data="calib:field:wind_max")],
        [InlineKeyboardButton(f"Максимальная влажность: {user['humidity_max']}", callback_data="calib:field:humidity_max")],
        [InlineKeyboardButton("<- Назад", callback_data="menu:settings")],
])

def kb_chose_pump (system_id:int, purpose: str, back: str) -> InlineKeyboardMarkup:
    rows = []
    row = []
    for i in range(1, 9):
        assignment = db.get_pump_assignment(system_id, i)
        if assignment:
            label = f"Насос {i} ({assignment['tree_name']})"
        else:
            label = f"Насос {i} (не привязан)"
        rows.append([InlineKeyboardButton(label, callback_data=f"{purpose}:pump:{system_id}:{i}")])

        if len(row) == 2:
            rows.append(row)
            row = []

    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton("<- Назад", callback_data=back)])

    return InlineKeyboardMarkup(rows)

def kb_calib_value(field: str) -> InlineKeyboardMarkup:
    if field == "pump_flow_rate":
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("Ввести вручную", callback_data=f"calib:manual:{field}")],
            [InlineKeyboardButton("Вернуть к стандартному", callback_data=f"calib:default:{field}")],
            [InlineKeyboardButton("<- Назад", callback_data="menu:calibration")],
        ])
    else:
        return InlineKeyboardMarkup([
            [InlineKeyboardButton("Считать текущее", callback_data=f"calib:read_sensor:{field}")],
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

def kb_calib_confirm(field: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Записать", callback_data=f"calib:save:{field}")],
        [InlineKeyboardButton("<- Назад", callback_data=f"calib:field:{field}")],
    ])

def kb_debug() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Вкл/Выкл насос", callback_data="debug:pumps_sys:0")],
        [InlineKeyboardButton("Показания всех датчиков", callback_data="debug:sensors")],
        [InlineKeyboardButton("<- Назад", callback_data="menu:settings")],
    ])

def kb_sensors_display() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Обновить значения", callback_data="debug:sensors")],
        [InlineKeyboardButton("<- Назад", callback_data="menu:debug")],
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
    start = page * SYSTEMS_PER_PAGE
    end = start + SYSTEMS_PER_PAGE
    page_systems = systems[start:end]
    logger.info("")
    rows = []
    for s in page_systems:
        rows.append([InlineKeyboardButton(
            f"{s['sys_id']}. {s['name']}",
            callback_data = f'{purpose}:sys:{s["id"]}:{page}'
        )])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("<<", callback_data=f"sys:page:{page - 1}"))
    if end < len(systems):
        nav.append(InlineKeyboardButton(">>", callback_data=f"sys:page:{page + 1}"))
    if nav:
        rows.append(nav)

    back_map = {
        "pumps" : "menu:debug",
        "tree_config": "menu:settings",
        "treatment" : "menu:treatment",
    }
    rows.append([InlineKeyboardButton("<- Назад", callback_data=back_map.get(purpose, 'menu:main'))])
    return InlineKeyboardMarkup(rows)

def kb_delete_confirm (system_id: int, page: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Удалить", callback_data=f"sys:delete:{system_id}:{page}")],
        [InlineKeyboardButton( "<- Назад", callback_data=f"sys:select:{system_id}:{page}")],
    ])

def kb_choose_tree_type(system_id: int, pump_num: int,page: int) -> InlineKeyboardMarkup:
    tree_types = db.get_tree_types()
    start = page * TREES_PER_PAGE
    end = start + TREES_PER_PAGE
    page_trees = tree_types[start:end]

    rows = []
    if page == 0:
        rows.append([InlineKeyboardButton(
            "Не выбрано",
            callback_data=f"tree_config:unassign:{system_id}:{pump_num}"
        )])
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

    return InlineKeyboardMarkup(rows)

def kb_treatment_menu () -> InlineKeyboardMarkup:
    current_month_id = datetime.now().month - 1
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Запланировать", callback_data=f"treat:plan:{current_month_id}")],
        [InlineKeyboardButton("Отложенные", callback_data="treat:list:pending")],
        [InlineKeyboardButton("В процессе", callback_data="treat:list:running")],
        [InlineKeyboardButton("<- Назад", callback_data="menu:main")],
    ])

def kb_task_list(tasks: list, status: str) -> InlineKeyboardMarkup:
    rows = []

    for task in tasks:
        label = f"{task['stage_name']} - {task['tree_name']} ({task['scheduled_time']})"
        rows.append([InlineKeyboardButton(label, callback_data=f"treat:task:{task['id']}:{status}")])
    rows.append([InlineKeyboardButton("<- Назад", callback_data="menu:treatment")])
    return InlineKeyboardMarkup(rows)

def kb_task_selected(task_id: int, status: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Отменить", callback_data=f"treat:cancel_confirm:{task_id}:{status}")],
        [InlineKeyboardButton("<- Назад", callback_data=f"treat:list:{status}")],
    ])

def kb_task_cancel_confirm(task_id: int, status: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Да", callback_data=f"treat:cancel:{task_id}:{status}")],
        [InlineKeyboardButton("<- Назад", callback_data=f"treat:task:{task_id}:{status}")],
    ])

def kb_plan_stage(month_id : int, page: int) -> InlineKeyboardMarkup:
    month_name = MONTHS_LIST[month_id]
    stages = treatments_db.get(month_name, {}).get('stages', [])
    start = page * TREES_PER_PAGE
    end = start + TREES_PER_PAGE
    page_stages = stages[start:end]

    rows = []
    for i, stage in enumerate(page_stages):
        rows.append([InlineKeyboardButton(stage['name'][3:len(stage['name'])-4], callback_data=f"treat:stage:{month_id}:{start+i}")]) #peredelat

    nav = []

    if page > 0:
        nav.append(InlineKeyboardButton("<<", callback_data=f"treat:plan_page:{month_id}:{page - 1}"))
    if end < len(stages):
        nav.append(InlineKeyboardButton(">>", callback_data=f"treat:plan_page:{month_id}:{page + 1}"))
    if nav:
        rows.append(nav)

    month_nav = []

    if month_id > 0:
        month_nav.append(InlineKeyboardButton("Пред. месяц", callback_data=f"treat:plan:{month_id - 1}"))
    if month_id < 11:
        month_nav.append(InlineKeyboardButton("След. месяц", callback_data=f"treat:plan:{month_id + 1}"))
    if month_nav:
        rows.append(month_nav)

    rows.append([InlineKeyboardButton("<- Назад", callback_data=f"menu:treatment")])

    return InlineKeyboardMarkup(rows)


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    init_user(update.effective_chat.id)
    await update.message.reply_text("Система управления садом.", reply_markup=kb_main_menu())


async def text_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            db.rename_system(system_id, str_id, text)
            await update.message.reply_text(f"Система переименована в: '{text}'", reply_markup=kb_chosen_system(system_id, page))
        else:
            await update.message.reply_text("ERROR!", reply_markup=kb_chosen_system(system_id, page))

    elif waiting == "sys:add_name":
        pending = state.get("pending_sys", {})
        slot = pending.get("slot")
        page = pending.get("page", 0)
        state['pending_sys'] = None
        state['awaiting_input'] = None
        if slot is not None:
            result = db.add_system(str_id, slot, name=text)
            if result:
                await update.message.reply_text(f"Система №{slot} '{text} добавлена!", reply_markup=kb_system_menu(str_id, page))
            else:
                await update.message.reply_text("Система с таким номером уже существует!", reply_markup=kb_system_menu(str_id, page))
        else:
            await update.message.reply_text("ERROR!", reply_markup=kb_main_menu())

    elif waiting == "calib:manual":
        pending = state.get("pending_calib", {})
        field = pending.get("field")
        if field:
            try:
                vol = float(text.replace(",", "."))
                if vol <= 0:
                    raise ValueError
                db.update_user_settings(str_id, field, vol)
                state['awaiting_input'] = None
                state['pending_calib'] = None
                await update.message.reply_text("Изменения внесены!", reply_markup=kb_calibration_menu(str_id))
            except ValueError:
                await update.message.reply_text("Введите положительное число!", reply_markup=kb_calibration_menu(str_id))
        else:
            await update.message.reply_text("ERROR!", reply_markup=kb_main_menu())

    elif waiting == "calib:bottle":
        try:
            val = float(text)
            if val <= 0:
                raise ValueError
            db.update_user_settings(str_id, "bottle_volume_l", val)
            state['awaiting_input'] = None
            await update.message.reply_text(f"Объём ёмкости сохранён! {val} Л.", reply_markup=kb_calibration_menu(str_id))

        except ValueError:
            await update.message.reply_text("ERROR! Введите положительное число (л):")

    else:
        await update.message.reply_text("Используйте кнопки меню!", reply_markup=kb_main_menu())


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.id in processed_callbacks:
        return
    processed_callbacks.add(query.id)

    if len(processed_callbacks) > 100:
        processed_callbacks.clear()

    data = query.data
    str_id = init_user(update.effective_chat.id)
    state = user_states[str_id]

    if data == 'menu:main':
        await query.edit_message_text("Главное меню:", reply_markup=kb_main_menu())

    elif data == "menu:settings":
        await query.edit_message_text("Настройки", reply_markup=kb_settings_menu())

    elif data == "menu:system_menu":
        await query.edit_message_text("Управление системами. Выберите систему!", reply_markup=kb_system_menu(str_id, 0))

    elif data == "menu:debug":
        await query.edit_message_text("Отладка насосов:", reply_markup=kb_debug())

    elif data == "menu:calibration":
        await query.edit_message_text("Калибровка:", reply_markup=kb_calibration_menu(str_id))

    elif data.startswith("sys:page"):
        page = int(data.split(":")[2])
        await query.edit_message_reply_markup(reply_markup=kb_system_menu(str_id, page))

    elif data.startswith("sys:select:"):
        parts = data.split(":")
        system_id = int(parts[2])
        page = int(parts[3])
        system = db.get_system(system_id)
        await query.edit_message_text(f"Выбрана система №{system['sys_id']}  \"{system['name']}\"", reply_markup=kb_chosen_system(system_id, page))

    elif data.startswith("sys:rename:"):
        parts = data.split(":")
        system_id = int(parts[2])
        page = int(parts[3])
        state['awaiting_input'] = 'sys_rename'
        state['pending_sys'] = {'system_id': system_id, 'page': page}
        await query.edit_message_text("Введите новое название системы:", reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("<- Назад", callback_data=f"sys:select:{system_id}:{page}"),
        ]]))

    elif data.startswith("sys:delete_confirm:"):
        parts = data.split(":")
        system_id = int(parts[2])
        page = int(parts[3])
        system = db.get_system(system_id)
        await query.edit_message_text(f"Удалить систему №{system['sys_id']} \"{system['name']}\"", reply_markup=kb_delete_confirm(system_id, page))

    elif data.startswith("sys:delete:"):
        parts = data.split(":")
        system_id = int(parts[2])
        page = int(parts[3])
        db.delete_system(system_id, str_id)
        await query.edit_message_text("Система удалена", reply_markup=kb_system_menu(str_id, page))

    elif data.startswith("sys:empty:"):
        parts = data.split(":")
        slot = int(parts[2])
        page = int(parts[3])
        await query.edit_message_text(f"Выбран слот №{slot} \n Слот свободен.", reply_markup=kb_chosen_empty_system(slot, page))

    elif data.startswith("sys:add:"):
        parts = data.split(":")
        slot = int(parts[2])
        page = int(parts[3])
        await query.edit_message_text(
            f"<b>Добавление системы №{slot}</b>\n\n"
            f"Вставьте ваш Telegram id и номер системы в настройках WiFi:\n\n"
            f"Ваш Telegram id: <code>{str_id}</code>\n"
            f"Номер системы: <code>{slot}</code>\n\n"
            f"После настройки устройства нажмите \"Далее\".",
            parse_mode="HTML",
            reply_markup=kb_add_new_system(slot, page))

    elif data.startswith("sys:add_name:"):
        parts = data.split(":")
        slot = int(parts[2])
        page = int(parts[3])
        state['awaiting_input'] = 'sys:add_name'
        state['pending_sys'] = {'slot': slot, 'page': page}
        await query.edit_message_text("Напишите название для вашей системы:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("<- Назад", callback_data="sys:add:")]]))

    elif data == "calib:bottle":
        user = db.get_or_create_user(str_id)
        state['awaiting_input'] = 'calib:bottle'
        await query.edit_message_text(f"Текущий объём: {user['bottle_volume_l']} л.\nВведите новый объём в литрах:", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("<- Назад", callback_data="menu:calibration")]]))

    elif data.startswith("calib:field"):
        field = data.split(":")[2]
        user = db.get_or_create_user(str_id)
        label, hint = CALIB_LABELS.get(field, (field, ""))
        state['pending_calib'] = {'field': field}
        await query.edit_message_text(f"{label}\nТекущее: {user[field]}{hint}",
                                      reply_markup=kb_calib_value(field))

    elif data.startswith("calib:manual"):
        field = data.split(":")[2]
        label, hint = CALIB_LABELS.get(field, (field, ""))
        user = db.get_or_create_user(str_id)
        state["awaiting_input"] = "calib:manual"
        state['pending_calib'] = {'field': field}
        await query.edit_message_text(f"{label}\nТекущее: {user[field]}\nВведите новое значение {hint}:",)

    elif data.startswith("calib:default"):
        field = data.split(":")[2]
        default_val = db.DEFAULTS.get(field)
        label = CALIB_LABELS.get(field, (field,))[0]
        if default_val is not None:
            db.update_user_settings(str_id, field, default_val)
            await query.edit_message_text(f"\"{label}\" сброшено до стандартного значения: {default_val}", reply_markup=kb_calibration_menu(str_id))

    elif data.startswith("calib:read_sensor:"):
        field = data.split(":")[2]
        systems = db.get_user_systems(str_id)
        if not systems:
            await query.edit_message_text("Нет привязанных систем!", reply_markup=kb_calibration_menu(str_id))
            return
        cmd = CALIB_SENSOR_CMD.get(field)
        if not cmd:
            await query.edit_message_text("Для этого параметра датчиков нет!", reply_markup=kb_calib_value(field))
            return
        publish_command(str_id, systems[0]["sys_id"], cmd)
        await query.edit_message_text("Запрашиваем значения...")

        cash = db.get_sensor_cash(str_id)
        cash_key = CALIB_CASH_FIELD.get(field)
        read_value = cash.get(cash_key) if cash_key else None

        if read_value is None:
            await query.edit_message_text("От датчика нет ответа! :(", reply_markup=kb_calib_value(field))
            return

        state['pending_calib'] = {'field': field, "read_value": read_value}
        label = CALIB_LABELS.get(field, (field,))[0]

        await query.edit_message_text(
            f"{label}\nТекущее значение с датчика: {read_value}\n\nЗаписать это значение?", reply_markup=kb_calib_confirm(field)
        )

    elif data.startswith("calib:save"):
        field = data.split(":")[2]
        pending =  state.get("pending_calib", {})
        value = pending.get("read_value")
        label = CALIB_LABELS.get(field, (field,))[0]

        if value is None:
            await query.edit_message_text("ERROR! Значене не найдено!", reply_markup=kb_calib_value(field))
            return
        db.update_user_settings(str_id, field, value)
        state["pending_calib"] = None
        await query.edit_message_text(f"\"{label}\" сохранено: {value}", reply_markup=kb_calibration_menu(str_id))

    elif data.startswith("debug:pumps_sys:"):
        page = int(data.split(":")[2])
        systems = db.get_user_systems(str_id)
        if not systems:
            await query.edit_message_text("У вас нет привязанных систем :(", reply_markup=kb_debug())
            return
        await query.edit_message_text("Выберите систему:", reply_markup=kb_choose_system_for(str_id, "pumps", page))

    elif data.startswith("pumps:sys:"):
        parts = data.split(":")
        system_id = int(parts[2])
        system = db.get_system(system_id)
        await query.edit_message_text(
            f"Выбрана система {system['sys_id']}. \"{system['name']}\'",
            reply_markup=kb_pumps_menu(str_id, system_id)
        )

    elif data.startswith("pumps:toggle:"):
        parts = data.split(":")
        system_id = int(parts[2])
        p_num = int(parts[3])
        system = db.get_system(system_id)
        if not system:
            return
        curr = state["pump_states"][p_num]
        action = "OFF" if curr else "ON"
        publish_command(str_id, system['sys_id'], f"NASOS:{p_num}:{action}")
        state["pump_states"][p_num] = not curr
        try:
            await query.edit_message_reply_markup(reply_markup=kb_pumps_menu(str_id, system_id))
        except Exception:
            pass

    elif data == "debug:sensors":
        systems = db.get_user_systems(str_id)
        if not systems:
            await query.edit_message_text("Нет привязанных систем!", reply_markup=kb_debug())
            return
        s = systems[0]
        publish_command(str_id, s['sys_id'], "GET_ALL")
        publish_command(str_id, s['sys_id'], "GET_VETER")
        publish_command(str_id, s['sys_id'], "GET_POT")

        await query.edit_message_text("Идёт запрос данных...", reply_markup=kb_debug())
        cash = db.get_sensor_cash(str_id)
        age = cash.get("age_minutes", "?")
        await query.edit_message_text(
            "Показания датчиков\n"
            f"Обновлено {age} минут назад\n\n"
            f"Температура: {cash.get("temp", "--")} ℃\n"
            f"Влажность: {cash.get("humidity", "--")}\n"
            f"Ветер: {cash.get("wind", "--"):.6f}\n"
            f"Свет: {cash.get("light", "--")}",

            reply_markup=kb_sensors_display()
        )

    elif data.startswith("menu:tree_config:"):
        page = int(data.split(":")[2])
        systems = db.get_user_systems(str_id)
        if not systems:
            await query.edit_message_text("Нет привязанных систем!", reply_markup=kb_settings_menu())
            return
        await query.edit_message_text("Конфигурация деревьев. Выберите систему:", reply_markup=kb_choose_system_for(str_id, "tree_config", page))

    elif data.startswith("tree_config:sys:"):
        parts = data.split(":")
        system_id = int(parts[2])
        system = db.get_system(system_id)
        await query.edit_message_text(
            f"Система \"{system['name']}\". Выберите насос:",
            reply_markup=kb_chose_pump(system_id, "tree_config", "menu:tree_config:0")
        )

    elif data.startswith("tree_config:pump:"):
        parts = data.split(":")
        system_id = int(parts[2])
        pump_num = int(parts[3])
        curr = db.get_pump_assignment(system_id, pump_num)
        curr_txt = f"Сейчас: {curr['tree_name']}" if curr else "Сейчас не привязан"
        await query.edit_message_text(
            f"Насос {pump_num} - выберите тип дерева: \n{curr_txt}",
            reply_markup=kb_choose_tree_type(system_id, pump_num, 0))

    elif data.startswith("tree_config:assign:"):
        parts = data.split(":")
        system_id = int(parts[2])
        pump_num = int(parts[3])
        tree_type_id = int(parts[4])
        result = db.assign_pump(system_id, pump_num, tree_type_id)
        if result:
            await query.edit_message_text(
                f"Насос {pump_num} привязан к \"{result['tree_name']}\"",
                reply_markup=kb_chose_pump(system_id, "tree_config", "menu:tree_config:0")
            )

    elif data.startswith("tree_config:unassign:"):
        parts = data.split(":")
        system_id = int(parts[2])
        pump_num = int(parts[3])
        db.remove_pump_assignment(system_id, pump_num)
        await query.edit_message_text(
            f"Насос {pump_num} - привязка снята",
            reply_markup=kb_chose_pump(system_id, "tree_config", "menu:tree_config:0")
        )

    elif data.startswith("tree_config:back_pumps:"):
        parts = data.split(":")
        system_id = int(parts[2])
        system = db.get_system(system_id)
        await query.edit_message_text(
            f"Система \"{system['name']}\". Выберите насос",
            reply_markup=kb_chose_pump(system_id, "tree_config", "menu:tree_config:0")
        )

    elif data.startswith("tree_config:tree_page:"):
        parts = data.split(":")
        system_id = int(parts[2])
        pump_num = int(parts[3])
        page = int(parts[4])
        await query.edit_message_reply_markup(
            reply_markup=kb_choose_tree_type(system_id, pump_num, page)
        )

    elif data.startswith("treatment:sys:"):
        parts = data.split(":")
        system_id = int(parts[2])
        system = db.get_system(system_id)
        pending = state.get("pending_treatment", {})
        month_name = pending.get("month_name")
        stage_id = pending.get("stage_id", 0)
        logger.info(f"month_name={month_name}, stage_id={stage_id}")
        if not month_name:
            await query.edit_message_text(
                "Ошибка! Начните выбор заново",
                reply_markup = kb_treatment_menu()
            )
            return
        stage = treatments_db.get(month_name, {}).get("stages", [])[stage_id]
        stage_name = stage["name"]
        stage_trees = stage.get("trees", [])

        all_pumps = db.get_system_pumps(system_id)
        target_pumps = [p for p in all_pumps if p["tree_name"] in stage_trees]

        if not target_pumps:
            await query.edit_message_text(
                "У вас нет подходящих деревьев для этой обработки",
                reply_markup=kb_choose_system_for(str_id, "treatment", 0)
            )
            return

        for pump in target_pumps:
            db.add_schedule_task(
                str_id,
                system_id,
                pump["id"],
                month_name,
                stage_name,
                "13:10"
            )

        state["pending_treatment"] = None
        label_map = {"pending": "Отложенные", "running": "В процессе"}
        trees_used = ", ".join(set(p["tree_name"] for p in target_pumps))
        text = (
            "Обработка запланирована на 13:10\n\n"
            f"Система: {system['name']}\n"
            f"Обработка: {stage_name}\n"
            f"Деревья: {trees_used}\n"
            f"Насосы: {len(target_pumps)}\n"
        )
        await delete_photo_and_show(query, state, str_id, text, kb_treatment_menu())


    elif data == "menu:treatment":
        await query.edit_message_text("Обработка деревьев", reply_markup=kb_treatment_menu())

    elif data.startswith("treat:list:"):
        status = data.split(":")[2]
        tasks = db.get_user_tasks(str_id, status=status)

        label_map = {"pending": "Отложенные", "running": "В процессе"}
        if not tasks:
            await query.edit_message_text(
                f"{label_map[status]}\nОбработок нет.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("<- Назад", callback_data="menu:treatment")]]))
            return
        await query.edit_message_text(f"{label_map[status]}\nОбработки", reply_markup=kb_task_list(tasks, status))

    elif data.startswith("treat:task:"):
        parts = data.split(":")
        task_id = int(parts[2])
        status = parts[3]
        tasks = db.get_user_tasks(str_id, status=status)
        task = next((t for t in tasks if t['id'] == task_id), None)
        label_map = {"pending": "Отложенные", "running": "В процессе"}
        if not task:
            await query.edit_message_text("Задача не найдена", reply_markup=kb_treatment_menu())
            return
        await query.edit_message_text(
            f"{task['tree_name']}\n"
            f"Обработка: {task['stage_name']}\n"
            f"Система: {task['system_name']}\n"
            f"Насос: {task['pump_number']}\n"
            f"Время: {task['scheduled_time']}\n"
            f"Статус: {label_map[status]}",
            reply_markup=kb_task_selected(task_id, status)
        )

    elif data.startswith("treat:cancel_confirm:"):
        parts = data.split(":")
        task_id = int(parts[2])
        status = parts[3]
        await query.edit_message_text('Вы уверены, что хотите отменить обработку?', reply_markup=kb_task_cancel_confirm(task_id, status))

    elif data.startswith("treat:cancel:"):
        parts = data.split(":")
        task_id = int(parts[2])
        status = parts[3]
        db.update_task_status(task_id, 'cancelled')
        tasks = db.get_user_tasks(str_id, status=status)
        label_map = {'pending': "Отложенные", 'running': "В процессе"}
        if not tasks:
            await query.edit_message_text(f"Обработка отменена\n{label_map[status]}\nОбработок нет", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("<- Назад", callback_data="menu:treatment")]]))
            return
        await query.edit_message_text(f"Обработка отменена. \n\n{label_map[status]}\nОбработки:", reply_markup=kb_task_list(tasks, status))

    elif data.startswith("treat:plan:"):
        month_id = int(data.split(":")[2])
        month_name = MONTHS_LIST[month_id]
        stages = treatments_db.get(month_name,{}).get('stages', [])
        text = (
            f"Месяц: {month_name}\n"
            f"Доступные обработки: {len(stages)}"
        )
        kb = kb_plan_stage(month_id, 0)
        await delete_photo_and_show(query, state, str_id, text, kb)

    elif data.startswith("treat:plan_page:"):
        parts = data.split(":")
        month_id = int(data.split(":")[2])
        page = int(parts[3])
        try:
            await query.edit_message_reply_markup(reply_markup=kb_plan_stage(month_id, page))
        except Exception:
            month_name = MONTHS_LIST[month_id]
            stages = treatments_db.get(month_name,{}).get('stages', [])
            await query.edit_message_caption(f"Месяц: {month_name}\nДоступные обработки: {len(stages)}", reply_markup=kb_plan_stage(month_id, page))

    elif data.startswith("treat:stage:"):
        parts = data.split(":")
        month_id = int(parts[2])
        stage_id = int(parts[3])
        month_name = MONTHS_LIST[month_id]

        stages = treatments_db.get(month_name,{}).get('stages', [])
        if stage_id >= len(stages):
            await query.edit_message_text("Обработка не найдена.", reply_markup=kb_treatment_menu())
            return

        stage = stages[stage_id]
        photo_url = stage.get("photo_url")

        text = f"{stage['name']}\n"
        if stage.get("instruction"):
            text += f"{stage['instruction']}"


        state['pending_treatment'] = {
            "month_name": month_name,
            "month_id": month_id,
            "stage_id": stage_id,
        }

        rows = [
            [InlineKeyboardButton("Готово!", callback_data=f"treat:confirm:{month_id}:{stage_id}")],
            [InlineKeyboardButton("<- Назад", callback_data=f"treat:plan:{month_id}")],
        ]

        if photo_url:
            sent = await query.message.reply_photo(
                photo = photo_url,
                caption=text,
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(rows),
            )
            state['photo_message_id'] = sent.message_id
            await query.delete_message()
        else:
            state['photo_message_id'] = None
            await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows))

    elif data.startswith("treat:confirm:"):
        parts = data.split(":")
        month_id = int(parts[2])
        stage_id = int(parts[3])
        month_name = MONTHS_LIST[month_id]
        stage = treatments_db.get(month_name,{}).get('stages', [])[stage_id]
        trees_text = ','.join(stage.get("trees", [])) or "-"

        text = (
            f"Подтверждение \n\n"
            f"Месяц: {month_name}\n"
            f"Обработка: {stage['name']}\n"
            f"Деревья: {trees_text}\n\n"
            "Приготовьте раствор и залейте в ёмкости. Затем выберите системы для обработки"
        )
        kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("Выбрать систему", callback_data="treat:choose_system")],
                [InlineKeyboardButton("<- Назад", callback_data=f"treat:stage:{month_id}:{stage_id}")],
            ])
        await delete_photo_and_show(query, state, str_id, text, kb)


    elif data == "treat:choose_system":
        systems = db.get_user_systems(str_id)
        if not systems:
            text = "Нет привязанных систем"
            kb = kb_treatment_menu()
        else:
            text = "Выберите систему:"
            kb = kb_choose_system_for(str_id, "treatment", 0)
        await delete_photo_and_show(query, state, str_id, text, kb)

    elif data.startswith("menu_treatment"):
        parts = data.split(":")
        system_id = int(parts[2])
        system = db.get_system(system_id)

        if state.get('pending_treatment') is None:
            state['pending_treatment'] = {}
        state['pending_treatment']["system_id"] = system_id

        pumps = db.get_system_pumps(system_id)

        if not pumps:
            await query.edit_message_text(f"В системе \"{system['name']}\" нет привязанных насосов!", reply_markup=kb_treatment_menu())
            return

        rows = []
        for pump in pumps:
            rows.append([InlineKeyboardButton(f"{pump['pump_number']}. {pump['tree_name']}", callback_data=f"treat:pump:{system_id}:{pump['id']}")])

        rows.append([InlineKeyboardButton("<- Назад", callback_data=f"treat:choose_system")])
        await query.edit_message_text(f"Система \"{system['name']}\"\nВыберите насос:", reply_markup=InlineKeyboardMarkup(rows))

    # elif data.startswith("treat:pump"):
    #     parts = data.split(":")
    #     system_id = int(parts[2])
    #     pump_assignment_id = int(parts[3])
    #     pending = state.get('pending_treatment', {})
    #     month_name = pending.get('month_name')
    #     stage_id = pending.get('stage_id', 0)
    #
    #     if not month_name:
    #         await query.edit_message_text("ERROR! Начините выбор заново.", reply_markup=kb_treatment_menu())
    #         return
    #
    #     stage = treatments_db.get(month_name,{}).get('stages', [])[stage_id]
    #     stage_name = stage['name']
    #     pumps = db.get_system_pumps(system_id)
    #     pump = next((p for p in pumps if p['id'] == pump_assignment_id), None)
    #
    #     db.add_schedule_task(
    #         str_id,
    #         system_id,
    #         pump_assignment_id,
    #         month_name,
    #         stage_name,
    #         "13:10"
    #     )
    #
    #     state['pending_treatment'] = None
    #     tree_name = pump['tree_name'] if pump else '-'
    #     pump_num = pump['pump_number'] if pump else '-'
    #
    #     await query.edit_message_text(
    #         f"Обработка запланирована на 13:10\nНасос {pump_num} - {tree_name}\nОбработка: {stage_name}", reply_markup=kb_treatment_menu()
    #     )

async def admin_commands(update: Update, context: ContextTypes.DEFAULT_TYPE):
    parts = update.message.text.split()
    command = parts[0].replace("/", "").lower()

    if command == "add_tree_type":
        if len(parts) < 2:
            await update.message.reply_text("Используй: /add_tree_type <Название дерева>")
            return

        tree_name = parts[1]
        tree_types = db.get_tree_types()

        if any(tree['name'] == tree_name for tree in tree_types):
            await update.message.reply_text("Такое дерево уже есть в бд")
            return

        else:
            result = db.add_tree_type(tree_name)
            if result != None:
                await update.message.reply_text(f"Дерево \"{tree_name}\" добавлено в базу!")
            else:
                await update.message.reply_text(f"Возникла ошибка при добавлении!")

    elif command == "delete_tree_type":
        if len(parts) < 2:
            await update.message.reply_text("Используй: /delete_tree_type <Название дерева>")
            return

        tree_name = parts[1]
        tree_types = db.get_tree_types()

        if any(tree['name'] == tree_name for tree in tree_types):
            result = db.delete_tree_type(tree_name)
            if result:
                await update.message.reply_text(f"Дерево \"{tree_name}\" удалено из базы!")
            else:
                await update.message.reply_text(f"Возникла ошибка при удалении! ")

        else:
            await update.message.reply_text("Такого дерева нет в бд!")
            return


async def task_scheduler(app):
    while True:
        await asyncio.sleep(60)
        try:
            tasks = db.get_pending_tasks()
            for task in tasks:
                await run_schedule_task(app, task)

        except Exception as e:
            logger.error(f"Ошибка планировщика: {e}")

async def run_schedule_task(app, task: dict):
    chat_id = task['chat_id']
    system_id = task['system_id']
    task_id = task['id']
    str_id = init_user(chat_id)
    systems = db.get_user_systems(str_id)
    s = systems[0]

    db.update_task_status(task_id, "checking")

    month_name = task['month_name']
    stage_name = task['stage_name']
    stage = next(
        (s for s in treatments_db.get(month_name, {}).get('stages', [])
        if s['name'] == stage_name),
        None
    )

    temp_min = stage.get('temp_min') if stage else None

    sensor_check = db.check_sensor_ok(chat_id, temp_min)

    publish_command(str_id, s['sys_id'], "GET_ALL")
    publish_command(str_id, s['sys_id'], "GET_VETER")
    publish_command(str_id, s['sys_id'], "GET_POT")

    if not sensor_check['ok']:
        reasons = ", ".join(sensor_check['reasons'])
        db.update_task_status(task_id, "skipped")
        db.log_treatment(
            chat_id,
            system_id,
            task['pump_assignment_id'],
            month_name,
            stage_name,
            "skipped",
            format_sensor_snapshot(sensor_check['cash'])
        )
        await app.bot.send_message(
            chat_id,
            f"Обработка пропущена ({stage_name}) \n{reasons}",
        )
        return

    system = db.get_system(system_id)
    duration = cale_pump_duration_sec(chat_id)
    pump_num = task['pump_number']
    cashe = db.get_sensor_cash(chat_id)
    snapshot = format_sensor_snapshot(cashe)

    db.update_task_status(task_id, "running")
    await app.bot.send_message(
        chat_id,
        f"Плановая обработка началась! \nОбработка: {stage_name}\nНасос: {pump_num}\nВремя: {duration} сек"
    )

    publish_command(chat_id, system["sys_id"], f"NASOS:{pump_num}:ON")
    if str_id in user_states:
        user_states[str_id]["pump_states"][pump_num] = True

    await asyncio.sleep(duration)

    publish_command(chat_id, system["sys_id"], f"NASOS:{pump_num}:OFF")

    if str_id in user_states:
        user_states[str_id]["pump_states"][pump_num] = False

    db.update_task_status(task_id, "done")
    db.log_treatment(
        chat_id,
        system_id,
        task['pump_assignment_id'],
        month_name,
        stage_name,
        "success",
        snapshot
    )

    await app.bot.send_message(
        chat_id,
        text = f"Обработка завершена!\nОбработка: {stage_name}"
    )

def main():
    global mqtt_client

    db.init_db()

    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()

    except Exception as e:
        logger.error(f"MQTT ошибка подключения: {e}")

    app = Application.builder().token(TOKEN).build()
    app.add_handler(MessageHandler(filters.PHOTO, cmd_get_file_id))
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_input_handler))
    app.add_handler(MessageHandler(filters.COMMAND & filters.User(user_id=ADMIN_ID), admin_commands))

    async def post_init(application):
        asyncio.create_task(task_scheduler(application))

    app.post_init = post_init

    logger.info("Бот запущен!")
    app.run_polling()

if __name__ == "__main__":
    main()
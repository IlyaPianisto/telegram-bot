import logging
import asyncio
import json
import os
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters, CallbackContext
import paho.mqtt.client as mqtt
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
TREATMENTS_PER_PAGE = 5

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

treatments_db = load_treatments()

# Маппинг месяцев
MONTHS_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель", 5: "Май", 6: "Июнь",
    7: "Июль", 8: "Август", 9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
}

MONTHS_LIST = ["Январь", "Февраль", "март", "Апрель", "Май", "Июнь", "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"]

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

def cale_pump_duration_sec(chat_id: str) -> int:
    user = db.get_or_create_user(chat_id)
    volume = user.get('bootle_volume_l', db.DEFAULTS['bootle_volume_l'])
    flow_rate = user.get('pump_flow_rate', db.DEFAULTS['pump_flow_rate'])
    return max(1, round(volume / flow_rate))

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
        rows.append([InlineKeyboardButton(f"Насос {i}", callback_data=f"{purpose}:pump:{system_id}:{i}")])
        if len(row) == 2:
            rows.append(row)
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

def kb_delete_confirm (system_id: int, page: int) -> InlineKeyboardMarkup:
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

    return InlineKeyboardMarkup(rows)

def kb_treatment_menu () -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Запланировать", callback_data="treat:plan:0")],
        [InlineKeyboardButton("Отложенные", callback_data="treat:list:pending")],
        [InlineKeyboardButton("В процессе", callback_data="treat:list:running")],
        [InlineKeyboardButton("<- Назад", callback_data="menu:main")],
    ])

def kb_task_list(tasks: list, status: str) -> InlineKeyboardMarkup:
    rows = []

    for task in tasks:
        label = f"" ### peredelat
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
    stages = treatments_db.get(month_id, {}).get('stages', [])
    start = page * TREES_PER_PAGE
    end = start + TREES_PER_PAGE
    page_stages = stages[start:end]

    rows = []
    for i, stage in enumerate(page_stages):
        rows.append([InlineKeyboardButton(stage['name'], callback_data=f"treat:stage:{month_id}:{start+i}")]) #peredelat

    nav = []

    if page > 0:
        nav.append(InlineKeyboardButton("<<", callback_data=f"treat:plan:{month_id}:{page - 1}"))
    if end < 11:
        nav.append(InlineKeyboardButton(">>", callback_data=f"treat:plan:{month_id}:{page + 1}"))
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
            db.rename_system(system_id, str_id, page)
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

    elif waiting == "calib:bootle":
        try:
            val = float(text)
            if val <= 0:
                raise ValueError
            db.update_user_settings(str_id, "bootle_volume_l", val)
            state['awaiting_input'] = None
            await update.message.reply_text(f"Объём ёмкости сохранён! {val} Л.", reply_markup=kb_calibration_menu(str_id))

        except ValueError:
            await update.message.reply_text("ERROR! Введите положительное число (л):")

    else:
        await update.message.reply_text("Используйте кнопки меню!", reply_markup=kb_main_menu())


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    str_id = init_user(update.effective_chat.id)
    state = user_states[str_id]

    if data == 'menu:main':
        await query.edit_message_text("Главное меню:", reply_markup=kb_main_menu())

    elif data == "menu:settings":
        await query.edit_message_text("Настройки", reply_markup=kb_settings_menu())

    elif data == "menu:system_menu":
        await query.edit_message_text("Управление системами. Выберите систему!", reply_markup=kb_system_menu(str_id, 0))

    elif data.startswith("sys:page"):
        page = int(data.split(":")[2])
        await query.edit_message_reply_markup(reply_markup=kb_system_menu(str_id, page))

    elif data.startswith("sys:select:"):
        parts = data.split(":")
        system_id = int(parts[2])
        page = int(parts[3])
        system = db.get_system(system_id)
        await query.edit_message_reply_markup(f"Выбрана система №{system['sys_id']}  \"{system['name']}\"", reply_markup=kb_system_menu(str_id, page))

    elif data.startswith("sys:rename:"):
        parts = data.split(":")
        system_id = int(parts[2])
        page = int(parts[3])
        state['awaiting_input'] = 'sys:rename'
        state['pending_sys'] = {'system_id': system_id, 'page': page}
        await query.edit_message_reply_markup("Введите новое название системы:" ,reply_markup=kb_system_menu(str_id, page))

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
        await query.edit_message_reply_markup("Напишите название для вашей системы:")

    elif data == "menu:treatment":
        await query.edit_message_text("Обработка деревьев", reply_markup=kb_treatment_menu())

    elif data.startswith("treat:list:"):
        status = data.split(":")[2]
        tasks = db.get_user_tasks(str_id, status=status)

        label_map = {"pending": "Отложенные", "running": "В процессе"}
        if not tasks:
            await query.edit_message_text(
                f"{label_map(status)}\nОбработок нет.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("<- Назад", reply_markup="menu:treatment")]]))
            return
        await query.edit_message_text(f"{label_map[status]}\nОбработки", reply_markup=kb_task_list(tasks, status))

    elif data.startswith("treat:task:"):
        parts = data.split(":")
        task_id = int(parts[2])
        status = parts[3]
        tasks = db.get_user_tasks(str_id, status=status)
        task = next((t for t in tasks if t['id'] == task_id), None)
        if not task:
            await query.edit_message_text("Задача не найдена", reply_markup=kb_treatment_menu())
            return
        await query.edit_message_text(
            f"{task['tree_name']}\n"
            f"Этап: {task['stage_name']}\n"
            f"Система: {task['system_name']}\n"
            f"Насос: {task['pump_number']}\n"
            f"Время: {task['scheduled_time']}"
            f"Статус: {task['status']}",
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
            await query.edit_message_text(f"Обработка отменена\n{label_map[status]}\nОбработок нет", reply_markup=InlineKeyboardMarkup([InlineKeyboardButton("<- Назад", callback_data="menu:treatment")]))
            return
        await query.edit_message_text(f"Обработка отменена. \n\n{label_map[status]}\nОбработки:", reply_markup=kb_task_list(tasks, status))

    elif data.startswith("treat:plan:"):
        month_id = int(data.split(":")[2])
        month_name = MONTHS_LIST[month_id]
        stages = treatments_db.get(month_name,{}).get('stages', [])
        await query.edit_message_text(
            f"Месяц: {month_name}\n"
            f"Доступные обработки: {len(stages)}", reply_markup=kb_plan_stage(month_id, 0)
        )

    elif data.startswith("treat:plan_page:"):
        parts = data.split(":")
        month_id = int(data.split(":")[2])
        page = int(parts[3])
        await query.edit_message_reply_markup(reply_markup=kb_plan_stage(month_id, page))

    elif data.startswith("treat:stage:"): # его если чё менять потом.
        parts = data.split(":")
        month_id = int(parts[2])
        stage_id = int(parts[3])
        month_name = MONTHS_LIST[month_id]
        stages = treatments_db.get(month_name,{}).get('stages', [])
        if stage_id >= len(stages):
            await query.edit_message_text("Обработка не найдена.", reply_markup=kb_treatment_menu())
            return

        stage = stages[stage_id]
        text = f"{month_name}:{stage['name']}\n"
        if stage.get("condition"):
            text += f"{stage['condition']}"

        for tree_name, info in stage.get('trees', {}).items():
            text += f"{tree_name}:{info.get('mixture', '-')}\n"
        if stage.get("temperature") is not None:
            text += f"{stage['temperature']}"

        state['pending_treatments'] = {
            "month_name": month_name,
            "month_id": month_id,
            "stage_id": stage_id,
        }

        rows = [
            [InlineKeyboardButton("Готово!", callback_data=f"treat:confirm:{month_id}:{stage_id}")],
            [InlineKeyboardButton("<- Назад", callback_data=f"treat:plan:{month_id}")],
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(rows))

    elif data.startswith("treat:confirm:"):
        parts = data.split(":")
        month_id = int(parts[2])
        stage_id = int(parts[3])
        month_name = MONTHS_LIST[month_id]
        stage = treatments_db.get(month_name,{}).get('stages', [])[stage_id]
        trees_text = ','.join(stage.get("trees", {}).keys()) or "-"

        await query.edit_message_text(
            f"Подтверждение \n\n"
            f"Месяц: {month_name}\n"
            f"Этап: {stage['name']}\n"
            f"Деревья: {trees_text}\n\n"
            "Приготовьте раствор и залейте в ёмкости. Затем выберите системы для обработки",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Выбрать систему", callback_data="treat:chose_system")],
                [InlineKeyboardButton("<- Назад", callback_data=f"treat:stage:{month_id}:{stage_id}")],
            ])
        )

    elif data == "treat:chose_system":
        systems = db.get_user_systems(str_id)
        if not systems:
            await query.edit_message_text('Нет привязанных систем', reply_markup=kb_treatment_menu())
            return
        await query.edit_message_text("Выберите систему:", reply_markup=kb_choose_system_for(str_id, "treatment", 0))

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

        rows.append([InlineKeyboardButton("<- Назад", callback_data=f"treat:chose_system")])
        await query.edit_message_text(f"Система \"{system['name']}\"\nВыберите насос:", reply_markup=InlineKeyboardMarkup(rows))

    elif data.startswith("treat:pump"):
        parts = data.split(":")
        system_id = int(parts[2])
        pump_assignment_id = int(parts[3])
        pending = state.get('pending_treatment', {})
        month_name = pending.get('month_name')
        stage_id = pending.get('stage_id', 0)

        if not month_name:
            await query.edit_message_text("ERROR! Начините выбор заново.", reply_markup=kb_treatment_menu())
            return

        stage = treatments_db.get(month_name,{}).get('stages', [])[stage_id]
        stage_name = stage['name']
        pumps = db.get_system_pumps(system_id)
        pump = next((p for p in pumps if p['id'] == pump_assignment_id), None)

        db.add_schedule_task(
            str_id,
            system_id,
            pump_assignment_id,
            month_name,
            stage_name,
            "22:00"
        )

        state['pending_treatment'] = None
        tree_name = pump['tree_name'] if pump else '-'
        pump_num = pump['pump_number'] if pump else '-'

        await query.edit_message_text(
            f"Обработка запланирована на 22:00\nНасос {pump_num} - {tree_name}\nЭтап: {stage_name}", reply_markup=kb_treatment_menu()
        )



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
        f"Плановая обработка началась! \nЭтап: {stage_name}\nНасос: {pump_num}\nВремя: {duration} сек"
    )

    publish_command(chat_id, system["sys_id"], f"NASOS:{pump_num}:1")
    if str_id in user_states:
        user_states[str_id]["pump_states"][pump_num] = True

        await asyncio.sleep(duration)

        publish_command(chat_id, system["sys_id"], f"NASOS:{pump_num}:0")

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
        text = f"Обработка завершена!\nЭтап: {stage_name}"
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

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_input_handler))

    async def post_init(application):
        asyncio.create_task(task_scheduler(application))

    app.post_init = post_init

    logger.info("Бот запущен!")
    app.run_polling()

if __name__ == "__main__":
    main()

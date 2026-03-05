import logging
import asyncio
import json
import os
from dotenv import load_dotenv
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
import paho.mqtt.client as mqtt
 
#---++++++++
#++++++++A+A+A
# --- НАСТРОЙКИ ---
load_dotenv()
TOKEN = os.getenv("TOKEN")
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT"))
MQTT_TOPIC_SUBSCRIBE = os.getenv("MQTT_TOPIC_SUBSCRIBE")

# Файлы
CONFIG_FILE = "user_config.json"
TREATMENTS_FILE = "treatments.json"

# Логирование
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# --- ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ И СОСТОЯНИЯ ---
global_bot_instance = None
mqtt_client = None

# Структура user_states:
# {
#   chat_id: {
#       "sys_id": "1",
#       "pump_states": {1: False, ...},
#       "sensors": {"wind": 0, "light": 0, "bme": "..."},
#       "awaiting_input": None | "sys_id",
#       "calibrating": None | "wind_zero" | "light_day"
#   }
# }
user_states = {}

# Маппинг месяцев
MONTHS_RU = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель", 5: "Май", 6: "Июнь",
    7: "Июль", 8: "Август", 9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
}

# --- РАБОТА С ФАЙЛАМИ ---

def load_json(filename, default_data):
    if not os.path.exists(filename):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(default_data, f, ensure_ascii=False, indent=4)
        return default_data
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading {filename}: {e}")
        return default_data

def save_json(filename, data):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

# Загрузка конфигов (калибровка и привязка деревьев хранится здесь)
user_config = load_json(CONFIG_FILE, {}) 
treatments_db = load_json(TREATMENTS_FILE, {})

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def init_user(chat_id):
    str_id = str(chat_id)
    if str_id not in user_states:
        user_states[str_id] = {
            "sys_id": "1", 
            "pump_states": {i: False for i in range(1, 9)},
            "sensors": {"wind": "Нет данных", "light": "Нет данных", "bme": "Нет данных"},
            "awaiting_input": None,
            "calibrating": None
        }
    # Подгружаем сохраненный системный ID если есть
    if str_id in user_config and "sys_id" in user_config[str_id]:
        user_states[str_id]["sys_id"] = user_config[str_id]["sys_id"]
    return str_id

def get_calibration(chat_id, sensor_type):
    return user_config.get(str(chat_id), {}).get("calibration", {}).get(sensor_type, 0)

def save_calibration(chat_id, sensor_type, value):
    str_id = str(chat_id)
    if str_id not in user_config: user_config[str_id] = {}
    if "calibration" not in user_config[str_id]: user_config[str_id]["calibration"] = {}
    
    user_config[str_id]["calibration"][sensor_type] = value
    save_json(CONFIG_FILE, user_config)

# --- MQTT ЛОГИКА ---

def publish_command(chat_id, command):
    str_id = init_user(chat_id)
    sys_id = user_states[str_id]["sys_id"]
    topic = f"app/{chat_id}/{sys_id}/control"
    if mqtt_client:
        mqtt_client.publish(topic, command)
        logger.info(f"MQTT OUT {topic}: {command}")

def on_connect(client, userdata, flags, rc, properties=None):
    logger.info("Connected to MQTT")
    client.subscribe(MQTT_TOPIC_SUBSCRIBE)

def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode()
        topic = msg.topic.split('/')
        owner_id = topic[1]
        
        # Обновляем кэш датчиков
        if owner_id in user_states:
            if payload.startswith("VETER:"):
                val = payload.split(":")[1]
                user_states[owner_id]["sensors"]["wind"] = val
                
                # Если сейчас идет калибровка ветра
                if user_states[owner_id].get("calibrating") == "wind":
                    save_calibration(owner_id, "wind_zero", val)
                    user_states[owner_id]["calibrating"] = None
                    # Отправляем уведомление (нужна ссылка на loop, но упростим через статус)

            elif payload.startswith("POT:"):
                val = payload.split(":")[1]
                user_states[owner_id]["sensors"]["light"] = val
                
                # Если калибровка света
                if user_states[owner_id].get("calibrating") == "light":
                    save_calibration(owner_id, "light_ref", val)
                    user_states[owner_id]["calibrating"] = None

            elif payload.startswith("BME:"):
                user_states[owner_id]["sensors"]["bme"] = payload # Храним сырую строку для парсинга

            elif payload.startswith("NASOS:"):
                parts = payload.split(":")
                if len(parts) >= 3:
                    p_num = int(parts[1])
                    state = (parts[2] == "ON")
                    user_states[owner_id]["pump_states"][p_num] = state

    except Exception as e:
        logger.error(f"MQTT Parse error: {e}")

# --- КЛАВИАТУРЫ (MENUS) ---

def get_main_menu():
    kb = [
        [InlineKeyboardButton("🌳 Обработка деревьев (Авто)", callback_data="menu:treatment_auto")],
        [InlineKeyboardButton("⚙️ Настройки", callback_data="menu:settings")]
    ]
    return InlineKeyboardMarkup(kb)

def get_settings_menu():
    kb = [
        [InlineKeyboardButton("🐞 Отладка", callback_data="menu:debug")],
        [InlineKeyboardButton("📝 Конфигурация деревьев", callback_data="menu:config_trees")],
        [InlineKeyboardButton("🔗 Привязать/Отвязать", callback_data="menu:binding")],
        [InlineKeyboardButton("🔙 В главное меню", callback_data="menu:main")]
    ]
    return InlineKeyboardMarkup(kb)

def get_debug_menu(chat_id):
    sys_id = user_states[str(chat_id)]["sys_id"]
    kb = [
        [InlineKeyboardButton(f"🆔 Ввести № системы (Текущий: {sys_id})", callback_data="debug:set_sys_id")],
        [InlineKeyboardButton("🚰 Вкл/Выкл Насос (1-8)", callback_data="menu:debug_pumps")],
        [InlineKeyboardButton("👀 Показания датчиков (ВСЕ)", callback_data="debug:sensors_all")],
        [InlineKeyboardButton("⚖️ Калибровка датчиков", callback_data="menu:calibration")],
        [InlineKeyboardButton("🔙 Назад", callback_data="menu:settings")]
    ]
    return InlineKeyboardMarkup(kb)

def get_calibration_menu():
    kb = [
        [InlineKeyboardButton("💨 Калибровать Анемометр (Ветер)", callback_data="calib:wind")],
        [InlineKeyboardButton("☀️ Калибровать Фоторезистор (Свет)", callback_data="calib:light")],
        [InlineKeyboardButton("🔙 Назад", callback_data="menu:debug")]
    ]
    return InlineKeyboardMarkup(kb)

def get_pumps_menu(chat_id):
    str_id = str(chat_id)
    kb = []
    row = []
    for i in range(1, 9):
        state = user_states[str_id]["pump_states"][i]
        icon = "🟢" if state else "🔴"
        row.append(InlineKeyboardButton(f"Насос {i} {icon}", callback_data=f"pump:toggle:{i}"))
        if len(row) == 2:
            kb.append(row)
            row = []
    if row: kb.append(row)
    kb.append([InlineKeyboardButton("🔙 Назад", callback_data="menu:debug")])
    return InlineKeyboardMarkup(kb)

# --- ОБРАБОТЧИКИ СООБЩЕНИЙ ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    init_user(update.effective_chat.id)
    await update.message.reply_text("🌱 Система управления садом.", reply_markup=get_main_menu())

async def text_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод текста (например, номер системы)"""
    chat_id = str(update.effective_chat.id)
    init_user(chat_id)
    
    state = user_states[chat_id].get("awaiting_input")
    
    if state == "sys_id":
        new_sys_id = update.message.text.strip()
        if new_sys_id.isdigit():
            user_states[chat_id]["sys_id"] = new_sys_id
            user_states[chat_id]["awaiting_input"] = None
            
            # Сохраняем в конфиг
            if chat_id not in user_config: user_config[chat_id] = {}
            user_config[chat_id]["sys_id"] = new_sys_id
            save_json(CONFIG_FILE, user_config)
            
            await update.message.reply_text(f"✅ Система переключена на №{new_sys_id}", reply_markup=get_debug_menu(chat_id))
        else:
            await update.message.reply_text("⚠️ Ошибка: Номер системы должен быть числом.")
    
    elif state == "treatment_time":
        # Логика ввода времени (если потребуется по схеме)
        pass

# --- ОБРАБОТЧИКИ КНОПОК ---

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    
    data = query.data
    chat_id = update.effective_chat.id
    str_id = init_user(chat_id)
    
    # 1. НАВИГАЦИЯ
    if data == "menu:main":
        await query.edit_message_text("Главное меню:", reply_markup=get_main_menu())
        
    elif data == "menu:settings":
        await query.edit_message_text("⚙️ Настройки:", reply_markup=get_settings_menu())
        
    elif data == "menu:debug":
        await query.edit_message_text("🐞 Меню отладки:", reply_markup=get_debug_menu(chat_id))
        
    elif data == "menu:debug_pumps":
        await query.edit_message_text("🚰 Нажмите для переключения:", reply_markup=get_pumps_menu(chat_id))
        
    elif data == "menu:calibration":
        await query.edit_message_text("⚖️ Выберите датчик для калибровки:", reply_markup=get_calibration_menu())

    # 2. ВВОД НОМЕРА СИСТЕМЫ
    elif data == "debug:set_sys_id":
        user_states[str_id]["awaiting_input"] = "sys_id"
        await query.edit_message_text("⌨️ Введите новый номер системы цифрами (например, 1):", reply_markup=None)

    # 3. ПОКАЗАНИЯ ДАТЧИКОВ (ВСЕХ)
    elif data == "debug:sensors_all":
        # Отправляем команды запроса данных
        publish_command(chat_id, "GET_ALL")   # Погода
        publish_command(chat_id, "GET_VETER") # Ветер
        publish_command(chat_id, "GET_POT")   # Свет
        
        await query.edit_message_text("⏳ Запрашиваю данные со всех датчиков...", reply_markup=get_debug_menu(chat_id))
        
        # Небольшая пауза и обновление (эмуляция риал-тайма)
        await asyncio.sleep(2)
        
        s = user_states[str_id]["sensors"]
        cal = user_config.get(str_id, {}).get("calibration", {})
        
        # Расчет с учетом калибровки
        wind_raw = s['wind']
        wind_cal = "Не откалибровано"
        if wind_raw != "Нет данных":
            try:
                # Пример простой логики: Текущее - Калибровка
                val = float(wind_raw)
                zero = float(cal.get("wind_zero", 0))
                wind_cal = f"{val - zero:.1f}"
            except: pass

        text = (
            f"📊 <b>Показания системы {user_states[str_id]['sys_id']}</b>\n\n"
            f"🌡 <b>BME (Погода):</b> {s['bme']}\n"
            f"💨 <b>Ветер:</b> {s['wind']} (С учетом калибровки: {wind_cal})\n"
            f"☀️ <b>Свет:</b> {s['light']}\n"
        )
        await query.message.reply_text(text, parse_mode="HTML")

    # 4. КАЛИБРОВКА
    elif data.startswith("calib:"):
        sensor = data.split(":")[1]
        if sensor == "wind":
            user_states[str_id]["calibrating"] = "wind"
            publish_command(chat_id, "GET_VETER")
            await query.edit_message_text("🌬 Замеряю 'Нулевой ветер'... Подождите.")
        elif sensor == "light":
            user_states[str_id]["calibrating"] = "light"
            publish_command(chat_id, "GET_POT")
            await query.edit_message_text("☀️ Замеряю текущую освещенность как эталон... Подождите.")
            
        # Ждем немного, пока придет MQTT ответ (он обработается в on_message и сбросит флаг)
        await asyncio.sleep(3)
        if user_states[str_id]["calibrating"] is None:
             await query.message.reply_text("✅ Калибровка успешно записана!", reply_markup=get_calibration_menu())
        else:
             user_states[str_id]["calibrating"] = None
             await query.message.reply_text("❌ Нет ответа от датчика. Попробуйте еще раз.", reply_markup=get_calibration_menu())

    # 5. УПРАВЛЕНИЕ НАСОСАМИ
    elif data.startswith("pump:toggle:"):
        p_num = int(data.split(":")[2])
        curr = user_states[str_id]["pump_states"][p_num]
        action = "OFF" if curr else "ON"
        
        publish_command(chat_id, f"NASOS:{p_num}:{action}")
        
        # Оптимистичное обновление UI
        user_states[str_id]["pump_states"][p_num] = not curr
        try:
            await query.edit_message_reply_markup(reply_markup=get_pumps_menu(chat_id))
        except: pass

    # 6. АВТОМАТИЧЕСКАЯ ОБРАБОТКА (ИСПРАВЛЕНИЕ П.1)
    elif data == "menu:treatment_auto":
        now = datetime.now()
        month_name = MONTHS_RU.get(now.month)
        
        # Для теста можно жестко задать месяц, если сейчас зима:
        # month_name = "Апрель" 
        
        if month_name in treatments_db:
            data_month = treatments_db[month_name]
            # Берем первый этап (или можно сделать меню выбора этапа)
            stages = data_month.get("stages", [])
            
            kb = []
            for i, stage in enumerate(stages):
                 kb.append([InlineKeyboardButton(f"{stage['name']}", callback_data=f"treat:show:{month_name}:{i}")])
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="menu:main")])
            
            await query.edit_message_text(f"📅 <b>Сейчас {month_name}</b>\nНайдено обработок: {len(stages)}", 
                                          parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))
        else:
            await query.edit_message_text(f"📅 Сейчас {month_name}.\n📭 Для этого месяца нет запланированных обработок в базе.", 
                                          reply_markup=get_main_menu())

    # 7. ПОКАЗ ИНСТРУКЦИИ
    elif data.startswith("treat:show:"):
        parts = data.split(":")
        m_name = parts[2]
        s_idx = int(parts[3])
        
        stage = treatments_db[m_name]["stages"][s_idx]
        text = f"🧪 <b>{m_name}: {stage['name']}</b>\n"
        text += f"Условие: {stage['condition']}\n\n"
        
        for tree, info in stage["trees"].items():
            text += f"🌳 <b>{tree}:</b>\n{info['mixture']}\n"
            
        kb = [[InlineKeyboardButton("🚀 Начать выполнение", callback_data="treat:exec")],
              [InlineKeyboardButton("🔙 Назад", callback_data="menu:treatment_auto")]]
        
        await query.edit_message_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))

# --- ЗАПУСК ---

def main():
    global mqtt_client
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()
    except Exception as e:
        logger.error(f"MQTT connection failed: {e}")

    app = Application.builder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    # Хендлер для текстового ввода (номер системы)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_input_handler))

    print("Бот запущен. Ожидание команд...")
    app.run_polling()

if __name__ == "__main__":
    main()
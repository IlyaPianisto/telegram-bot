#include <Wire.h>
#include <SoftwareSerial.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_BME680.h>


SoftwareSerial espSerial(12, 13); // Пин 10 = RX (к ESP D6), Пин 11 = TX (к ESP D5)

#define Nasos_1 11
#define Nasos_2 10
#define Nasos_3 9
#define Nasos_4 8
#define Nasos_5 7
#define Nasos_6 6
#define Nasos_7 5 
#define Nasos_8 4 
#define POT_PIN A0
#define Veter_PIN 2

Adafruit_BME680 bme;

volatile unsigned long veterPulseCount = 0;
unsigned long lastVeterCheck = 0;

// Этот фактор нужно откалибровать!
// Он переводит Гц (импульсы/сек) в м/с
// Сейчас стоит 1.0 (1 Гц = 1 м/с)
const float VETER_FACTOR = 0.01884; 

void countVeterPulse() {
  veterPulseCount++;
}

void setup() {

  Serial.begin(9600);
  Serial.println("Nano-Helper запущен.");
  espSerial.begin(9600);

  if (!bme.begin()) {
    Serial.println("Ошибка BME680! Проверьте подключение.");
  } else {
    Serial.println("BME680 инициализирован.");
    bme.setTemperatureOversampling(BME680_OS_8X);
    bme.setHumidityOversampling(BME680_OS_2X);
    bme.setPressureOversampling(BME680_OS_4X);
    bme.setIIRFilterSize(BME680_FILTER_SIZE_3);
    bme.setGasHeater(320, 150);
  }
  
  pinMode(POT_PIN, INPUT);

  pinMode(Nasos_1, OUTPUT); digitalWrite(Nasos_1, HIGH);
  pinMode(Nasos_2, OUTPUT); digitalWrite(Nasos_2, HIGH);
  pinMode(Nasos_3, OUTPUT); digitalWrite(Nasos_3, HIGH);
  pinMode(Nasos_4, OUTPUT); digitalWrite(Nasos_4, HIGH);
  pinMode(Nasos_5, OUTPUT); digitalWrite(Nasos_5, HIGH);
  pinMode(Nasos_6, OUTPUT); digitalWrite(Nasos_6, HIGH);
  pinMode(Nasos_7, OUTPUT); digitalWrite(Nasos_7, HIGH); 
  pinMode(Nasos_8, OUTPUT); digitalWrite(Nasos_8, HIGH);

  pinMode(Veter_PIN, INPUT_PULLUP);
  attachInterrupt(digitalPinToInterrupt(Veter_PIN), countVeterPulse, RISING);
  lastVeterCheck = millis();
}

void loop() {
  checkESPCommands();
}

void checkESPCommands() {
  if (espSerial.available() > 0) {
    String command = espSerial.readStringUntil('\n');
    command.trim();
    Serial.print("Получена команда от ESP: ");
    Serial.println(command);

    if (command == "GET_ALL") {
      readAllBMEData();
    }
    else if (command == "GET_POT") {
      readResistor();
    }
    else if (command == "GET_VETER") {
      readVeter();
    }
    // Ожидаем команду в формате "NASOS:Номер:Состояние"
    // Например: "NASOS:1:ON" или "NASOS:3:OFF"
    else if (command.startsWith("NASOS:")) {
      int firstColon = command.indexOf(':');      // Индекс 1-го ':' (в позиции 5)
      int secondColon = command.indexOf(':', firstColon + 1); // Индекс 2-го ':' (в позиции 7)

      if (firstColon != -1 && secondColon != -1) {
        String numStr = command.substring(firstColon + 1, secondColon);
        int pumpNum = numStr.toInt();
        String stateStr = command.substring(secondColon + 1);

        controlNasos(pumpNum, stateStr);
      }
    }
  }
}

void controlNasos(int num, String state) {
  int pinToControl = -1;

  switch (num) {
    case 1: pinToControl = Nasos_1; break;
    case 2: pinToControl = Nasos_2; break;
    case 3: pinToControl = Nasos_3; break;
    case 4: pinToControl = Nasos_4; break;
    case 5: pinToControl = Nasos_5; break;
    case 6: pinToControl = Nasos_6; break;
    case 7: pinToControl = Nasos_7; break;
    case 8: pinToControl = Nasos_8; break;
    default:
      Serial.println("Неверный номер насоса!");
      espSerial.println("NASOS:Error:InvalidNumber");
      return;
  }

  if (state == "ON") {
    digitalWrite(pinToControl, LOW);
    Serial.print("Насос "); Serial.print(num); Serial.println(" ВКЛЮЧЕН");
    espSerial.println("NASOS:" + String(num) + ":ON");
  } else if (state == "OFF") {
    digitalWrite(pinToControl, HIGH);
    Serial.print("Насос "); Serial.print(num); Serial.println(" ВЫКЛЮЧЕН");
    espSerial.println("NASOS:" + String(num) + ":OFF");
  } else {
    Serial.println("Неверная команда состояния (нужно ON или OFF)");
    espSerial.println("NASOS:Error:InvalidState");
  }
}

void readVeter() {
  unsigned long duration = millis() - lastVeterCheck;
  noInterrupts();
  unsigned long pulseCount = veterPulseCount;
  veterPulseCount = 0;
  interrupts();
  lastVeterCheck = millis();

  // Считаем частоту (Гц = импульсы в секунду)
  // (float)pulseCount / (duration / 1000.0)
  float frequency = (float)pulseCount / (duration / 500.0);
  float windSpeed = frequency * VETER_FACTOR;

  Serial.print("Скорость ветра (м/с): ");
  Serial.println(windSpeed, 6);
  espSerial.println("VETER:" + String(windSpeed));
}

void readAllBMEData() {
  if (!bme.performReading()) {
    Serial.println("Ошибка чтения BME680!");
    espSerial.println("BME:Error");
    return;
  }
  
  float tempC = bme.temperature;
  float humidity = bme.humidity;
  float pressure = bme.pressure / 100.0F;
  
  Serial.print("Температура: "); Serial.print(tempC); Serial.println(" *C");
  Serial.print("Влажность: "); Serial.print(humidity); Serial.println(" %");
  Serial.print("Давление: "); Serial.print(pressure); Serial.println(" hPa");
  String response = "BME:T:" + String(tempC) + "|H:" + String(humidity) + "|P:" + String(pressure);
  espSerial.println(response);
}

void readResistor() {
  int potValue = analogRead(POT_PIN);
  Serial.print("Значение фоторезистора: ");
  Serial.println(potValue);
  espSerial.println("POT:" + String(potValue));
}
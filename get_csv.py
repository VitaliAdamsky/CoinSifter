import requests
import os
import time
from dotenv import load_dotenv

# --- Настройки ---
load_dotenv()

SECRET_TOKEN = os.getenv("SECRET_TOKEN")
COIN_SIFTER_API = os.getenv("COIN_SIFTER_API")

if not SECRET_TOKEN:
    raise ValueError("❌ SECRET_TOKEN не найден в .env файле")
if not COIN_SIFTER_API:
    raise ValueError("❌ COIN_SIFTER_API не найден в .env файле")

# Убедимся, что URL начинается с https://
if not COIN_SIFTER_API.startswith(("http://", "https://")):
    COIN_SIFTER_API = "https://" + COIN_SIFTER_API

url = f"{COIN_SIFTER_API}/coins/filtered/csv"
headers = {"X-Auth-Token": SECRET_TOKEN}

# --- Функция для "пробуждения" сервера (опционально) ---
def wake_up_server():
    """Делает лёгкий запрос к /docs, чтобы разбудить Render-сервер."""
    try:
        health_url = f"{COIN_SIFTER_API}/docs"
        print("💤 Сервер может быть в спящем режиме. Пробуждение...")
        requests.get(health_url, timeout=5)
    except Exception:
        pass  # Игнорируем ошибки — это лишь попытка

# --- Основной запрос ---
try:
    print(f"📡 Запрос CSV: {url}")
    print("⏳ Ожидание ответа (макс. 120 сек)...")

    # Пробуем "разбудить" сервер, если это Render
    if "render.com" in COIN_SIFTER_API:
        wake_up_server()
        time.sleep(5)  # Даём время на запуск

    response = requests.get(url, headers=headers, timeout=120)
    response.raise_for_status()

    # Сохраняем CSV
    with open("filtered_coins.csv", "w", encoding="utf-8") as f:
        f.write(response.text)

    print("✅ Файл успешно сохранён: filtered_coins.csv")

except requests.Timeout:
    print("❌ Таймаут: сервер не ответил за 120 секунд.")
    print("   → На бесплатном Render это нормально. Попробуйте ещё раз через 30–60 сек.")
except requests.ConnectionError:
    print("❌ Не удаётся подключиться к серверу. Проверьте URL и интернет.")
except requests.HTTPError as e:
    print(f"❌ HTTP ошибка: {e.response.status_code}")
    print(f"   Ответ сервера: {e.response.text[:200]}")
except Exception as e:
    print(f"❌ Неизвестная ошибка: {e}")
import requests
import time
import os
from datetime import datetime
from dotenv import load_dotenv

# Загружаем переменные из .env
load_dotenv()

# Получаем базовый URL
BASE_URL = os.getenv("COIN_SIFTER_URL")
if not BASE_URL:
    raise ValueError("❌ Переменная COIN_SIFTER_URL не найдена в .env файле")

# Формируем полный URL health-эндпоинта
HEALTH_URL = f"{BASE_URL.rstrip('/')}/health"

def check_health():
    try:
        response = requests.get(HEALTH_URL, timeout=10)
        status_code = response.status_code
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        if status_code == 200:
            try:
                data = response.json()
                print(f"[{timestamp}] ✅ Сервер в порядке | Ответ: {data}")
            except ValueError:
                print(f"[{timestamp}] ✅ Сервер в порядке | Ответ: {response.text[:100]}")
        else:
            print(f"[{timestamp}] ❌ Ошибка | HTTP {status_code} | Тело: {response.text[:100]}")
            
    except requests.exceptions.Timeout:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] ⏱️ Таймаут: сервер не ответил за 10 секунд")
        
    except requests.exceptions.ConnectionError:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] 🌐 Ошибка подключения: проверьте интернет или URL")
        
    except Exception as e:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] 💥 Неизвестная ошибка: {e}")

if __name__ == "__main__":
    print("🚀 Запуск мониторинга health-эндпоинта...")
    print(f"📍 URL: {HEALTH_URL}")
    print("🔁 Проверка каждые 5 минут (300 секунд)\n")
    
    while True:
        check_health()
        time.sleep(300)  # 5 минут
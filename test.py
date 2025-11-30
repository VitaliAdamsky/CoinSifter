# get_counts.py

import requests
import os
import sys
from dotenv import load_dotenv

def fetch_counts():
    """
    Загружает переменные окружения, выполняет запросы к двум эндпоинтам
    и выводит количество монет из каждого.
    """
    
    # --- 1. Загрузка конфигурации ---
    print("ℹ️ Загрузка переменных из .env файла...")
    load_dotenv()

    SECRET_TOKEN = os.getenv("SECRET_TOKEN")
    BASE_URL = os.getenv("COIN_SIFTER_URL")

    if not SECRET_TOKEN:
        print("❌ ОШИБКА: SECRET_TOKEN не найден в .env файле.", file=sys.stderr)
        sys.exit(1)
    if not BASE_URL:
        print("❌ ОШИБКА: COIN_SIFTER_API не найден в .env файле.", file=sys.stderr)
        sys.exit(1)

    # Убедимся, что URL корректный
    if not BASE_URL.startswith(("http://", "https://")):
        BASE_URL = "https://" + BASE_URL

    headers = {"X-Auth-Token": SECRET_TOKEN}
    print(f"✅ Конфигурация загружена. Цель: {BASE_URL}\n")

    # --- 2. Эндпоинты для проверки ---
    endpoints_to_check = {
        "filtered": f"{BASE_URL}/coins/filtered",
        "formatted": f"{BASE_URL}/coins/formatted-symbols"
    }

    results = {}

    # --- 3. Выполнение запросов ---
    try:
        # --- Запрос 1: /coins/filtered ---
        url_filtered = endpoints_to_check["filtered"]
        print(f"📡 Запрос 1: {url_filtered}")
        
        response_filtered = requests.get(url_filtered, headers=headers, timeout=20)
        response_filtered.raise_for_status() # Проверка на HTTP ошибки
        
        data_filtered = response_filtered.json()
        count_filtered = data_filtered.get('count')
        results["filtered"] = count_filtered
        print(f"✅ Успех. Ключ 'count': {count_filtered}\n")


        # --- Запрос 2: /coins/formatted-symbols ---
        url_formatted = endpoints_to_check["formatted"]
        print(f"📡 Запрос 2: {url_formatted}")
        
        response_formatted = requests.get(url_formatted, headers=headers, timeout=20)
        response_formatted.raise_for_status() # Проверка на HTTP ошибки
        
        data_formatted = response_formatted.json()
        count_formatted = data_formatted.get('count')
        results["formatted"] = count_formatted
        print(f"✅ Успех. Ключ 'count': {count_formatted}\n")

    except requests.Timeout:
        print("❌ ОШИБКА: Таймаут запроса. Сервер не ответил вовремя.", file=sys.stderr)
        sys.exit(1)
    except requests.ConnectionError:
        print(f"❌ ОШИБКА: Не удалось подключиться к {BASE_URL}.", file=sys.stderr)
        sys.exit(1)
    except requests.HTTPError as e:
        print(f"❌ ОШИБКА HTTP: {e.response.status_code} {e.response.reason}", file=sys.stderr)
        try:
            print(f"   Ответ сервера: {e.response.json()}", file=sys.stderr)
        except requests.JSONDecodeError:
            print(f"   Ответ сервера: {e.response.text[:200]}...", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"❌ Неизвестная ошибка: {e}", file=sys.stderr)
        sys.exit(1)


    # --- 4. Итог ---
    print("---" * 10)
    print("📊 ИТОГОВЫЙ РЕЗУЛЬТАТ:")
    print(f"  Кол-во монет в /coins/filtered:          {results.get('filtered', 'N/A')}")
    print(f"  Кол-во монет в /coins/formatted-symbols: {results.get('formatted', 'N/A')}")
    print("---" * 10)

if __name__ == "__main__":
    fetch_counts()
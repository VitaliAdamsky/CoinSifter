import os
import requests
import time
import json
import sys

# (ВАЖНО) pip install python-dotenv requests
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("ℹ️ Загружены переменные из .env файла.")
except ImportError:
    print("⚠️ 'python-dotenv' не найден. Используются системные переменные окружения.")

# --- НАСТРОЙКИ ---

# --- (ПЕРЕКЛЮЧАТЕЛЬ) ---
# Закомментируйте/Раскомментируйте нужный URL
BASE_URL = "http://127.0.0.1:8000"  # Для локального теста
# BASE_URL = os.getenv("COIN_SIFTER_URL", "https://coin-sifter-server.onrender.com") # Для Render
# --- (КОНЕЦ ПЕРЕКЛЮЧАТЕЛЯ) ---

# 2. Ваш секретный токен
SECRET_TOKEN = os.getenv("SECRET_TOKEN")

# --- Утилиты для вывода ---
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

if not SECRET_TOKEN:
    print(f"\n{Colors.RED}❌ ОШИБКА: SECRET_TOKEN не найден.{Colors.END}")
    print("  Пожалуйста, проверьте ваш .env файл.")
    sys.exit(1)

# --- Глобальные заголовки для защищенных эндпоинтов ---
HEADERS = {
    "X-Auth-Token": SECRET_TOKEN,
    "Content-Type": "application/json"
}

def print_header(title):
    print("\n" + "="*70)
    print(f"{Colors.BOLD}🧪 ТЕСТ: {title}{Colors.END}")
    print("="*70)

def print_success(message):
    print(f"{Colors.GREEN}✅ {message}{Colors.END}")

def print_fail(message):
    print(f"{Colors.RED}❌ {message}{Colors.END}")

def print_info(message):
    print(f"{Colors.BLUE}ℹ️ {message}{Colors.END}")

# --- E2E ТЕСТЫ (Без изменений) ---

def test_1_health_check():
    """Тест 1: Проверяет /health (префикс /api/v1 удален)."""
    print_header("Тест 1: Проверка /health (Доступность)")
    endpoint = f"{BASE_URL}/health"
    print_info(f"Выполняем: GET {endpoint}")

    try:
        response = requests.get(endpoint, timeout=15)
        
        if response.status_code == 200:
            print_success(f"(200 OK) Cервер 'жив'.")
            print_success("✅ Тест 1 ПРОЙДЕН.")
            return True
        else:
            print_fail(f"Ошибка: Статус {response.status_code}. Ожидался 200.")
            print_fail(f"Ответ: {response.text}")
            print_fail("❌ Тест 1 ПРОВАЛЕН.")
            return False
            
    except requests.exceptions.RequestException as e:
        print_fail(f"Критическая ошибка (Connection Error): {e}")
        print_fail("❌ Тест 1 ПРОВАЛЕН.")
        return False

def test_2_log_clearing():
    """Тест 2: Проверяет POST /logs/clear (БЕЗ /trigger)."""
    print_header("Тест 2: Проверка POST /logs/clear (Очистка логов)")
    
    try:
        # --- Шаг 1: Получаем логи (ДО) ---
        print_info(f"Шаг 1: Получаем логи (GET /logs) (Узнаем N)...")
        r_get1 = requests.get(f"{BASE_URL}/logs", headers=HEADERS, timeout=10)
        
        if r_get1.status_code != 200:
            print_fail(f"Не удалось получить логи *до* очистки (Статус: {r_get1.status_code}).")
            print_fail("❌ Тест 2 ПРОВАЛЕН.")
            return False
            
        count_before = r_get1.json().get('count', 0)
        print_info(f"         Лог-записей (до очистки): {count_before}")

        # --- Шаг 2: Очищаем логи ---
        print_info(f"Шаг 2: Очищаем логи (POST /logs/clear)...")
        r_clear = requests.post(f"{BASE_URL}/logs/clear", headers=HEADERS, timeout=30)
        
        if r_clear.status_code != 200:
            print_fail(f"Ошибка (POST /logs/clear): Статус {r_clear.status_code}.")
            print_fail(f"Ответ: {r_clear.text}")
            print_fail("❌ Тест 2 ПРОВАЛЕН.")
            return False
            
        deleted_count = r_clear.json().get('logs_deleted', -1)
        print_success(f"(200 OK) Эндпоинт /logs/clear отработал.")
        print_success(f"         Ответ сервера: удалено {deleted_count} логов.")

        # --- Шаг 3: Получаем логи (ПОСЛЕ) ---
        print_info(f"Шаг 3: Получаем логи (GET /logs) (Ожидаем N = 0)...")
        r_get2 = requests.get(f"{BASE_URL}/logs", headers=HEADERS, timeout=10)
        count_after = r_get2.json().get('count', -1)
        print_info(f"         Лог-записей (после очистки): {count_after}")
        
        # --- Шаг 4: Проверка ---
        if count_after == 0 and deleted_count == count_before:
            print_success(f"Результат: ({count_before} -> 0).")
            print_success("✅ Тест 2 ПРОЙДЕН.")
            return True
        else:
            print_fail(f"Ожидалось 0 логов, но получено {count_after} (Удалено: {deleted_count} из {count_before}).")
            print_fail("❌ Тест 2 ПРОВАЛЕН.")
            return False

    except Exception as e:
        print_fail(f"Критическая ошибка (RequestException): {e}")
        print_fail("❌ Тест 2 ПРОВАЛЕН.")
        return False

def test_3_cache_reload():
    """Тест 3: Проверяет POST /health/cache/reload."""
    print_header("Тест 3: Проверка POST /health/cache/reload (Перезагрузка кэша)")

    try:
        # --- Шаг 1: Получаем монеты (ДО) ---
        print_info(f"Выполняем: GET {BASE_URL}/coins/filtered (Кэш ДО перезагрузки)")
        r_get1 = requests.get(f"{BASE_URL}/coins/filtered", headers=HEADERS, timeout=15)
        
        if r_get1.status_code != 200:
            print_fail(f"Не удалось получить монеты *до* перезагрузки (Статус: {r_get1.status_code}).")
            print_fail(f"Ответ: {r_get1.text}")
            print_fail("❌ Тест 3 ПРОВАЛЕН.")
            return False
            
        count_before = r_get1.json().get('count', -1)
        print_info(f"         Монет в кэше (ДО перезагрузки): {count_before}")

        # --- Шаг 2: Перезагружаем кэш ---
        print_info(f"Выполняем: POST {BASE_URL}/health/cache/reload")
        r_reload = requests.post(f"{BASE_URL}/health/cache/reload", headers=HEADERS, timeout=30)
        
        if r_reload.status_code != 200:
            print_fail(f"Ошибка (POST /health/cache/reload): Статус {r_reload.status_code}.")
            print_fail(f"Ответ: {r_reload.text}")
            print_fail("❌ Тест 3 ПРОВАЛЕН.")
            return False
            
        loaded_count = r_reload.json().get('coins_loaded', -2) 
        print_success(f"(200 OK) Эндпоинт /health/cache/reload отработал.")
        print_success(f"         Ответ сервера: загружено {loaded_count} монет.")

        # --- Шаг 3: Получаем монеты (ПОСЛЕ) ---
        print_info(f"Выполняем: GET {BASE_URL}/coins/filtered (Кэш ПОСЛЕ перезагрузки)")
        r_get2 = requests.get(f"{BASE_URL}/coins/filtered", headers=HEADERS, timeout=15)
        count_after = r_get2.json().get('count', -3) 
        print_info(f"         Монет в кэше (ПОСЛЕ перезагрузки): {count_after}")

        # --- Шаг 4: Проверка ---
        if count_before == loaded_count and count_after == loaded_count:
            print_success(f"Консистентность: (До: {count_before}, Загружено: {loaded_count}, После: {count_after})")
            print_success("✅ Тест 3 ПРОЙДЕН.")
            return True
        else:
            print_fail(f"Несоответствие! (До: {count_before}, Загружено: {loaded_count}, После: {count_after})")
            print_fail("❌ Тест 3 ПРОВАЛЕН.")
            return False

    except Exception as e:
        print_fail(f"Критическая ошибка (RequestException): {e}")
        print_fail("❌ Тест 3 ПРОВАЛЕН.")
        return False

def test_4_data_endpoints():
    """Тест 4: Проверяет все остальные READ-ONLY эндпоинты."""
    print_header("Тест 4: Проверка Read-Only эндпоинтов (Blacklist, DQ, CSV, Formatted)")
    errors = 0

    # --- 4.1: Blacklist ---
    try:
        r_bl = requests.get(f"{BASE_URL}/blacklist", headers=HEADERS, timeout=10)
        if r_bl.status_code == 200 and 'count' in r_bl.json():
            print_success(f"GET /blacklist (200 OK), Найдено: {r_bl.json()['count']} записей")
        else:
            print_fail(f"GET /blacklist (ОШИБКА: {r_bl.status_code})")
            errors += 1
    except Exception as e:
        print_fail(f"GET /blacklist (КРИТИЧЕСКАЯ ОШИБКА: {e})")
        errors += 1

    # --- 4.2: Data Quality Report ---
    try:
        r_dq = requests.get(f"{BASE_URL}/data-quality-report", headers=HEADERS, timeout=15)
        
        # (Исправленная проверка - просто 200 OK)
        if r_dq.status_code == 200:
            print_success(f"GET /data-quality-report (200 OK)")
        else:
            print_fail(f"GET /data-quality-report (ОШИБКА: {r_dq.status_code})")
            errors += 1
    except Exception as e:
        print_fail(f"GET /data-quality-report (КРИТИЧЕСКАЯ ОШИБКА: {e})")
        errors += 1
        
    # --- 4.3: CSV (Публичный) ---
    try:
        r_csv = requests.get(f"{BASE_URL}/coins/filtered/csv", timeout=15)
        if r_csv.status_code == 200 and 'text/csv' in r_csv.headers.get('content-type',''):
            print_success(f"GET /coins/filtered/csv (200 OK), Content-Type: text/csv")
        else:
            print_fail(f"GET /coins/filtered/csv (ОШИБКА: {r_csv.status_code}, {r_csv.headers.get('content-type')})")
            errors += 1
    except Exception as e:
        print_fail(f"GET /coins/filtered/csv (КРИТИЧЕСКАЯ ОШИБКА: {e})")
        errors += 1

    # --- 4.4: Formatted Symbols ---
    try:
        r_fmt = requests.get(f"{BASE_URL}/coins/formatted-symbols", headers=HEADERS, timeout=15)
        if r_fmt.status_code == 200 and 'count' in r_fmt.json():
            print_success(f"GET /coins/formatted-symbols (200 OK), Найдено: {r_fmt.json()['count']} записей")
        else:
            print_fail(f"GET /coins/formatted-symbols (ОШИБКА: {r_fmt.status_code})")
            errors += 1
    except Exception as e:
        print_fail(f"GET /coins/formatted-symbols (КРИТИЧЕСКАЯ ОШИБКА: {e})")
        errors += 1

    if errors == 0:
        print_success("✅ Тест 4 ПРОЙДЕН.")
        return True
    else:
        print_fail(f"❌ Тест 4 ПРОВАЛЕН (Ошибок: {errors}).")
        return False

# --- Запуск ---
if __name__ == "__main__":
    print(f"{Colors.BOLD}🚀 Запуск E2E Read-Only тестов для CoinSifter API...{Colors.END}")
    print(f"{Colors.YELLOW}   Цель: {BASE_URL}{Colors.END}")
    
    # (УДАЛЕНА проверка argparse и input)
    
    results = []
    
    results.append(test_1_health_check())
    time.sleep(1) 
    
    results.append(test_2_log_clearing())
    time.sleep(1)
    
    results.append(test_3_cache_reload())
    time.sleep(1)
    
    results.append(test_4_data_endpoints())

    print("\n" + "="*70)
    print(f"{Colors.BOLD}🏁 E2E ТЕСТИРОВАНИЕ ЗАВЕРШЕНО{Colors.END}")
    
    if all(results):
        print_success(f"ИТОГ: ВСЕ {len(results)} ТЕСТА ПРОЙДENЫ.")
    else:
        print_fail(f"ИТОГ: {results.count(False)} из {len(results)} ТЕСТОВ ПРОВАЛЕНЫ.")
    print("="*70)
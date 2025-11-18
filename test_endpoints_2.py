import os
import requests
import time
import json
import sys

# (ВАЖНО) Убедитесь, что у вас есть .env файл или переменные окружения
# pip install python-dotenv requests
try:
    from dotenv import load_dotenv
    load_dotenv()
    print("ℹ️ Загружены переменные из .env файла.")
except ImportError:
    print("⚠️ 'python-dotenv' не найден. Используются системные переменные окружения.")

# --- НАСТРОЙКИ ---

# 1. URL вашего API на Render
BASE_URL = os.getenv("COIN_SIFTER_URL", "http://127.0.0.1:8000")

# 2. Ваш секретный токен
SECRET_TOKEN = os.getenv("SECRET_TOKEN")

if not SECRET_TOKEN:
    print(f"\n{Colors.RED}❌ ОШИБКА: SECRET_TOKEN не найден.{Colors.END}")
    print("  Пожалуйста, создайте файл .env или установите переменную окружения.")
    print("  Пример .env:")
    print(f"  COIN_SIFTER_URL={BASE_URL}")
    print("  SECRET_TOKEN=O0hrTGEd3meImdof/...")
    sys.exit(1)

# --- Глобальные заголовки для защищенных эндпоинтов ---
HEADERS = {
    "X-Auth-Token": SECRET_TOKEN,
    "Content-Type": "application/json"
}

# --- Утилиты для вывода ---
# (для читаемости в терминале)
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

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

# --- E2E ТЕСТЫ ---

def test_1_health_check_prefix():
    """
    Тест 1: Проверяет, что префикс /api/v1 удален 
    и эндпоинт /health отвечает.
    """
    print_header("Тест 1: Проверка /health (Удаление префикса /api/v1)")
    endpoint = f"{BASE_URL}/health"
    print_info(f"Выполняем: GET {endpoint}")

    try:
        response = requests.get(endpoint, timeout=10)
        
        if response.status_code == 200:
            print_success(f"(200 OK) Cервер 'жив'.")
            print_success("✅ Тест 1 ПРОЙДЕН (api/router.py работает).")
            return True
        else:
            print_fail(f"Ошибка: Статус {response.status_code}. Ожидался 200.")
            print_fail("❌ Тест 1 ПРОВАЛЕН.")
            return False
            
    except requests.exceptions.RequestException as e:
        print_fail(f"Критическая ошибка (RequestException): {e}")
        print_fail("❌ Тест 1 ПРОВАЛЕН.")
        return False

# --- (ИЗМЕНЕННАЯ ФУНКЦИЯ) ---
def test_2_log_clearing():
    """
    Тест 2: Проверяет эндпоинт POST /logs/clear
    (api/endpoints/logs.py, services/mongo_service.py)
    
    (ИЗМЕНЕНО) Теперь включает шаг "ЗАПИСЬ".
    """
    print_header("Тест 2: Проверка POST /logs/clear (Создание -> Очистка -> Проверка)")
    
    try:
        # --- (НОВЫЙ ШАГ 1) ---
        print_info(f"Шаг 1: Запускаем /trigger, чтобы *создать* запись в логе...")
        r_trigger = requests.post(
            f"{BASE_URL}/trigger/run-analysis", 
            headers=HEADERS, 
            timeout=10
        )
        
        if r_trigger.status_code != 200:
            print_fail(f"Не удалось вызвать /trigger (Статус: {r_trigger.status_code}).")
            print_fail("❌ Тест 2 ПРОВАЛЕН (не можем создать лог).")
            return False
        
        run_id = r_trigger.json().get("run_id")
        print_success(f"(200 OK) Триггер запущен. Run ID: {run_id}")
        print_info("         Ждем 3 секунды, чтобы лог гарантированно записался...")
        time.sleep(3)

        # --- Шаг 2: Получаем логи (ДО) ---
        print_info(f"Шаг 2: Получаем логи (GET /logs) (Ожидаем N > 0)...")
        r_get1 = requests.get(f"{BASE_URL}/logs", headers=HEADERS, timeout=10)
        
        if r_get1.status_code != 200:
            print_fail(f"Не удалось получить логи *до* очистки (Статус: {r_get1.status_code}).")
            print_fail("❌ Тест 2 ПРОВАЛЕН.")
            return False
            
        count_before = r_get1.json().get('count', 0)
        print_info(f"         Лог-записей (до очистки): {count_before}")
        
        if count_before == 0:
            print_fail("Лог не был создан (count == 0). Тест не может быть продолжен.")
            print_fail("❌ Тест 2 ПРОВАЛЕН.")
            return False

        # --- Шаг 3: Очищаем логи ---
        print_info(f"Шаг 3: Очищаем логи (POST /logs/clear)...")
        r_clear = requests.post(f"{BASE_URL}/logs/clear", headers=HEADERS, timeout=30)
        
        if r_clear.status_code != 200:
            print_fail(f"Ошибка (POST /logs/clear): Статус {r_clear.status_code}.")
            print_fail("❌ Тест 2 ПРОВАЛЕН.")
            return False
            
        deleted_count = r_clear.json().get('logs_deleted', -1)
        print_success(f"(200 OK) Эндпоинт /logs/clear отработал.")
        print_success(f"         Ответ сервера: удалено {deleted_count} логов.")

        # --- Шаг 4: Получаем логи (ПОСЛЕ) ---
        print_info(f"Шаг 4: Получаем логи (GET /logs) (Ожидаем N = 0)...")
        r_get2 = requests.get(f"{BASE_URL}/logs", headers=HEADERS, timeout=10)
        count_after = r_get2.json().get('count', -1)
        print_info(f"         Лог-записей (после очистки): {count_after}")
        
        # --- Шаг 5: Проверка ---
        if count_after == 0:
            print_success(f"Результат: ({count_before} -> 0).")
            print_success("✅ Тест 2 ПРОЙДЕН.")
            return True
        else:
            print_fail(f"Ожидалось 0 логов, но получено {count_after}.")
            print_fail("❌ Тест 2 ПРОВАЛЕН.")
            return False

    except Exception as e:
        print_fail(f"Критическая ошибка (RequestException): {e}")
        print_fail("❌ Тест 2 ПРОВАЛЕН.")
        return False

# --- (БЕЗ ИЗМЕНЕНИЙ) ---
def test_3_cache_reload():
    """
    Тест 3: Проверяет эндпоинт POST /health/cache/reload
    (api/endpoints/health.py)
    """
    print_header("Тест 3: Проверка POST /health/cache/reload (Перезагрузка кэша)")

    try:
        # --- Шаг 1: Получаем монеты (ДО) ---
        print_info(f"Выполняем: GET {BASE_URL}/coins/filtered (Кэш ДО перезагрузки)")
        r_get1 = requests.get(f"{BASE_URL}/coins/filtered", headers=HEADERS, timeout=10)
        
        if r_get1.status_code != 200:
            print_fail(f"Не удалось получить монеты *до* перезагрузки (Статус: {r_get1.status_code}).")
            print_fail("❌ Тест 3 ПРОВАЛЕН.")
            return False
            
        count_before = r_get1.json().get('count', -1)
        print_info(f"         Монет в кэше (ДО перезагрузки): {count_before}")

        # --- Шаг 2: Перезагружаем кэш ---
        print_info(f"Выполняем: POST {BASE_URL}/health/cache/reload")
        r_reload = requests.post(f"{BASE_URL}/health/cache/reload", headers=HEADERS, timeout=30)
        
        if r_reload.status_code != 200:
            print_fail(f"Ошибка (POST /health/cache/reload): Статус {r_reload.status_code}.")
            print_fail("❌ Тест 3 ПРОВАЛЕН.")
            return False
            
        loaded_count = r_reload.json().get('coins_loaded', -2) # Используем -2 для явной ошибки
        print_success(f"(200 OK) Эндпоинт /health/cache/reload отработал.")
        print_success(f"         Ответ сервера: загружено {loaded_count} монет.")

        # --- Шаг 3: Получаем монеты (ПОСЛЕ) ---
        print_info(f"Выполняем: GET {BASE_URL}/coins/filtered (Кэш ПОСЛЕ перезагрузки)")
        r_get2 = requests.get(f"{BASE_URL}/coins/filtered", headers=HEADERS, timeout=10)
        count_after = r_get2.json().get('count', -3) # Используем -3
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

# --- Запуск ---
if __name__ == "__main__":
    print(f"{Colors.BOLD}🚀 Запуск E2E тестов для CoinSifter API...{Colors.END}")
    print(f"{Colors.YELLOW}   Цель: {BASE_URL}{Colors.END}")
    
    results = []
    
    results.append(test_1_health_check_prefix())
    time.sleep(1) # Небольшая пауза
    
    results.append(test_2_log_clearing())
    time.sleep(1)
    
    results.append(test_3_cache_reload())

    print("\n" + "="*70)
    print(f"{Colors.BOLD}🏁 E2E ТЕСТИРОВАНИЕ ЗАВЕРШЕНО{Colors.END}")
    
    if all(results):
        print_success(f"ИТОГ: ВСЕ {len(results)} ТЕСТА ПРОЙДENЫ.")
    else:
        print_fail(f"ИТОГ: {results.count(False)} из {len(results)} ТЕСТОВ ПРОВАЛЕНЫ.")
    print("="*70)
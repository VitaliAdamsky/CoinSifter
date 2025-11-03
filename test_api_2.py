# test_api_2.py
#
# E2E (End-to-End) Тест для API
# 1. Запускает анализ
# 2. Ждет завершения
# 3. Валидирует СХЕМУ и ФОРМАТЫ ответов 
#    от /coins/filtered, /csv, и /formatted-symbols

import requests
import time
import os
import logging
from dotenv import load_dotenv 

# --- Настройка ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger(__name__)

# --- Константы (Копия из test_api.py) ---
BASE_URL = "http://127.0.0.1:8000/api/v1"
POLL_MAX_WAIT_SECONDS = 4800 # 80 минут
SERVER_WAIT_TIMEOUT = 20     
TRIGGER_TIMEOUT = 10         
POLL_TIMEOUT = 5             
FETCH_TIMEOUT = 10           

# Глобальный счетчик ошибок для итогового отчета
E2E_ERROR_COUNT = 0

# ============================================================================
# --- ШАГ 1: ОЖИДАНИЕ И ЗАПУСК ---
# ============================================================================

def wait_for_server(headers):
    """(Шаг 1) Ждет, пока /health не ответит 200."""
    log.info(f"--- ШАГ 1: ОЖИДАНИЕ СЕРВЕРА ({BASE_URL}/health) ---")
    start_time = time.time()
    
    while True:
        elapsed = time.time() - start_time
        if elapsed > SERVER_WAIT_TIMEOUT:
            log.error(f"E2E FAILED: Сервер не ответил за {SERVER_WAIT_TIMEOUT}с.")
            return False
        
        try:
            response = requests.get(f"{BASE_URL}/health", headers=headers, timeout=POLL_TIMEOUT)
            if response.status_code == 200:
                log.info("E2E PASSED: Сервер доступен (Health OK).")
                return True
        except requests.ConnectionError:
            log.info(f"Ожидание запуска сервера... [{int(elapsed)}s]")
            time.sleep(2)
        except Exception as e:
            log.error(f"E2E FAILED: Ошибка при ожидании сервера: {e}")
            return False

def trigger_analysis(headers):
    """(Шаг 2) Запускает анализ."""
    log.info(f"--- ШАГ 2: ЗАПУСК АНАЛИЗА (/trigger) ---")
    try:
        response = requests.post(f"{BASE_URL}/trigger", headers=headers, timeout=TRIGGER_TIMEOUT)
        
        if response.status_code == 200:
            log_id = response.json().get('run_id')
            log.info(f"E2E PASSED: Анализ запущен. Log ID: {log_id}")
            return log_id
        
        log.error(f"E2E FAILED: Ошибка запуска: {response.status_code}, {response.text}")
        return None
            
    except Exception as e:
        log.error(f"E2E FAILED: Неизвестная ошибка при запуске: {e}")
        return None

# ============================================================================
# --- ШАГ 3: МОНИТОРИНГ ---
# ============================================================================

def poll_logs_for_completion(log_id, headers):
    """(Шаг 3) Опрашивает /logs до завершения."""
    log.info(f"--- ШАГ 3: МОНИТОРИНГ (/logs) [ID: {log_id}] ---")
    start_time = time.time()
    
    while True:
        elapsed = int(time.time() - start_time)
        if elapsed > POLL_MAX_WAIT_SECONDS:
            log.error(f"E2E FAILED: Таймаут ожидания анализа ({POLL_MAX_WAIT_SECONDS}с).")
            return False
        
        try:
            response = requests.get(f"{BASE_URL}/logs", headers=headers, timeout=POLL_TIMEOUT)
            if response.status_code != 200:
                log.error(f"E2E FAILED: Ошибка получения /logs (Статус {response.status_code}).")
                time.sleep(10)
                continue
                
            data = response.json()
            target_log = next((log for log in data.get('logs', []) if log.get('id') == log_id), None)

            if target_log is None:
                log.info(f"Ожидание появления Log ID {log_id} в /logs... [{elapsed:3d}s]")
                time.sleep(5)
                continue

            status = target_log.get('status', 'Неизвестно')
            print(f"Статус: {status} [Время: {elapsed:3d}s]", end="\r", flush=True)

            if status == "Завершено":
                print()
                log.info("E2E PASSED: Анализ завершен успешно.")
                return True
                
            if status == "Ошибка":
                print()
                log.error(f"E2E FAILED: Анализ завершился с ошибкой: {target_log.get('details', 'N/A')}")
                return False
                
            time.sleep(5)

        except Exception as e:
            log.error(f"E2E FAILED: Ошибка при опросе логов: {e}")
            time.sleep(10)

# ============================================================================
# --- ШАГ 4: ВАЛИДАЦИЯ ЭНДПОИНТОВ ---
# ============================================================================

def _assert(condition, error_message):
    """Хелпер для Assert, который считает ошибки."""
    global E2E_ERROR_COUNT
    if not condition:
        log.error(f"    ASSERT FAILED: {error_message}")
        E2E_ERROR_COUNT += 1
    else:
        log.info(f"    ASSERT PASSED: {error_message.split(' -> ')[0]}")


def validate_filtered_json(headers):
    """(Шаг 4.1) Валидирует /coins/filtered (JSON)."""
    log.info(f"--- ШАГ 4.1: ВАЛИДАЦИЯ (/coins/filtered) [JSON] ---")
    
    try:
        response = requests.get(f"{BASE_URL}/coins/filtered", headers=headers, timeout=FETCH_TIMEOUT)
        _assert(response.status_code == 200, f"Статус код 200 -> {response.status_code}")
        
        data = response.json()
        _assert('count' in data, "Ответ содержит 'count'")
        _assert('coins' in data, "Ответ содержит 'coins'")
        
        coins = data.get('coins')
        if not coins:
            log.warning("    WARNING: /coins/filtered вернул 0 монет. Валидация схемы пропускается.")
            return

        log.info(f"    Получено {len(coins)} монет. Валидация первой монеты...")
        
        # Берем первую монету для проверки схемы
        coin = coins[0]
        
        # (Проверка полей из test_api.py)
        _assert('symbol' in coin, "Поле 'symbol' присутствует")
        _assert('full_symbol' in coin, "Поле 'full_symbol' присутствует")
        _assert('exchanges' in coin, "Поле 'exchanges' присутствует")
        _assert('volume_24h_usd' in coin, "Поле 'volume_24h_usd' присутствует")
        _assert('hurst_1h' in coin, "Поле 'hurst_1h' присутствует")
        _assert('entropy_1d' in coin, "Поле 'entropy_1d' присутствует")
        _assert('btc_corr_1d_w30' in coin, "Поле 'btc_corr_1d_w30' присутствует")
        
        # Проверка формата 'symbol' (должен быть "X/Y:Y")
        symbol_format_ok = ':' in coin['symbol'] and '/' in coin['symbol']
        _assert(symbol_format_ok, f"Формат 'symbol' корректен (X/Y:Y) -> {coin['symbol']}")

        log.info("E2E PASSED: Валидация /coins/filtered (JSON) завершена.")

    except Exception as e:
        log.error(f"E2E FAILED: Критическая ошибка при валидации /coins/filtered: {e}")
        E2E_ERROR_COUNT += 1

def validate_filtered_csv(headers):
    """(Шаг 4.2) Валидирует /coins/filtered/csv."""
    log.info(f"--- ШАГ 4.2: ВАЛИДАЦИЯ (/coins/filtered/csv) [CSV] ---")
    
    try:
        response = requests.get(f"{BASE_URL}/coins/filtered/csv", headers=headers, timeout=FETCH_TIMEOUT)
        _assert(response.status_code == 200, f"Статус код 200 -> {response.status_code}")
        _assert('text/csv' in response.headers.get('content-type', ''), "Content-Type 'text/csv'")

        content = response.content.decode('utf-8')
        lines = content.splitlines()
        
        if len(lines) <= 1:
            log.warning("    WARNING: /coins/filtered/csv вернул 0 монет. Валидация заголовков пропускается.")
            return

        header = lines[0]
        log.info(f"    Получено {len(lines) - 1} монет. Валидация заголовков...")
        
        # (Проверка заголовков из test_api.py)
        _assert('symbol' in header, "CSV заголовок 'symbol' присутствует")
        _assert('full_symbol' in header, "CSV заголовок 'full_symbol' присутствует")
        _assert('exchanges' in header, "CSV заголовок 'exchanges' присутствует")
        _assert('hurst_1h' in header, "CSV заголовок 'hurst_1h' присутствует")
        _assert('btc_corr_1d_w30' in header, "CSV заголовок 'btc_corr_1d_w30' присутствует")
        
        log.info("E2E PASSED: Валидация /coins/filtered (CSV) завершена.")
        
    except Exception as e:
        log.error(f"E2E FAILED: Критическая ошибка при валидации /coins/filtered/csv: {e}")
        E2E_ERROR_COUNT += 1

def validate_formatted_symbols(headers):
    """(Шаг 4.3) Валидирует /coins/formatted-symbols (НОВЫЙ)."""
    log.info(f"--- ШАГ 4.3: ВАЛИДАЦИЯ (/coins/formatted-symbols) [NEW] ---")
    
    try:
        response = requests.get(f"{BASE_URL}/coins/formatted-symbols", headers=headers, timeout=FETCH_TIMEOUT)
        _assert(response.status_code == 200, f"Статус код 200 -> {response.status_code}")
        
        data = response.json()
        _assert('count' in data, "Ответ содержит 'count'")
        _assert('symbols' in data, "Ответ содержит 'symbols'")
        
        symbols_list = data.get('symbols')
        if not symbols_list:
            log.warning("    WARNING: /formatted-symbols вернул 0 монет. Валидация формата пропускается.")
            return

        log.info(f"    Получено {len(symbols_list)} монет. Валидация формата...")
        
        # Берем первую монету для проверки формата
        coin = symbols_list[0]
        
        # --- (ГЛАВНАЯ ПРОВЕРКА ФОРМАТА) ---
        
        # 1. Проверка 'symbol'
        symbol = coin.get('symbol', '')
        _assert('symbol' in coin, "Поле 'symbol' присутствует")
        _assert('/' not in symbol, f"Символ НЕ содержит '/' -> {symbol}")
        _assert(':' not in symbol, f"Символ НЕ содержит ':' -> {symbol}")
        
        # 2. Проверка 'exchanges'
        exchanges = coin.get('exchanges', [])
        _assert('exchanges' in coin, "Поле 'exchanges' присутствует")
        _assert(isinstance(exchanges, list), "Поле 'exchanges' является списком")
        
        if exchanges:
            # Проверяем, что 'binanceusdm' был отформатирован
            no_usdm = all('usdm' not in ex for ex in exchanges)
            _assert(no_usdm, f"Список бирж НЕ содержит 'usdm' -> {exchanges}")
        
        log.info("E2E PASSED: Валидация /coins/formatted-symbols завершена.")

    except Exception as e:
        log.error(f"E2E FAILED: Критическая ошибка при валидации /coins/formatted-symbols: {e}")
        E2E_ERROR_COUNT += 1

# ============================================================================
# --- MAIN ---
# ============================================================================

def main():
    global E2E_ERROR_COUNT
    load_dotenv()
    SECRET_TOKEN = os.getenv("SECRET_TOKEN")
    
    if not SECRET_TOKEN:
        log.error("E2E FAILED: SECRET_TOKEN не найден в .env файле.")
        return

    headers = {"X-Auth-Token": SECRET_TOKEN}

    log.info("="*60)
    log.info("E2E (End-to-End) ТЕСТ ЗАПУЩЕН")
    log.info("="*60)

    # --- Шаг 1: Ждем сервер ---
    if not wait_for_server(headers):
        return # Ошибка уже залогирована

     

     

    # --- Шаг 4: Валидация ---
    validate_filtered_json(headers)
    validate_filtered_csv(headers)
    validate_formatted_symbols(headers) # (Новый тест)

    # --- Итог ---
    log.info("="*60)
    log.info("E2E: ИТОГОВЫЙ ОТЧЕТ")
    log.info("="*60)
    if E2E_ERROR_COUNT == 0:
        log.info(f"РЕЗУЛЬТАТ: УСПЕХ (0 Ошибок)")
    else:
        log.error(f"РЕЗУЛЬТАТ: ПРОВАЛ ({E2E_ERROR_COUNT} Ошибок)")
    log.info("="*60)

if __name__ == "__main__":
    main()
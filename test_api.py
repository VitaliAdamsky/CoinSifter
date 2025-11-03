# test_api.py (ИСПРАВЛЕНО)

import requests
import time
import os
import logging
from colorama import Fore, Style, init
from dotenv import load_dotenv 

# --- Настройка ---
init(autoreset=True)

# --- Константы ---
BASE_URL = "http://127.0.0.1:8000/api/v1"
POLL_MAX_WAIT_SECONDS = 8800 # 30 минут
TRIGGER_TIMEOUT = 10         # Таймаут для /trigger
POLL_TIMEOUT = 5             # Таймаут для опроса /logs/{id}
FETCH_TIMEOUT = 10           # Таймаут для /coins/filtered и /csv

# (ИСПРАВЛЕНИЕ) Новый таймаут для ожидания сервера
SERVER_WAIT_TIMEOUT = 20     # Ждем 20 секунд, пока сервер запустится

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)


# ============================================================================
# --- ИЗМЕНЕНИЕ №1: СИНХРОНИЗАЦИЯ С CONFIG.PY ---
# (Эти списки теперь соответствуют 91 колонке из DATABASE_SCHEMA)
# ============================================================================

METRIC_FIELDS = [
    "hurst_1h", "hurst_2h", "hurst_4h", "hurst_12h", "hurst_1d",
    "entropy_1h", "entropy_2h", "entropy_4h", "entropy_12h", "entropy_1d",
    "trend_quality_1h_w20", "trend_quality_2h_w20", "trend_quality_4h_w20", "trend_quality_12h_w20", "trend_quality_1d_w20",
    "mr_quality_1h_w20", "mr_quality_2h_w20", "mr_quality_4h_w20", "mr_quality_12h_w20", "mr_quality_1d_w20",
    "swing_quality_1h_w5", "swing_quality_2h_w5", "swing_quality_4h_w5", "swing_quality_12h_w5", "swing_quality_1d_w5",
    "movement_efficiency_1h", "movement_efficiency_2h", "movement_efficiency_4h", "movement_efficiency_12h", "movement_efficiency_1d",
    "fractal_dimension_1h", "fractal_dimension_2h", "fractal_dimension_4h", "fractal_dimension_12h", "fractal_dimension_1d",
    "adx_above_25_pct_90d_1h", "adx_above_25_pct_90d_2h", "adx_above_25_pct_90d_4h", "adx_above_25_pct_90d_12h", "adx_above_25_pct_90d_1d",
    "di_plus_dominant_pct_90d_1h", "di_plus_dominant_pct_90d_2h", "di_plus_dominant_pct_90d_4h", "di_plus_dominant_pct_90d_12h", "di_plus_dominant_pct_90d_1d",
    "imp_corr_length_ratio_1h_w5", "imp_corr_length_ratio_2h_w5", "imp_corr_length_ratio_4h_w5", "imp_corr_length_ratio_12h_w5", "imp_corr_length_ratio_1d_w5",
    "amplitude_harmony_1h_w5", "amplitude_harmony_2h_w5", "amplitude_harmony_4h_w5", "amplitude_harmony_12h_w5", "amplitude_harmony_1d_w5",
    "smoothness_index_1h_w20", "smoothness_index_2h_w20", "smoothness_index_4h_w20", "smoothness_index_12h_w20", "smoothness_index_1d_w20",
    "skewness_1h_w50", "skewness_2h_w50", "skewness_4h_w50", "skewness_12h_w50", "skewness_1d_w50",
    "kurtosis_1h_w50", "kurtosis_2h_w50", "kurtosis_4h_w50", "kurtosis_12h_w50", "kurtosis_1d_w50",
    "movement_intensity_1h_w14", "movement_intensity_2h_w14", "movement_intensity_4h_w14", "movement_intensity_12h_w14", "movement_intensity_1d_w14",
    "atr_stability_1h_w14", "atr_stability_2h_w14", "atr_stability_4h_w14", "atr_stability_12h_w14", "atr_stability_1d_w14",
    "btc_corr_1d_w30",
    "btc_corr_stability_current_correlation",
    "btc_corr_stability_correlation_std",
    "btc_corr_stability_correlation_stability_score"
]

BASE_FIELDS = [
    "symbol",
    "full_symbol",
    "exchanges",
    "logoUrl",
    "volume_24h_usd",
    "category",
    "analyzed_at"
]

REQUIRED_FIELDS = BASE_FIELDS + METRIC_FIELDS

# ============================================================================

def _safe_format(value, precision=3, default="N/A", is_currency=False, is_percent=False):
    """
    Безопасно форматирует число для вывода.
    """
    if value is None:
        return f"{Style.DIM}{default}"
    try:
        f_val = float(value)
        
        # (ИСПРАВЛЕНО) Проверка на NaN
        if f_val != f_val:
            return f"{Style.DIM}{default}"
        
        if is_currency:
            # Форматируем как валюту (например, $1,234,567)
            return f"{Fore.GREEN}${f_val:,.0f}"
        
        if is_percent:
            # Форматируем как процент (например, +1.23%)
            color = Fore.GREEN if f_val >= 0 else Fore.RED
            return f"{color}{f_val:+.2f}%"

        # Обычное число
        return f"{f_val:.{precision}f}"
        
    except (ValueError, TypeError):
        return f"{Style.DIM}{default}"


def _validate_coin_data(coin):
    """
    (ИЗМЕНЕНИЕ №2)
    Расширенная валидация данных монеты.
    Проверяет только недостающие поля и NaN.
    """
    errors = []
    
    # 1. Проверка наличия обязательных полей
    missing_fields = [field for field in REQUIRED_FIELDS if field not in coin]
    if missing_fields:
        log.debug(f"[{coin.get('symbol', 'N/A')}] Отсутствуют поля: {', '.join(missing_fields)}")
        # (Мы не проваливаем валидацию из-за этого, т.к. фильтры могли быть применены)
    
    # 2. Проверка типов и значений метрик (Только тех, что ЕСТЬ)
    for field in METRIC_FIELDS:
        value = coin.get(field)
        if value is not None:
            try:
                f_val = float(value)
                if f_val != f_val:  # NaN check
                    # (ИСПРАВЛЕНО) Пустые метрики (NaN) - это НОРМАЛЬНО, не ошибка
                    pass
            except (ValueError, TypeError):
                errors.append(f"{field}: не числовое значение ({value})")
    
    # (ИСПРАВЛЕНО) Убраны проверки 'exchanges', 'usdPrice', 'volume'
    # т.к. они либо не в схеме, либо уже проверены фильтрами config.py
    
    return len(errors) == 0, errors


# --- Функции API ---

def trigger_analysis(headers):
    """(Шаг 1) (ИСПРАВЛЕНО) Запускает анализ и возвращает ID лога. ЖДЕТ СЕРВЕР."""
    log.info(f"{Style.BRIGHT}{'='*60}")
    log.info(f"{Style.BRIGHT}ШАГ 1: ЗАПУСК АНАЛИЗА")
    log.info(f"{Style.BRIGHT}{'='*60}")
    
    start_time = time.time()
    
    while True:
        elapsed = time.time() - start_time
        if elapsed > SERVER_WAIT_TIMEOUT:
            log.error(f"{Fore.RED}✗ ТАЙМАУТ: Сервер не ответил за {SERVER_WAIT_TIMEOUT} секунд")
            log.error(f"{Fore.YELLOW}  Проверьте, что сервер запущен и база данных доступна")
            return None
        
        try:
            response = requests.post(f"{BASE_URL}/trigger", headers=headers, timeout=TRIGGER_TIMEOUT)
            
            if response.status_code == 200:
                data = response.json()
                log_id = data.get('run_id') # (ИЗМЕНЕНИЕ №2) V3 router.py возвращает 'run_id'
                log.info(f"{Fore.GREEN}✓ Анализ запущен успешно")
                log.info(f"{Fore.CYAN}  Log ID: {log_id}")
                return log_id
            
            elif response.status_code == 401:
                log.error(f"{Fore.RED}✗ ОШИБКА 401: Неверный X-Auth-Token") # (ИЗМЕНЕНИЕ №1) Обновлен текст ошибки
                log.error(f"{Fore.YELLOW}  Проверьте SECRET_TOKEN в .env файле")
                return None
            elif response.status_code == 403:
                log.error(f"{Fore.RED}✗ ОШИБКА 403: Заголовок X-Auth-Token отсутствует") # (ИЗМЕНЕНИЕ №1) Обновлен текст ошибки
                return None
            else:
                log.error(f"{Fore.RED}✗ Ошибка при запуске: {response.status_code}")
                log.error(f"{Fore.YELLOW}  Ответ: {response.text[:200]}")
                return None
                
        except requests.Timeout:
            log.error(f"{Fore.RED}✗ ТАЙМАУТ POST: Сервер не ответил за {TRIGGER_TIMEOUT} секунд")
            return None
            
        except requests.ConnectionError:
            # (ИСПРАВЛЕНО) Это больше не "смертельная" ошибка.
            log.warning(f"{Fore.YELLOW}⚠ Сервер недоступен (ConnectionError). Ожидание...")
            print(f"Ожидание запуска сервера... [{int(elapsed)}s]", end="\r", flush=True)
            time.sleep(30) # Ждем 2 секунды и пробуем снова
            
        except Exception as e:
            log.error(f"{Fore.RED}✗ Неизвестная ошибка: {e}")
            return None


def poll_logs(log_id, headers):
    """(Шаг 2) (ИЗМЕНЕНИЕ №3) Опрашивает ОБЩИЙ эндпоинт /logs до завершения."""
    log.info(f"\n{Style.BRIGHT}{'='*60}")
    log.info(f"{Style.BRIGHT}ШАГ 2: МОНИТОРИНГ ВЫПОЛНЕНИЯ")
    log.info(f"{Style.BRIGHT}{'='*60}")
    log.info(f"{Fore.CYAN}Log ID: {log_id}")
    
    start_time = time.time()
    last_status = None
    
    while True:
        try:
            elapsed = int(time.time() - start_time)
            
            # Проверка таймаута
            if elapsed > POLL_MAX_WAIT_SECONDS:
                print()
                log.error(f"{Fore.RED}✗ ТАЙМАУТ: Анализ не завершился за {POLL_MAX_WAIT_SECONDS}с")
                return False
            
            # (ИЗМЕНЕНИЕ №3) Обращаемся к /logs, а не /logs/{id}
            response = requests.get(f"{BASE_URL}/logs", headers=headers, timeout=POLL_TIMEOUT)
            
            if response.status_code != 200:
                log.error(f"{Fore.RED}✗ Ошибка получения лога: {response.status_code}")
                time.sleep(30)
                continue
                
            data = response.json()
            
            # (ИЗМЕНЕНИЕ №3) Ищем наш log_id в общем списке
            target_log = None
            if 'logs' in data and data['logs']:
                for log_entry in data['logs']:
                    if log_entry.get('id') == log_id:
                        target_log = log_entry
                        break
            
            if target_log is None:
                log.warning(f"{Fore.YELLOW}⚠ Не удалось найти log_id {log_id} в ответе /logs. Повтор...")
                time.sleep(30)
                continue

            status = target_log.get('status', 'Неизвестно')
            
            if status != last_status:
                print() 
                log.info(f"{Fore.YELLOW}Статус: {status}")
                last_status = status
            
            spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
            spin_char = spinner[elapsed % len(spinner)]
            
            print(f"{spin_char} Ожидание... [{elapsed:3d}s]", end="\r", flush=True)
            
            if status == "Завершено": # (ИЗМЕНЕНИЕ №3) Статус из V3 router.py
                print()
                coins_saved = target_log.get('coins_saved', 0)
                details = target_log.get('details', 'N/A')
                
                log.info(f"{Fore.GREEN}{'='*60}")
                log.info(f"{Fore.GREEN}✓ АНАЛИЗ ЗАВЕРШЕН УСПЕШНО")
                log.info(f"{Fore.GREEN}{'='*60}")
                log.info(f"{Fore.CYAN}  Сохранено монет: {coins_saved}")
                log.info(f"{Fore.CYAN}  Время выполнения: {elapsed}s")
                log.info(f"{Fore.CYAN}  Детали: {details}")
                return True
                
            if status in ["Ошибка", "Критическая ошибка"]:
                print()
                details = target_log.get('details', 'N/A')
                
                log.error(f"{Fore.RED}{'='*60}")
                log.error(f"{Fore.RED}✗ ОШИБКА АНАЛИЗА")
                log.error(f"{Fore.RED}{'='*60}")
                log.error(f"{Fore.YELLOW}  Детали: {details}")
                log.error(f"{Fore.YELLOW}  Проверьте логи сервера для подробностей")
                return False
                
            time.sleep(30)

        except requests.Timeout:
            log.warning(f"{Fore.YELLOW}⚠ Таймаут запроса лога. Повторная попытка...")
            time.sleep(30)
        except requests.ConnectionError:
            log.error(f"{Fore.RED}⚠ Потеряно соединение с сервером. Переподключение...")
            time.sleep(30)
        except KeyboardInterrupt:
            print()
            log.warning(f"{Fore.YELLOW}Прервано пользователем")
            return False
        except Exception as e:
            log.error(f"{Fore.RED}✗ Ошибка при опросе логов: {e}")
            time.sleep(30)


# --- (РЕФАКТОРИНГ) Разделение логики и вывода ---

def _fetch_and_validate_coins_data(headers):
    """
    (Рефакторинг) Шаг 3.1: Получает и валидирует данные.
    Возвращает (coins, all_errors, valid_count, invalid_count) или None при ошибке.
    """
    try:
        response = requests.get(f"{BASE_URL}/coins/filtered", headers=headers, timeout=FETCH_TIMEOUT)
        
        if response.status_code != 200:
            log.error(f"{Fore.RED}✗ Ошибка получения монет: {response.status_code}")
            log.error(f"{Fore.YELLOW}  Ответ: {response.text[:200]}")
            return None

        # (ИЗМЕНЕНИЕ №4) V3 router.py возвращает {'count': N, 'coins': [...]}
        data = response.json()
        coins = data.get('coins', [])
        
        if not coins:
            log.warning(f"{Fore.YELLOW}⚠ Сервер вернул 0 монет после фильтрации")
            log.info(f"{Fore.CYAN}  Возможные причины:")
            log.info(f"{Fore.CYAN}    - (ПЛАН \"ЧИСТОГАН\") Все монеты отфильтрованы Черным списком")
            log.info(f"{Fore.CYAN}    - Ошибка при сохранении в БД (проверьте лог Шага 2)")
            return None

        log.info(f"{Fore.GREEN}✓ Получено {len(coins)} монет с сервера")
        
        valid_count = 0
        invalid_count = 0
        all_errors = []
        
        for i, coin in enumerate(coins, 1):
            is_valid, errors = _validate_coin_data(coin)
            if is_valid:
                valid_count += 1
            else:
                invalid_count += 1
                symbol = coin.get('symbol', f'Coin #{i}')
                all_errors.append((symbol, errors))
        
        return coins, all_errors, valid_count, invalid_count

    except requests.Timeout:
        log.error(f"{Fore.RED}✗ ТАЙМАУТ: Сервер не ответил за {FETCH_TIMEOUT} секунд")
        return None
    except requests.ConnectionError:
        log.error(f"{Fore.RED}✗ ОШИБКА ПОДКЛЮЧЕНИЯ: Сервер недоступен")
        return None
    except Exception as e:
        log.error(f"{Fore.RED}✗ Неизвестная ошибка в _fetch_and_validate_coins_data: {e}")
        return None

def _print_validation_report(all_errors, valid_count, invalid_count, total_count):
    """(Рефакторинг) Шаг 3.2: Печатает отчет о валидации."""
    log.info(f"\n{Style.BRIGHT}{'─'*60}")
    log.info(f"{Style.BRIGHT}ВАЛИДАЦИЯ ДАННЫХ")
    log.info(f"{Style.BRIGHT}{'─'*60}")
    
    if invalid_count == 0:
        log.info(f"{Fore.GREEN}✓ ВСЕ {total_count} МОНЕТ ПРОШЛИ ВАЛИДАЦИЮ")
    else:
        log.warning(f"{Fore.YELLOW}⚠ НАЙДЕНЫ ПРОБЛЕМЫ:")
        log.warning(f"{Fore.YELLOW}  Валидных: {valid_count}")
        log.warning(f"{Fore.YELLOW}  С ошибками: {invalid_count}")
        
        # Показываем первые 3 ошибки
        for symbol, errors in all_errors[:3]:
            log.error(f"{Fore.RED}  [{symbol}]:")
            for error in errors:
                log.error(f"{Fore.RED}    - {error}")
        
        if len(all_errors) > 3:
            log.warning(f"{Fore.YELLOW}  ... и еще {len(all_errors) - 3} монет с ошибками")


# ============================================================================
# --- ИЗМЕНЕНИЕ №3: ПОЛНАЯ ПЕРЕРАБОТКА ВЫВОДА ДЕТАЛЕЙ ---
# ============================================================================

def _print_metric_table(coin, title, metrics_prefix):
    """
    Вспомогательная функция для печати таблицы метрик (1h, 2h, 4h, 12h, 1d)
    """
    log.info(f"│")
    log.info(f"{Style.BRIGHT}│ {title}:")
    
    # Заголовок
    header = f"│   {metrics_prefix:<20} "
    header += f"{'1h':>10} {'2h':>10} {'4h':>10} {'12h':>10} {'1d':>10}"
    log.info(header)
    log.info(f"│   {'-'*20:<20} {'-'*10:>10} {'-'*10:>10} {'-'*10:>10} {'-'*10:>10} {'-'*10:>10}")

    # Ищем все метрики, начинающиеся с префикса
    # (Hurst, Entropy, ... )
    
    # Получаем уникальные "суффиксы" (w20, w5, w50, w14)
    suffixes = set()
    for m in METRIC_FIELDS:
        if m.startswith(metrics_prefix):
            parts = m.split('_')
            # Если есть суффикс (w20, w5, и т.д.)
            if len(parts) > 2 and 'w' in parts[-1]:
                suffixes.add(f"_{parts[-1]}")
            else:
                suffixes.add("") # Для метрик без суффикса (Hurst, Entropy)
    
    # Сортируем (w5, w14, w20, w50)
    sorted_suffixes = sorted(list(suffixes), key=lambda x: int(x.split('w')[-1]) if 'w' in x else 0)

    for suffix in sorted_suffixes:
        row_name = f"{metrics_prefix}{suffix}"
        
        # Название строки (Hurst, Entropy, Trend (w20))
        name = f"{metrics_prefix.split('_')[0].capitalize()}"
        if suffix:
            name += f" ({suffix.replace('_', '')})"
        
        row = f"│   {name:<20} "
        
        # Собираем значения
        row += f"{_safe_format(coin.get(f'{row_name}_1h'), 4):>10} "
        row += f"{_safe_format(coin.get(f'{row_name}_2h'), 4):>10} "
        row += f"{_safe_format(coin.get(f'{row_name}_4h'), 4):>10} "
        row += f"{_safe_format(coin.get(f'{row_name}_12h'), 4):>10} "
        row += f"{_safe_format(coin.get(f'{row_name}_1d'), 4):>10}"
        log.info(row)


def _print_coin_details(coin):
    """(ИСПРАВЛЕНО) Печатает детальный блок для одной монеты."""
    symbol = coin.get('symbol', 'N/A')
    
    log.info(f"\n{Fore.CYAN}{Style.BRIGHT}╔═══ {symbol} ═══╗")
    
    # 1. Общая информация
    log.info(f"{Style.BRIGHT}│ Общая информация:")
    log.info(f"│   Full Symbol:    {coin.get('full_symbol', 'N/A')}")
    log.info(f"│   Exchanges:      {', '.join(coin.get('exchanges', []))}")
    log.info(f"│   Volume 24h:     {_safe_format(coin.get('volume_24h_usd'), is_currency=True)}")
    log.info(f"│   Category:       {coin.get('category', 'N/A')}")
    log.info(f"│   Analyzed At:    {coin.get('analyzed_at', 'N/A')}")

    # 2. Метрики по таймфреймам
    _print_metric_table(coin, "Показатель Хёрста (H)", "hurst")
    _print_metric_table(coin, "Энтропия (E)", "entropy")
    _print_metric_table(coin, "Качество Тренда (R²)", "trend_quality")
    _print_metric_table(coin, "Качество Флэта (MR R²)", "mr_quality")
    _print_metric_table(coin, "Качество Свингов (R²)", "swing_quality")
    _print_metric_table(coin, "Эффективность Движения", "movement_efficiency")
    _print_metric_table(coin, "Фрактальная Размерность", "fractal_dimension")
    _print_metric_table(coin, "ADX > 25 (%/90d)", "adx_above_25_pct_90d")
    _print_metric_table(coin, "DI+ Доминирует (%/90d)", "di_plus_dominant_pct_90d")
    _print_metric_table(coin, "Соотношение Длин (Imp/Corr)", "imp_corr_length_ratio")
    _print_metric_table(coin, "Гармония Амплитуд (Imp/Corr)", "amplitude_harmony")
    _print_metric_table(coin, "Индекс Плавности (Smoothness)", "smoothness_index")
    _print_metric_table(coin, "Асимметрия (Skewness)", "skewness")
    _print_metric_table(coin, "Эксцесс (Kurtosis)", "kurtosis")
    _print_metric_table(coin, "Интенсивность Движения", "movement_intensity")
    _print_metric_table(coin, "Стабильность ATR (CV)", "atr_stability")

    # 3. BTC Корреляция (1d only)
    log.info(f"│")
    log.info(f"{Style.BRIGHT}│ BTC Корреляция (1d):")
    log.info(f"│   Corr (w30):     {_safe_format(coin.get('btc_corr_1d_w30'), 4)}")
    log.info(f"│   Stab. Score:    {_safe_format(coin.get('btc_corr_stability_correlation_stability_score'), 4)}")
    log.info(f"│   Stab. (Current):{_safe_format(coin.get('btc_corr_stability_current_correlation'), 4)}")
    log.info(f"│   Stab. (Std):    {_safe_format(coin.get('btc_corr_stability_correlation_std'), 4)}")
    
    log.info(f"{Fore.CYAN}{Style.BRIGHT}╚{'═'*(len(symbol)+8)}╝")


def fetch_filtered_coins(headers):
    """
    (Рефакторинг) (Шаг 3)
    Получает, валидирует и выводит отфильтрованные монеты (JSON).
    Является "оркестратором", вызывая вспомогательные функции.
    """
    log.info(f"\n{Style.BRIGHT}{'='*60}")
    log.info(f"{Style.BRIGHT}ШАГ 3: ПРОВЕРКА РЕЗУЛЬТАТОВ (JSON)")
    log.info(f"{Style.BRIGHT}{'='*60}")
    
    result = _fetch_and_validate_coins_data(headers)
    if result is None:
        log.error(f"{Fore.RED}✗ Не удалось получить или валидировать данные JSON.")
        return
    
    coins, all_errors, valid_count, invalid_count = result
    
    # (ИСПРАВЛЕНИЕ) Проверка на None
    if coins is None:
        log.error(f"{Fore.RED}✗ Не удалось получить или валидировать данные JSON.")
        return
        
    total_count = len(coins)
    
    # 2. Печать отчета о валидации
    _print_validation_report(all_errors, valid_count, invalid_count, total_count)
    
    # 3. ДЕТАЛЬНЫЙ ВЫВОД ПЕРВОЙ МОНЕТЫ
    if coins:
        log.info(f"\n{Style.BRIGHT}{'─'*60}")
        log.info(f"{Style.BRIGHT}ДЕТАЛЬНЫЙ ПРОСМОТР (первая монета)")
        log.info(f"{Style.BRIGHT}{'─'*60}")
        _print_coin_details(coins[0])
        
        if len(coins) > 1:
            log.info(f"\n{Style.DIM}... и еще {len(coins) - 1} монет (показан только первый)")
    
    # 4. ИТОГОВАЯ СТАТИСТИКА
    log.info(f"\n{Style.BRIGHT}{'─'*60}")
    log.info(f"{Style.BRIGHT}ИТОГОВАЯ СТАТИСТИКА (JSON)")
    log.info(f"{Style.BRIGHT}{'─'*60}")
    log.info(f"{Fore.CYAN}  Всего монет:       {total_count}")
    log.info(f"{Fore.GREEN}  Валидных:          {valid_count}")
    if invalid_count > 0:
        log.info(f"{Fore.RED}  С ошибками:        {invalid_count}")
    log.info(f"{Style.BRIGHT}{'='*60}")


def fetch_filtered_coins_csv(headers):
    """
    (Рефакторинг) (Шаг 4)
    Загружает CSV файл, проверяет его и сохраняет.
    """
    log.info(f"\n{Style.BRIGHT}{'='*60}")
    log.info(f"{Style.BRIGHT}ШАГ 4: ПРОВЕРКА РЕЗУЛЬТАТОВ (CSV)")
    log.info(f"{Style.BRIGHT}{'='*60}")
    
    try:
        response = requests.get(f"{BASE_URL}/coins/filtered/csv", headers=headers, timeout=FETCH_TIMEOUT)
        
        if response.status_code != 200:
            log.error(f"{Fore.RED}✗ Ошибка получения CSV: {response.status_code}")
            log.error(f"{Fore.YELLOW}  Ответ: {response.text[:200]}")
            return

        content = response.content.decode('utf-8')
        
        if not content or len(content.splitlines()) <= 1:
            log.warning(f"{Fore.YELLOW}⚠ Сервер вернул пустой CSV (0 монет)")
            return

        lines = content.splitlines()
        num_coins = len(lines) - 1 # Минус заголовок
        
        log.info(f"{Fore.GREEN}✓ Получен CSV файл, {num_coins} монет")

        # Сохраняем файл
        filename = "filtered_coins.csv"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)
        
        log.info(f"{Fore.CYAN}  Файл сохранен как: {filename}")
        log.info(f"{Style.BRIGHT}{'='*60}")

    except requests.Timeout:
        log.error(f"{Fore.RED}✗ ТАЙМАУТ: Сервер не ответил за {FETCH_TIMEOUT} секунд")
    except requests.ConnectionError:
        log.error(f"{Fore.RED}✗ ОШИБКА ПОДКЛЮЧЕНИЯ: Сервер недоступен")
    except Exception as e:
        log.error(f"{Fore.RED}✗ Неизвестная ошибка: {e}")


def main():
    load_dotenv()
    SECRET_TOKEN = os.getenv("SECRET_TOKEN")
    
    if not SECRET_TOKEN:
        log.error(f"{Fore.RED}{'='*60}")
        log.error(f"{Fore.RED}✗ КРИТИЧЕСКАЯ ОШИБКА")
        log.error(f"{Fore.RED}{'='*60}")
        log.error(f"{Fore.YELLOW}SECRET_TOKEN не найден в .env файле")
        log.info(f"{Fore.CYAN}Создайте .env файл и добавьте:")
        log.info(f"{Fore.CYAN}  SECRET_TOKEN=ваш_токен_здесь")
        return
        
    headers = {
        "X-Auth-Token": SECRET_TOKEN # (ИЗМЕНЕНИЕ №1) X-API-TOKEN -> X-Auth-Token
    }

    log.info(f"{Fore.CYAN}{Style.BRIGHT}")
    log.info(f"╔{'═'*58}╗")
    log.info(f"║{'ТЕСТОВЫЙ КЛИЕНТ АНАЛИЗА КРИПТОВАЛЮТ':^58}║")
    log.info(f"╚{'═'*58}╝")
    log.info(f"{Style.RESET_ALL}")

    # Шаг 1: Запуск анализа
    log_id = trigger_analysis(headers)
    if not log_id:
        log.error(f"\n{Fore.RED}Не удалось запустить анализ. Завершение.")
        return

    # Шаг 2: Мониторинг выполнения
    success = poll_logs(log_id, headers)
    if not success:
        log.error(f"\n{Fore.RED}Анализ завершился с ошибкой.")
        log.info(f"{Fore.YELLOW}Проверьте логи сервера для подробностей")
        return

    # Шаг 3: Проверка и вывод результатов (JSON)
    fetch_filtered_coins(headers)
    
    # Шаг 4: Проверка и загрузка (CSV)
    fetch_filtered_coins_csv(headers)
    
    log.info(f"\n{Fore.GREEN}{Style.BRIGHT}{'='*60}")
    log.info(f"{Fore.GREEN}{Style.BRIGHT}ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    log.info(f"{Fore.GREEN}{Style.BRIGHT}{'='*60}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}Прервано пользователем")
    except Exception as e:
        log.error(f"\n{Fore.RED}Критическая ошибка: {e}")
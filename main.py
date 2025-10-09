import os
import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta_classic as ta
import json
import psycopg2
from psycopg2.extras import execute_values
import asyncio
from tqdm.asyncio import tqdm
from hurst import compute_Hc
from scipy.stats import skew
import numpy as np
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from ccxt.base.errors import RateLimitExceeded, NetworkError, ExchangeError, ExchangeNotAvailable
import uvicorn
from fastapi import FastAPI, Request, HTTPException, BackgroundTasks
from contextlib import asynccontextmanager

# --- ЧАСТЬ 1: Настройка веб-сервера FastAPI ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управляет жизненным циклом сервера: выполняется при старте и остановке."""
    logging.info("Сервер запущен и готов к работе.")
    yield
    logging.info("Сервер останавливается.")

app = FastAPI(lifespan=lifespan)
SECRET_TOKEN = os.getenv("SECRET_TOKEN")

# --- ЧАСТЬ 2: КОНФИГУРАЦИЯ СКРИПТА ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

EXCHANGES_TO_PROCESS = ['binance', 'bybit'] 
MIN_DAILY_VOLUME_USDT = 3_000_000
VOLUME_CATEGORIES = 6
BLACKLIST_FILE = 'blacklist.json'
ANALYSIS_PERIOD_DAYS = 90
MIN_HISTORY_DAYS = 180
HURST_TIMEFRAMES = ['4h', '8h', '12h', '1d']
ATR_PERIOD = 14
BTC_SYMBOL = 'BTC/USDT'
CONCURRENT_REQUEST_LIMIT = 10
RETRY_ATTEMPTS = 3
RETRY_WAIT_MIN = 1
RETRY_WAIT_MAX = 10

# --- ЧАСТЬ 3: ВСПОМОГАТЕЛЬНЫЕ И СЛУЖЕБНЫЕ ФУНКЦИИ ---

def load_blacklist():
    """Загружает черный список монет из файла blacklist.json."""
    try:
        with open(BLACKLIST_FILE, 'r') as f:
            logging.info("Черный список blacklist.json успешно загружен.")
            return json.load(f)
    except FileNotFoundError:
        logging.warning(f"Файл {BLACKLIST_FILE} не найден, используется пустой черный список.")
        return []
    except json.JSONDecodeError:
        logging.error(f"Ошибка чтения файла {BLACKLIST_FILE}. Файл может быть поврежден. Используется пустой черный список.")
        return []

RETRYABLE_EXCEPTIONS = (RateLimitExceeded, NetworkError, ExchangeError, ExchangeNotAvailable)

@retry(
    stop=stop_after_attempt(RETRY_ATTEMPTS),
    wait=wait_exponential(multiplier=1, min=RETRY_WAIT_MIN, max=RETRY_WAIT_MAX),
    retry=retry_if_exception_type(RETRYABLE_EXCEPTIONS),
    before_sleep=lambda retry_state: logging.warning(f"Retrying API call... Attempt #{retry_state.attempt_number}")
)
async def fetch_with_retry(func, *args, **kwargs):
    """Надежная обертка для асинхронных вызовов API с повторными попытками."""
    return await func(*args, **kwargs)

def calculate_entropy(series, bins=10):
    """Рассчитывает энтропию Шеннона для серии данных, устойчиво к NaN/inf."""
    cleaned_series = series.replace([np.inf, -np.inf], np.nan).dropna()
    if cleaned_series.empty or cleaned_series.nunique() <= 1:
        return 0.0
    try:
        hist = np.histogram(cleaned_series, bins=bins, density=False)[0]
        probabilities = hist / len(cleaned_series)
        probabilities = probabilities[probabilities > 0]
        return -np.sum(probabilities * np.log2(probabilities))
    except Exception:
        return None

async def initialize_exchange(exchange_name):
    """Инициализирует биржу и проверяет наличие BTC_SYMBOL."""
    logging.info(f"Инициализация биржи {exchange_name}...")
    try:
        exchange_map = {'binance': ccxt.binance, 'bybit': ccxt.bybit}
        exchange_class = exchange_map.get(exchange_name)
        if not exchange_class:
            logging.error(f"Биржа '{exchange_name}' не поддерживается.")
            return None
        
        exchange = exchange_class({'options': {'defaultType': 'swap'}, 'timeout': 30000})
        await exchange.load_markets()

        if BTC_SYMBOL not in exchange.markets:
            logging.error(f"Критическая ошибка: {BTC_SYMBOL} не найден на бирже {exchange_name}. Эта биржа не будет использоваться.")
            await exchange.close()
            return None

        return exchange
    except Exception as e:
        logging.error(f"Не удалось инициализировать биржу {exchange_name}: {e}")
        return None

# --- ЧАСТЬ 4: ГЛАВНАЯ ЛОГИКА АНАЛИЗА ---

async def run_analysis_logic():
    """Содержит всю основную логику анализа."""
    logging.info("Запуск анализа по триггеру...")
    
    blacklist = load_blacklist()
    init_tasks = [initialize_exchange(name) for name in EXCHANGES_TO_PROCESS]
    initialized_exchanges = await asyncio.gather(*init_tasks)
    exchanges_map = {ex.id: ex for ex in initialized_exchanges if ex}

    try:
        if not exchanges_map:
            logging.critical("Не удалось инициализировать ни одной биржи. Выход.")
            return

        async def fetch_markets(exchange):
            logging.info(f"Загрузка рынков с биржи {exchange.id}...")
            try:
                tickers = await fetch_with_retry(exchange.fetch_tickers)
                return tickers
            except Exception as e:
                logging.error(f"Не удалось загрузить рынки/тикеры с {exchange.id}: {e}")
                return {}

        fetch_tasks = [fetch_markets(ex) for ex in exchanges_map.values()]
        all_tickers_list = await asyncio.gather(*fetch_tasks)
        all_tickers = {ex.id: tickers for ex, tickers in zip(exchanges_map.values(), all_tickers_list)}

        def filter_coins(exchange, tickers):
            filtered = {}
            for symbol, ticker in tickers.items():
                market_data = exchange.markets.get(symbol)
                if not (market_data and market_data.get('swap', False) and market_data.get('quote', '').upper() == 'USDT'):
                    continue
                if not (ticker.get('quoteVolume') and ticker['quoteVolume'] >= MIN_DAILY_VOLUME_USDT):
                    if symbol != BTC_SYMBOL: continue
                base_currency = market_data.get('base', '').upper()
                if base_currency in blacklist: continue
                filtered[symbol] = {'quote_volume_24h': ticker['quoteVolume'], 'base_currency': base_currency}
            logging.info(f"На бирже {exchange.id} найдено {len(filtered)} подходящих монет.")
            return filtered

        all_exchanges_coins = {ex.id: filter_coins(ex, all_tickers[ex.id]) for ex in exchanges_map.values()}
        
        def aggregate_exchanges_data(all_exchanges_coins):
            logging.info("Агрегация данных со всех бирж по приоритету...")
            processed_coins = {}
            priority_order = [ex_id for ex_id in EXCHANGES_TO_PROCESS if ex_id in exchanges_map]
            
            for ex_id in priority_order:
                for symbol, data in all_exchanges_coins[ex_id].items():
                    if symbol not in processed_coins:
                        processed_coins[symbol] = {**data, 'exchanges': [ex_id]}
                    else:
                        processed_coins[symbol]['exchanges'].append(ex_id)
            logging.info(f"Всего уникальных монет после агрегации: {len(processed_coins)}")
            return [{'symbol': symbol, **data} for symbol, data in processed_coins.items()]

        def categorize_by_volume(df, num_categories):
            if df.empty or 'quote_volume_24h' not in df.columns or df['quote_volume_24h'].isnull().all():
                df['category'] = None
                return df
            logging.info(f"Категоризация монет на {num_categories} групп...")
            df['category'] = pd.qcut(df['quote_volume_24h'], q=num_categories, labels=range(1, num_categories + 1), duplicates='drop')
            return df
            
        async def analyze_single_coin(coin_data, exchanges_map, btc_data_cache, semaphore):
            symbol = coin_data['symbol']
            
            ohlcv_data = {}
            primary_exchange_id_for_btc = coin_data['exchanges'][0] # For BTC correlation consistency
            
            for exchange_id in coin_data['exchanges']:
                exchange = exchanges_map[exchange_id]
                async with semaphore:
                    try:
                        limit_for_history_check = MIN_HISTORY_DAYS + ATR_PERIOD
                        ohlcv_1d = await fetch_with_retry(exchange.fetch_ohlcv, symbol, '1d', limit=limit_for_history_check)

                        if len(ohlcv_1d) < MIN_HISTORY_DAYS:
                            logging.info(f"Пропуск {symbol}: история торгов ({len(ohlcv_1d)} дней) меньше минимально требуемой ({MIN_HISTORY_DAYS} дней).")
                            return None
                        
                        ohlcv_data['1d_data'] = ohlcv_1d
                        
                        tasks = {}
                        for tf in HURST_TIMEFRAMES:
                            limit = int((MIN_HISTORY_DAYS / (exchange.parse_timeframe(tf) / exchange.parse_timeframe('1d'))))
                            tasks[f'hurst_{tf}_data'] = fetch_with_retry(exchange.fetch_ohlcv, symbol, tf, limit=limit)
                        
                        hurst_responses = await asyncio.gather(*tasks.values(), return_exceptions=True)
                        for i, tf in enumerate(HURST_TIMEFRAMES):
                            ohlcv_data[f'hurst_{tf}_data'] = hurst_responses[i]

                        break
                    except Exception as e:
                        logging.warning(f"Не удалось получить данные для {symbol} с биржи {exchange_id}: {e}. Пробую следующую...")
            
            if '1d_data' not in ohlcv_data:
                logging.error(f"Не удалось получить данные для {symbol} ни с одной из доступных бирж.")
                return None

            # --- BTC Data Fetching ---
            if primary_exchange_id_for_btc not in btc_data_cache:
                try:
                    exchange_btc = exchanges_map[primary_exchange_id_for_btc]
                    limit_1d = int(MIN_HISTORY_DAYS)
                    btc_ohlcv = await fetch_with_retry(exchange_btc.fetch_ohlcv, BTC_SYMBOL, '1d', limit=limit_1d)
                    df_btc = pd.DataFrame(btc_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                    btc_data_cache[primary_exchange_id_for_btc] = {'returns': df_btc['close'].pct_change().dropna(), 'perf': (df_btc['close'].iloc[-1] / df_btc['close'].iloc[0])}
                except Exception as e:
                    logging.error(f"Не удалось загрузить данные по BTC с {primary_exchange_id_for_btc}: {e}")
                    btc_data_cache[primary_exchange_id_for_btc] = None
            
            btc_data = btc_data_cache[primary_exchange_id_for_btc]
            
            # --- Analysis ---
            result_row = {'symbol': symbol}
            df_ohlcv_1d = pd.DataFrame(ohlcv_data['1d_data'], columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df_analysis = df_ohlcv_1d.tail(ANALYSIS_PERIOD_DAYS).copy()
            daily_returns = df_analysis['close'].pct_change().dropna()
            
            last_close = df_ohlcv_1d['close'].iloc[-1]
            if last_close > 0:
                atr_series = df_ohlcv_1d.ta.atr(length=ATR_PERIOD, high='high', low='low', close='close')
                if atr_series is not None and not atr_series.empty:
                    last_atr = atr_series.iloc[-1]
                    result_row['volatility_index'] = round((last_atr / last_close) * 100, 4) if pd.notna(last_atr) else None

            if not daily_returns.empty:
                result_row['returns_skewness'] = round(skew(daily_returns), 4)
                result_row['entropy'] = calculate_entropy(daily_returns)
                result_row['kurtosis'] = round(daily_returns.kurtosis(), 4)
                result_row['autocorrelation'] = round(daily_returns.autocorr(), 4)

                change = df_analysis['close'].diff().abs().sum()
                net_change = abs(df_analysis['close'].iloc[-1] - df_analysis['close'].iloc[0])
                result_row['efficiency_index'] = round(net_change / change, 4) if change > 0 else 0
                
                up_days = (df_analysis['close'] > df_analysis['open']).sum()
                down_days = (df_analysis['close'] < df_analysis['open']).sum()
                total_days = len(df_analysis)
                result_row['trend_harmony_index'] = round(abs(up_days - down_days) / total_days, 4) if total_days > 0 else 0

                wicks = (df_analysis['high'] - df_analysis['low']) - (df_analysis['close'] - df_analysis['open']).abs()
                body = (df_analysis['close'] - df_analysis['open']).abs()
                result_row['avg_wick_ratio'] = round((wicks / body).mean(), 4) if not body.eq(0).all() else 0

                cumulative_returns = (1 + daily_returns).cumprod()
                peak = cumulative_returns.expanding(min_periods=1).max()
                drawdown = (cumulative_returns - peak) / peak
                result_row['max_drawdown_percent'] = round(drawdown.min() * 100, 2)

            if btc_data and not btc_data['returns'].empty:
                aligned_returns, btc_aligned_returns = daily_returns.align(btc_data['returns'], join='inner')
                if len(aligned_returns) > 1:
                    result_row['btc_correlation'] = round(aligned_returns.corr(btc_aligned_returns), 4)

                coin_perf = (df_analysis['close'].iloc[-1] / df_analysis['close'].iloc[0]) if df_analysis['close'].iloc[0] != 0 else 1
                btc_perf = btc_data['perf']
                result_row['relative_strength_vs_btc'] = round(coin_perf / btc_perf, 4) if btc_perf != 0 else 1
            
            for tf in HURST_TIMEFRAMES:
                hurst_data = ohlcv_data.get(f'hurst_{tf}_data')
                if isinstance(hurst_data, Exception):
                    logging.warning(f"Не удалось загрузить данные для Хёрста ({tf}) для {symbol}: {hurst_data}")
                    continue
                
                if hurst_data and len(hurst_data) > 100:
                    close_prices = np.array([candle[4] for candle in hurst_data])
                    try:
                        if np.std(close_prices) > 1e-9: # Check for non-zero volatility
                            H, _, _ = compute_Hc(close_prices, kind='price', simplified=True)
                            result_row[f'hurst_{tf}'] = round(H, 4)
                    except Exception as e:
                        logging.warning(f"Не удалось рассчитать Хёрста для {symbol} на {tf}: {e}")

            return result_row

        async def analyze_and_enhance_data(df, exchanges_map):
            if df.empty: return df
            logging.info(f"Начало асинхронного анализа для {len(df)} монет...")
            btc_data_cache = {}
            semaphore = asyncio.Semaphore(CONCURRENT_REQUEST_LIMIT)
            
            analysis_tasks = [analyze_single_coin(row, exchanges_map, btc_data_cache, semaphore) for _, row in df.iterrows()]

            analysis_results = []
            for f in tqdm.as_completed(analysis_tasks, desc="Анализ монет"):
                try:
                    result = await f
                    if result:
                        analysis_results.append(result)
                except Exception as e:
                    logging.error(f"Критическая ошибка в задаче анализа для монеты: {e}", exc_info=True)

            if not analysis_results:
                logging.warning("Не удалось проанализировать ни одной монеты.")
                return pd.DataFrame()

            df_results = pd.DataFrame(analysis_results).set_index('symbol')
            df = df.set_index('symbol').join(df_results).reset_index()
            return df

        def save_to_database(data_df):
            db_url = os.getenv('DATABASE_URL')
            if not db_url:
                logging.error("DATABASE_URL не установлена.")
                return
            
            if data_df.empty:
                logging.info("Нет данных для сохранения в базу.")
                return

            logging.info("Сохранение данных в PostgreSQL...")
            conn = None
            final_columns = [
                'symbol', 'exchanges', 'category', 'logoUrl', 'volatility_index',
                'hurst_4h', 'hurst_8h', 'hurst_12h', 'hurst_1d',
                'efficiency_index', 'trend_harmony_index', 'btc_correlation',
                'returns_skewness', 'avg_wick_ratio', 'relative_strength_vs_btc', 
                'max_drawdown_percent', 'entropy', 'kurtosis', 'autocorrelation'
            ]
            
            column_types = {
                'exchanges': 'TEXT[]', 'category': 'INTEGER', 'logoUrl': 'VARCHAR(255)',
                'volatility_index': 'REAL', 'hurst_4h': 'REAL', 'hurst_8h': 'REAL',
                'hurst_12h': 'REAL', 'hurst_1d': 'REAL', 'efficiency_index': 'REAL',
                'trend_harmony_index': 'REAL', 'btc_correlation': 'REAL', 'returns_skewness': 'REAL',
                'avg_wick_ratio': 'REAL', 'relative_strength_vs_btc': 'REAL', 'max_drawdown_percent': 'REAL',
                'entropy': 'REAL', 'kurtosis': 'REAL', 'autocorrelation': 'REAL'
            }

            for col in final_columns:
                if col not in data_df.columns:
                    data_df[col] = None
            
            try:
                conn = psycopg2.connect(db_url)
                cursor = conn.cursor()
                
                # --- ИЗМЕНЕНИЕ: Создание таблицы и колонок отдельно ---
                # Создаём таблицу, если её нет
                create_table_query = """
                CREATE TABLE IF NOT EXISTS monthly_coin_selection (
                    id SERIAL PRIMARY KEY,
                    "symbol" VARCHAR(255) UNIQUE,
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
                """
                cursor.execute(create_table_query)

                # Добавляем колонки, если их нет
                for col, col_type in column_types.items():
                    cursor.execute(f'ALTER TABLE monthly_coin_selection ADD COLUMN IF NOT EXISTS "{col}" {col_type};')
                
                # Фиксируем изменения схемы
                conn.commit()
                # --- КОНЕЦ ИЗМЕНЕНИЯ ---

                # Теперь вставляем данные в отдельной транзакции
                update_cols = [f'"{col}" = EXCLUDED."{col}"' for col in final_columns if col != 'symbol']
                upsert_query = f"""
                INSERT INTO monthly_coin_selection ({', '.join(f'"{c}"' for c in final_columns)})
                VALUES %s
                ON CONFLICT (symbol) DO UPDATE SET
                {', '.join(update_cols)};
                """
                
                data_to_insert = [tuple(row) for row in data_df.reindex(columns=final_columns).where(pd.notna(data_df), None).to_numpy()]
                if data_to_insert:
                    execute_values(cursor, upsert_query, data_to_insert)
                    conn.commit()  # <-- Вот тут фиксируем данные
                    logging.info(f"Успешно сохранено/обновлено {len(data_to_insert)} записей.")
            except Exception as e:
                logging.error(f"Ошибка при работе с базой данных: {e}")
                if conn:
                    conn.rollback()  # <-- Откатываем данные, если ошибка
            finally:
                if conn: conn.close()
        
        aggregated_list = aggregate_exchanges_data(all_exchanges_coins)
        if not aggregated_list:
            logging.info("Не найдено монет для анализа. Завершение.")
            return

        df = pd.DataFrame(aggregated_list)
        df = categorize_by_volume(df, VOLUME_CATEGORIES)
        df['logoUrl'] = df['base_currency'].str.lower() + '.png'
        
        enhanced_df = await analyze_and_enhance_data(df, exchanges_map)
        
        if not enhanced_df.empty:
            save_to_database(enhanced_df)

    finally:
        logging.info("Закрытие всех соединений с биржами...")
        close_tasks = [ex.close() for ex in exchanges_map.values() if ex]
        await asyncio.gather(*close_tasks)
        logging.info("Скрипт успешно завершил работу.")


# --- ЧАСТЬ 5: ЭНДПОИНТЫ СЕРВЕРА ---

@app.post("/trigger")
async def trigger_run(request: Request, background_tasks: BackgroundTasks):
    auth_header = request.headers.get('Authorization')
    if not SECRET_TOKEN:
        raise HTTPException(status_code=500, detail={"error": "SECRET_TOKEN не настроен на сервере."})
    
    if not auth_header or auth_header != f"Bearer {SECRET_TOKEN}":
        raise HTTPException(status_code=401, detail={"error": "Неверный токен авторизации."})

    background_tasks.add_task(run_analysis_logic)
    
    return {"message": "Запрос на анализ принят. Процесс запущен в фоновом режиме."}

@app.get("/health")
async def health_check():
    """Простой эндпоинт для проверки, что сервер жив (требуется Render)."""
    return {"status": "ok"}

# --- Точка входа для Uvicorn (для локального теста) ---
if __name__ == "__main__":
    logging.info("Для локального запуска сервера используйте команду: uvicorn main:app --reload")


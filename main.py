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

# Используем биржи, лояльные к европейским серверам
EXCHANGES_TO_PROCESS = ['okx', 'bybit'] 
MIN_DAILY_VOLUME_USDT = 3_000_000
VOLUME_CATEGORIES = 6
BLACKLIST_FILE = 'blacklist.json'
ANALYSIS_PERIOD_DAYS = 90
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
        logging.error(f"Ошибка чтения файла {BLACKLIST_FILE}. Используется пустой черный список.")
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
    """Рассчитывает энтропию Шеннона для серии данных."""
    if series.empty or series.nunique() <= 1:
        return 0.0
    try:
        hist = np.histogram(series, bins=bins, density=False)[0]
        probabilities = hist / len(series)
        probabilities = probabilities[probabilities > 0]
        return -np.sum(probabilities * np.log2(probabilities))
    except Exception:
        return None

async def initialize_exchange(exchange_name):
    """Инициализирует биржу с правильными опциями."""
    logging.info(f"Инициализация биржи {exchange_name}...")
    try:
        exchange_map = {'okx': ccxt.okx, 'bybit': ccxt.bybit}
        exchange_class = exchange_map.get(exchange_name)
        if not exchange_class:
            logging.warning(f"Биржа '{exchange_name}' не поддерживается.")
            return None
        
        options = {'options': {'defaultType': 'swap'}}
        exchange = exchange_class(options)
        exchange.options['httpRequestTimeout'] = 30000
        return exchange
    except Exception as e:
        logging.error(f"Не удалось инициализировать биржу {exchange_name}: {e}")
        return None

# --- ЧАСТЬ 4: ГЛАВНАЯ ЛОГИКА АНАЛИЗА ---

async def run_analysis_logic():
    """Содержит всю основную логику анализа, которая раньше была в main()."""
    logging.info("Запуск анализа по триггеру...")
    
    blacklist = load_blacklist()
    
    init_tasks = [initialize_exchange(name) for name in EXCHANGES_TO_PROCESS]
    initialized_exchanges = await asyncio.gather(*init_tasks)
    exchanges_map = {ex.id: ex for ex in initialized_exchanges if ex}

    if not exchanges_map:
        logging.critical("Не удалось инициализировать ни одной биржи. Выход.")
        return

    async def fetch_and_filter_markets(exchange, min_volume_usdt, blacklist):
        logging.info(f"Загрузка рынков с биржи {exchange.id}...")
        try:
            markets = await fetch_with_retry(exchange.load_markets)
            tickers = await fetch_with_retry(exchange.fetch_tickers)
        except Exception as e:
            logging.error(f"Не удалось загрузить рынки/тикеры с {exchange.id}: {e}")
            return {}

        filtered_coins = {}
        for symbol, ticker in tickers.items():
            market_data = markets.get(symbol)
            if not (market_data and market_data.get('swap', False) and market_data.get('quote', '').upper() == 'USDT'):
                continue
            if not (ticker.get('quoteVolume') and ticker['quoteVolume'] >= min_volume_usdt):
                if symbol != BTC_SYMBOL: continue
            base_currency = market_data.get('base', '').upper()
            if base_currency in blacklist: continue
            filtered_coins[symbol] = {'quote_volume_24h': ticker['quoteVolume'], 'base_currency': base_currency}
        logging.info(f"На бирже {exchange.id} найдено {len(filtered_coins)} подходящих монет.")
        return filtered_coins

    fetch_tasks = [fetch_and_filter_markets(ex, MIN_DAILY_VOLUME_USDT, blacklist) for ex in exchanges_map.values()]
    all_exchanges_coins_list = await asyncio.gather(*fetch_tasks)
    all_exchanges_coins = {ex.id: coins for ex, coins in zip(exchanges_map.values(), all_exchanges_coins_list)}
    
    def aggregate_exchanges_data(all_exchanges_coins, exchanges_map):
        logging.info("Агрегация данных со всех бирж...")
        processed_coins = {}
        priority_order = [ex.id for ex in exchanges_map.values()]
        
        for ex_id in priority_order:
            if ex_id not in all_exchanges_coins: continue
            for symbol, data in all_exchanges_coins[ex_id].items():
                if symbol not in processed_coins:
                    processed_coins[symbol] = {**data, 'exchanges': [ex_id]}
                else:
                    processed_coins[symbol]['exchanges'].append(ex_id)
        logging.info(f"Всего уникальных монет после агрегации: {len(processed_coins)}")
        return [{'symbol': symbol, **data} for symbol, data in processed_coins.items()]

    def categorize_by_volume(df, num_categories):
        if df.empty: return df
        logging.info(f"Категоризация монет на {num_categories} групп...")
        df_sorted = df.sort_values(by='quote_volume_24h', ascending=False)
        df_sorted['rank'] = range(len(df_sorted))
        df_sorted['category'] = pd.cut(df_sorted['rank'], bins=num_categories, labels=range(1, num_categories + 1))
        return df_sorted.drop(columns=['rank'])
    
    def get_candles_for_period(days, timeframe_str, exchange):
        tf_in_ms = exchange.parse_timeframe(timeframe_str) * 1000
        days_in_ms = days * 24 * 60 * 60 * 1000
        if tf_in_ms == 0: return 200
        return int(days_in_ms / tf_in_ms)
        
    async def analyze_single_coin(coin_data, exchange, btc_data, semaphore):
        symbol = coin_data['symbol']
        result_row = {'symbol': symbol}
        
        async with semaphore:
            tasks = {}
            limit_1d_full = get_candles_for_period(ANALYSIS_PERIOD_DAYS, '1d', exchange) + ATR_PERIOD
            tasks['1d_data'] = fetch_with_retry(exchange.fetch_ohlcv, symbol, '1d', limit=limit_1d_full)
            for tf in HURST_TIMEFRAMES:
                limit = get_candles_for_period(ANALYSIS_PERIOD_DAYS, tf, exchange)
                tasks[f'hurst_{tf}_data'] = fetch_with_retry(exchange.fetch_ohlcv, symbol, tf, limit=limit)
            
            responses = await asyncio.gather(*tasks.values(), return_exceptions=True)
        
        ohlcv_1d = responses[0]
        if not isinstance(ohlcv_1d, Exception) and len(ohlcv_1d) >= ANALYSIS_PERIOD_DAYS:
            df_ohlcv_1d = pd.DataFrame(ohlcv_1d, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
            df_analysis = df_ohlcv_1d.tail(ANALYSIS_PERIOD_DAYS).copy()
            daily_returns = df_analysis['c'].pct_change().dropna()
            
            last_close = df_ohlcv_1d['c'].iloc[-1]
            if last_close > 0:
                df_ohlcv_1d.ta.atr(length=ATR_PERIOD, append=True)
                last_atr = df_ohlcv_1d[f'ATR_{ATR_PERIOD}'].iloc[-1]
                result_row['volatility_index'] = round((last_atr / last_close) * 100, 4) if pd.notna(last_atr) else None

            if not daily_returns.empty:
                result_row['returns_skewness'] = round(skew(daily_returns), 4)
                result_row['entropy'] = calculate_entropy(daily_returns)
                result_row['kurtosis'] = round(daily_returns.kurtosis(), 4)
                result_row['autocorrelation'] = round(daily_returns.autocorr(), 4)

                change = df_analysis['c'].diff().abs().sum()
                net_change = abs(df_analysis['c'].iloc[-1] - df_analysis['c'].iloc[0])
                result_row['efficiency_index'] = round(net_change / change, 4) if change > 0 else 0
                
                up_days = (df_analysis['c'] > df_analysis['o']).sum()
                down_days = (df_analysis['c'] < df_analysis['o']).sum()
                total_days = len(df_analysis)
                result_row['trend_harmony_index'] = round(abs(up_days - down_days) / total_days, 4) if total_days > 0 else 0

                wicks = (df_analysis['h'] - df_analysis['l']) - (df_analysis['c'] - df_analysis['o']).abs()
                body = (df_analysis['c'] - df_analysis['o']).abs()
                result_row['avg_wick_ratio'] = round((wicks / body).mean(), 4) if not body.eq(0).all() else 0

                cumulative_returns = (1 + daily_returns).cumprod()
                peak = cumulative_returns.expanding(min_periods=1).max()
                drawdown = (cumulative_returns - peak) / peak
                result_row['max_drawdown_percent'] = round(drawdown.min() * 100, 2)

            if btc_data and not btc_data['returns'].empty:
                aligned_returns, btc_aligned_returns = daily_returns.align(btc_data['returns'], join='inner')
                if len(aligned_returns) > 1:
                    result_row['btc_correlation'] = round(aligned_returns.corr(btc_aligned_returns), 4)

                coin_perf = (df_analysis['c'].iloc[-1] / df_analysis['c'].iloc[0])
                btc_perf = btc_data['perf']
                result_row['relative_strength_vs_btc'] = round(coin_perf / btc_perf, 4) if btc_perf != 0 else 1

        for i, tf in enumerate(HURST_TIMEFRAMES):
            ohlcv_hurst = responses[i+1]
            if not isinstance(ohlcv_hurst, Exception) and len(ohlcv_hurst) > 100:
                close_prices = [candle[4] for candle in ohlcv_hurst]
                H, _, _ = compute_Hc(close_prices, kind='price', simplified=True)
                result_row[f'hurst_{tf}'] = round(H, 4)

        return result_row

    async def analyze_and_enhance_data(df, exchanges_map):
        if df.empty: return df
        logging.info(f"Начало асинхронного анализа для {len(df)} монет...")
        btc_data_cache = {}
        semaphore = asyncio.Semaphore(CONCURRENT_REQUEST_LIMIT)
        
        analysis_tasks = []
        for _, row in df.iterrows():
            primary_exchange_id = row['exchanges'][0]
            exchange = exchanges_map[primary_exchange_id]

            if primary_exchange_id not in btc_data_cache:
                try:
                    limit_1d = get_candles_for_period(ANALYSIS_PERIOD_DAYS, '1d', exchange)
                    btc_ohlcv = await fetch_with_retry(exchange.fetch_ohlcv, BTC_SYMBOL, '1d', limit=limit_1d)
                    df_btc = pd.DataFrame(btc_ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
                    btc_data_cache[primary_exchange_id] = {'returns': df_btc['c'].pct_change().dropna(), 'perf': (df_btc['c'].iloc[-1] / df_btc['c'].iloc[0])}
                except Exception as e:
                    logging.error(f"Не удалось загрузить данные по BTC с {primary_exchange_id}: {e}")
                    btc_data_cache[primary_exchange_id] = None
            
            btc_data = btc_data_cache[primary_exchange_id]
            analysis_tasks.append(analyze_single_coin(row, exchange, btc_data, semaphore))

        analysis_results = await tqdm.gather(*analysis_tasks, desc="Анализ монет")
        
        df_results = pd.DataFrame(analysis_results).set_index('symbol')
        df = df.set_index('symbol').join(df_results).reset_index()
        return df

    def save_to_database(data_df):
        db_url = os.getenv('DATABASE_URL')
        if not db_url:
            logging.error("DATABASE_URL не установлена. Запись в БД невозможна.")
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
        
        for col in final_columns:
            if col not in data_df.columns:
                data_df[col] = None

        data_df_to_save = data_df.reindex(columns=final_columns)
        
        try:
            conn = psycopg2.connect(db_url)
            cursor = conn.cursor()
            
            create_table_query = """
            CREATE TABLE IF NOT EXISTS monthly_coin_selection (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(255) UNIQUE,
                exchanges TEXT[],
                category INTEGER,
                logoUrl VARCHAR(255),
                volatility_index REAL,
                hurst_4h REAL,
                hurst_8h REAL,
                hurst_12h REAL,
                hurst_1d REAL,
                efficiency_index REAL,
                trend_harmony_index REAL,
                btc_correlation REAL,
                returns_skewness REAL,
                avg_wick_ratio REAL,
                relative_strength_vs_btc REAL,
                max_drawdown_percent REAL,
                entropy REAL,
                kurtosis REAL,
                autocorrelation REAL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(create_table_query)
            
            cursor.execute("TRUNCATE TABLE monthly_coin_selection RESTART IDENTITY;")
            data_to_insert = [tuple(row) for row in data_df_to_save.where(pd.notna(data_df_to_save), None).to_numpy()]
            cols = ', '.join(f'"{c}"' for c in final_columns)
            execute_values(cursor, f"INSERT INTO monthly_coin_selection ({cols}) VALUES %s", data_to_insert)
            
            conn.commit()
            logging.info(f"Успешно сохранено {len(data_to_insert)} записей.")
        except Exception as e:
            logging.error(f"Ошибка при работе с базой данных: {e}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()

    aggregated_list = aggregate_exchanges_data(all_exchanges_coins, exchanges_map)
    if not aggregated_list:
        logging.info("Не найдено монет, удовлетворяющих критериям. Завершение работы.")
        return

    df = pd.DataFrame(aggregated_list)
    df = df.sort_values(by='quote_volume_24h', ascending=False).reset_index(drop=True)
    df = categorize_by_volume(df, VOLUME_CATEGORIES)
    df['logoUrl'] = df['base_currency'].str.lower() + '.png'
    
    enhanced_df = await analyze_and_enhance_data(df, exchanges_map)
    
    save_to_database(enhanced_df)
    
    logging.info("Закрытие всех соединений с биржами...")
    close_tasks = [ex.close() for ex in exchanges_map.values()]
    await asyncio.gather(*close_tasks)
    
    logging.info("Скрипт успешно завершил работу.")

# --- ЧАСТЬ 5: ЭНДПОИНТЫ СЕРВЕРА ---

@app.post("/trigger")
async def trigger_run(request: Request, background_tasks: BackgroundTasks):
    """
    Этот эндпоинт запускает основной скрипт в фоновом режиме.
    Он защищен секретным токеном.
    """
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
    # Эта часть не используется при запуске на Render
    logging.info("Для локального запуска сервера используйте команду: uvicorn main:app --reload")


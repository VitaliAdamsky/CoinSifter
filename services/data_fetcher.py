# services/data_fetcher.py 

import logging
import asyncio
from datetime import datetime, timedelta
import pandas as pd
from collections import defaultdict

import config

from .exchange_api import fetch_ohlcv, fetch_markets, fetch_tickers
from .exchange_utils import initialize_exchange

log = logging.getLogger(__name__)


# ============================================================================
# === _fetch_ohlcv_single_tf ===
# ============================================================================

async def _fetch_ohlcv_single_tf(exchange, symbol, timeframe, since, log_prefix):
    """
    Загружает и обрабатывает один таймфрейм.
    """
    data = await fetch_ohlcv(exchange, symbol, timeframe, since, config.CANDLE_LIMIT_DEFAULT, f"{log_prefix} {timeframe}")
    
    if data:
        try:
            df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
            df.set_index('timestamp', inplace=True)
            df = df.astype(float)
            
            return timeframe, df
        except Exception as e:
            log.error(f"{log_prefix} {symbol} {timeframe}: Ошибка при конвертации в DataFrame: {e}")
            return timeframe, None
    else:
        log.debug(f"{log_prefix} {symbol} {timeframe}: Данные не загружены (пустой ответ).")
        return timeframe, None


# ============================================================================
# === fetch_all_ohlcv_data ===
# ============================================================================

async def fetch_all_ohlcv_data(exchange, symbol, tf_config, log_prefix=""):
    """
    Загружает OHLCV данные для всех таймфреймов ПАРАЛЛЕЛЬНО.
    """
    ohlcv_data = {}
    
    since_timestamps = {}
    for tf, days in tf_config.items():
        since_timestamps[tf] = exchange.parse8601((datetime.utcnow() - timedelta(days=days)).isoformat())

    tasks = []
    for timeframe, days_to_load in tf_config.items():
        since = since_timestamps[timeframe]
        tasks.append(
            _fetch_ohlcv_single_tf(exchange, symbol, timeframe, since, log_prefix)
        )
        
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    loaded_tf_count = 0
    for result in results:
        if isinstance(result, Exception):
            log.error(f"{log_prefix} {symbol}: Необработанная ошибка при загрузке ТФ: {result}", exc_info=True)
            continue
            
        timeframe, df = result
        
        if df is not None:
            ohlcv_data[timeframe] = df
            loaded_tf_count += 1
            
    if loaded_tf_count < len(tf_config):
        if loaded_tf_count > 0:
            log.info(f"{log_prefix} {symbol}: Загружено {loaded_tf_count} из {len(tf_config)} ТФ (частично).")
        return {}
    else:
        log.debug(f"{log_prefix} {symbol}: Успешно загружены все {len(tf_config)} ТФ.")
        return ohlcv_data


# ============================================================================
# === _parse_ticker_data ===
# ============================================================================

def _parse_ticker_data(ticker, exchange_id, log_prefix=""):
    """
    Парсит данные тикера.
    """
    try:
        symbol = ticker.get('symbol')
        quote_currency = ticker.get('quoteVolumeCurrency', 'USDT')
        
        full_symbol = f"{symbol}:{quote_currency}"
        
        volume_key = config.EXCHANGE_VOLUME_KEYS.get(exchange_id, 'quoteVolume')
        
        raw_volume = ticker.get(volume_key)
        volume_24h_usd = float(raw_volume) if raw_volume is not None else 0.0

        raw_base_volume = ticker.get('volume')
        volume24h_base = float(raw_base_volume) if raw_base_volume is not None else 0.0
        
        raw_price = ticker.get('last')
        usd_price = float(raw_price) if raw_price is not None else 0.0
        
        raw_change = ticker.get('percentage')
        change24h = float(raw_change) if raw_change is not None else 0.0

        return {
            'symbol': symbol,
            'full_symbol': full_symbol,
            'quoteCurrency': quote_currency,
            'usdPrice': usd_price,
            'volume_24h_usd': volume_24h_usd,
            'volume24h_base': volume24h_base,
            'change24h': change24h
        }
    except Exception as e:
        log.error(f"{log_prefix} Ошибка парсинга тикера (ID: {exchange_id}, Ticker: {ticker}): {e}", exc_info=True)
        return None


# ============================================================================
# === _extract_base_symbol (НОВОЕ) ===
# ============================================================================

def _extract_base_symbol(ccxt_symbol: str) -> str:
    """
    Извлекает базовый символ из ccxt-формата (e.g., 'SOL/USDT' -> 'SOL').
    Это обеспечивает единую логику сравнения с Черным списком.
    """
    if not ccxt_symbol:
        return ""
    # Базовый символ - это часть до первого слэша (/).
    return ccxt_symbol.split('/')[0]


# ============================================================================
# === fetch_all_coins_data (Оптимизировано для 300 монет) ===
# ============================================================================

async def fetch_all_coins_data(exchange_ids, quote_currencies, blacklist=None, log_prefix=""):
    """
    Загружает данные о всех монетах с бирж (Этап 1).
    Оптимизировано для ~300 монет с разблокировкой каждые 100 итераций.
    """
    if blacklist is None:
        blacklist = set()
    
    log.info(f"{log_prefix} (Этап 1) Запуск с бирж: {exchange_ids}, Валюты: {quote_currencies}")
    
    all_coins_data = {}
    active_exchanges = {}
    markets_map = {}
    skipped_coins = defaultdict(set)
    
    # --- Инициализация и загрузка рынков ---
    
    async def init_exchange_and_markets(ex_id):
        log_prefix_ex = f"{log_prefix} [{ex_id}]"
        try:
            exchange = await initialize_exchange(ex_id, log_prefix_ex)
            if not exchange:
                return ex_id, None, None
                
            markets = await fetch_markets(exchange, quote_currencies, log_prefix_ex)
            if not markets:
                log.error(f"{log_prefix_ex} ❌ Не удалось загрузить рынки. Пропуск биржи.") 
                if hasattr(exchange, 'close'):
                    await exchange.close()
                return ex_id, None, None
                
            return ex_id, exchange, markets
        except Exception as e:
            log.error(f"{log_prefix_ex} ❌ Крит. ошибка при инициализации/загрузке: {e}", exc_info=True) 
            return ex_id, None, None
    
    init_results = await asyncio.gather(*[init_exchange_and_markets(ex_id) for ex_id in exchange_ids])
    
    # Разблокировка после gather (для других задач event loop)
    await asyncio.sleep(0)

    for ex_id, exchange, markets in init_results:
        if exchange and markets:
            active_exchanges[ex_id] = exchange
            markets_map[ex_id] = markets
        else:
            pass 

    if not active_exchanges:
        log.error(f"{log_prefix} (Этап 1) ❌ Не удалось инициализировать НИ ОДНОЙ биржи. Остановка.")
        return [], {}, {}, skipped_coins
    
    # --- Загрузка тикеров ---

    log.info(f"{log_prefix} (Этап 1) Загрузка тикеров с {list(active_exchanges.keys())}...")
    
    fetch_tasks = []
    for ex_id, exchange in active_exchanges.items():
        fetch_tasks.append(fetch_tickers(exchange, f"{log_prefix} [{ex_id}]"))
        
    tickers_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)
    
    # Разблокировка после gather
    await asyncio.sleep(0)
    
    log.info(f"{log_prefix} (Этап 1) Обработка {sum(len(t) for t in tickers_results if isinstance(t, dict))} тикеров...")
    
    min_volume = config.MIN_VOLUME_24H_USD
    
    for (ex_id, exchange), tickers_data in zip(active_exchanges.items(), tickers_results):
        log_prefix_ex = f"{log_prefix} [{ex_id}]"
        
        if isinstance(tickers_data, Exception):
            log.error(f"{log_prefix_ex} Ошибка загрузки тикеров: {tickers_data}")
            continue
        if not tickers_data:
            log.warning(f"{log_prefix_ex} Тикеры не получены.")
            continue
            
        markets = markets_map[ex_id]
        
        # Счётчик для разблокировки каждые 100 монет
        i = 0 
        for ccxt_symbol, ticker in tickers_data.items():
            
            if ccxt_symbol not in markets:
                continue
                
            coin = _parse_ticker_data(ticker, ex_id, log_prefix_ex)
            if not coin:
                continue
                
            full_symbol = coin['full_symbol']
            
            # --- ИЗМЕНЕНИЕ №2: ЕДИНАЯ ЛОГИКА ЧЕРНОГО СПИСКА ---
            base_symbol = _extract_base_symbol(coin['symbol']) 
            
            if base_symbol in blacklist:
                skipped_coins['Blacklist'].add(full_symbol)
                continue
            # --- КОНЕЦ ИЗМЕНЕНИЯ №2 ---
                
            volume = coin['volume_24h_usd']
            if volume is None or volume < min_volume:
                skipped_coins['Volume'].add(full_symbol)
                continue
                
            if full_symbol not in all_coins_data:
                base_currency = _extract_base_symbol(coin['symbol']) # Используем новую утилиту
                logo_url = f"{base_currency.lower()}.png"
                
                all_coins_data[full_symbol] = {
                    **coin,
                    'exchanges': [ex_id],
                    'name': markets[ccxt_symbol].get('name', coin['symbol']),
                    'logoUrl': logo_url,
                    '_volumes_by_exchange': {ex_id: volume} 
                }
            else:
                existing_coin = all_coins_data[full_symbol]
                
                existing_coin['exchanges'].append(ex_id)
                
                existing_coin['_volumes_by_exchange'][ex_id] = volume
                
                if volume > existing_coin['volume_24h_usd']:
                    existing_coin['volume_24h_usd'] = volume
                    existing_coin['volume24h_base'] = coin['volume24h_base']
                    existing_coin['usdPrice'] = coin['usdPrice']
                    existing_coin['change24h'] = coin['change24h']

            i += 1
            # Разблокировка каждые 100 монет (оптимально для ~300 монет)
            if i % 100 == 0:
                await asyncio.sleep(0)
    
    final_coin_list = list(all_coins_data.values())
    
    total_skipped_step_1 = sum(len(s) for s in skipped_coins.values())
    
    log.info(f"{log_prefix} (Этап 1) ✅ Завершен. Найдено {len(final_coin_list)} монет, прошедших фильтры.")
    if total_skipped_step_1 > 0:
        log.info(f"{log_prefix} (Этап 1) 📋 Пропущено (всего): {total_skipped_step_1}")
        for reason, symbols in skipped_coins.items():
            # --- ИЗМЕНЕНИЕ №3: Выделение лога Черного списка ---
            if reason == 'Blacklist':
                # Используем log.warning и специальный символ
                log.warning(f"{log_prefix} ├─ Пропуск (ЧЕРНЫЙ СПИСОК): {len(symbols)} монет 🚫")
            else:
                 log.info(f"{log_prefix} ├─ Пропуск ({reason}): {len(symbols)} монет")
            # --- КОНЕЦ ИЗМЕНЕНИЯ №3 ---
    
    return final_coin_list, active_exchanges, markets_map, skipped_coins
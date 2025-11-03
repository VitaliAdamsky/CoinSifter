# metrics/character.py (Финальная версия MCI)

import logging
import pandas as pd
import numpy as np

log = logging.getLogger(__name__)

# Диапазоны нормализации, основанные на аудите и клиппированных данных [0.01, 0.99]
# H_center = 0.5 (Random Walk)
# FD_center = 0.79 (Сложность Random Walk в вашем клиппированном диапазоне)
H_MAX_DEVIATION = 0.49
FD_MAX_DEVIATION = 0.78 # Масштабирующий фактор для FD, чтобы сбалансировать вклад


def calculate_movement_character_index(hurst_exponent, fractal_dimension):
    """
    Calculate the Integrated Movement Character Index (MCI).
    
    MCI combines Hurst (persistence) and Fractal Dimension (complexity) 
    into a single normalized metric in the range [-1.0, 1.0].
    
    MCI > 0: Тренд (Trend)
    MCI ~ 0: Случайное блуждание (Random Walk / Flat)
    MCI < 0: Антитренд/Шум (Mean Reversion / Noise)
    """
    if np.isnan(hurst_exponent) or np.isnan(fractal_dimension):
        return np.nan

    try:
        # 1. Нормализация Херста (Прямая зависимость: Тренд -> 1.0, Антитренд -> -1.0)
        # H находится в диапазоне [0.01, 0.99]
        normalized_H = (hurst_exponent - 0.5) / H_MAX_DEVIATION
        
        # 2. Нормализация Фрактальной Размерности (Обратная зависимость: Низкая Сложность (Тренд) -> 1.0)
        # Центр = 0.79. Чем ниже FD относительно центра, тем лучше тренд.
        # Формула: (Центр - FD) / Масштабный фактор
        normalized_FD = (0.79 - fractal_dimension) / FD_MAX_DEVIATION
        
        # 3. Интеграция: Среднее двух нормализованных компонентов
        mci = 0.5 * normalized_H + 0.5 * normalized_FD
        
        # 4. Клиппинг для гарантии границ [-1.0, 1.0]
        mci_clipped = np.clip(mci, -1.0, 1.0)
        
        return float(mci_clipped)
        
    except Exception as e:
        log.warning(f"Error calculating MCI: {e}")
        return np.nan
# metrics/__init__.py

# Этот файл "хирургически" пуст.
# Его "хирургическое" присутствие "хирургически" 
# превращает директорию 'metrics/' в Python-модуль (пакет),
# что "хирургически" исправляет 'ModuleNotFoundError'.# metrics/__init__.py

import warnings

# Suppress deprecation warnings from pandas_ta_classic
warnings.filterwarnings('ignore', category=UserWarning, module='pandas_ta_classic')
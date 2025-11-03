# analysis/__init__.py

"""
Модуль Analysis.

Этот __init__.py файл делает директорию 'analysis' Python-модулем 
и "выставляет" главную дирижерскую функцию 'analysis_logic' 
наружу.

Это позволяет 'router.py' (и другим частям) импортировать ее 
(например: `import analysis`, `await analysis.analysis_logic(...)`).
"""

# Импортируем "дирижера" из 'logic.py'
from .logic import analysis_logic
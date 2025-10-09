import os
import pandas as pd
import psycopg2
import logging

# Настройка логирования для вывода сообщений в консоль
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_data_from_db():
    """
    Подключается к базе данных, извлекает все данные из таблицы 
    monthly_coin_selection и сохраняет их в CSV-файл.
    """
    db_url = "postgresql://coin_sifter_db_43u4_user:o2JLzcpHrsZC7lBBOQDpDVLhWQxTL1H7@dpg-d3jodet6ubrc73d0h4dg-a.frankfurt-postgres.render.com/coin_sifter_db_43u4"
    
    if not db_url:
        logging.error("Ошибка: Переменная окружения DATABASE_URL не установлена.")
        logging.info("Пожалуйста, установите ее перед запуском: export DATABASE_URL='...'")
        return

    try:
        logging.info("Подключение к базе данных...")
        conn = psycopg2.connect(db_url)
        
        # SQL-запрос для выбора всех данных, отсортированных по дате (новые вверху)
        query = "SELECT * FROM monthly_coin_selection ORDER BY created_at DESC"
        
        logging.info("Извлечение данных...")
        # Используем pandas для удобного чтения данных из SQL в DataFrame
        df = pd.read_sql_query(query, conn)
        
        # Закрываем соединение
        conn.close()
        
        if df.empty:
            logging.warning("База данных пуста. Файл не будет создан.")
            return

        # Сохраняем DataFrame в CSV-файл
        output_filename = 'coin_data.csv'
        df.to_csv(output_filename, index=False)
        
        logging.info(f"Успешно! Данные сохранены в файл: {output_filename}")
        logging.info(f"Всего записей извлечено: {len(df)}")

    except Exception as e:
        logging.error(f"Произошла ошибка при работе с базой данных: {e}")

if __name__ == "__main__":
    fetch_data_from_db()

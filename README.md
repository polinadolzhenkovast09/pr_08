# pr_08

Практическая работа 8. Анализ метода загрузки данных

Цель:
Определить наиболее эффективный метод загрузки данных (малых и больших объемов) из CSV-файлов в СУБД PostgreSQL, сравнивая время выполнения для методов: pandas.to_sql(), psycopg2.copy_expert() (с файлом и с io.StringIO), и пакетная вставка (psycopg2.extras.execute_values).

Задачи:

Подключиться к предоставленной базе данных PostgreSQL.

Проанализировать структуру исходных CSV-файлов (upload_test_data.csv , upload_test_data_big.csv).


Создать эскизы ER-диаграмм для таблиц, соответствующих структуре CSV-файлов.

Реализовать три различных метода загрузки данных в PostgreSQL(pandas.to_sql(), copy_expert(), io.StringIO).

Измерить время, затраченное каждым методом на загрузку данных из малого файла (upload_test_data.csv).

Измерить время, затраченное каждым методом на загрузку данных из большого файла (upload_test_data_big.csv).

Визуализировать результаты сравнения времени загрузки с помощью гистограммы (matplotlib).

Сделать выводы об эффективности каждого метода для разных объемов данных.

Подключаемся к библиотекам и даем путь к файлам cvs
````
!pip install psycopg2-binary pandas sqlalchemy matplotlib numpy

import psycopg2

from psycopg2 import Error

from psycopg2 import extras # For execute_values

import pandas as pd

from sqlalchemy import create_engine

import io # For StringIO

import time

import matplotlib.pyplot as plt

import numpy as np

import os # To check file existence
````

![image](https://github.com/user-attachments/assets/464ba098-88f4-440e-92b3-72ad33bde21d)

![image](https://github.com/user-attachments/assets/bd195adc-f2d7-44b9-9233-99ae377895fe)

Далее указываем путь к файлам csv и подключимся к БД
```
small_csv_paprint("Libraries installed and imported successfully.")

big_csv_path = r'C:\Users\coco4\Downloads\upload_test_data_big.csv'

!ls


DB_USER = "postgres"
DB_PASSWORD = "1"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "Lect_8"


small_csv_path = 'upload_test_data.csv'
big_csv_path = 'upload_test_data_big.csv' # Corrected filename

table_name = 'sales_data'th = r'C:\Users\coco4\Downloads\upload_test_data.csv'
````

Проведем анализ скорости загрузки.
````

timing_results = {
    'small_file': {},
    'big_file': {}
}


if not os.path.exists(small_csv_path):
    print(f"ERROR: Small CSV file not found: {small_csv_path}. Upload it and restart.")
elif not os.path.exists(big_csv_path):
    print(f"ERROR: Big CSV file not found: {big_csv_path}. Upload it and restart.")
elif not connection or not cursor or not engine:
    print("ERROR: Database connection not ready. Cannot proceed.")
else:
    # --- Method 1: pandas.to_sql() ---
    def load_with_pandas_to_sql(eng, df, tbl_name, chunk_size=1000):
        """Loads data using pandas.to_sql() and returns time taken."""
        start_time = time.perf_counter()
        try:
            # Using method='multi' might be faster for some DBs/data
            # Chunksize helps manage memory for large files
            df.to_sql(tbl_name, eng, if_exists='append', index=False, method='multi', chunksize=chunk_size)
        except Exception as e:
             print(f"Error in pandas.to_sql: {e}")
             # Note: No explicit transaction management here, relies on SQLAlchemy/DBAPI defaults or engine settings.
             # For critical data, wrap in a try/except with explicit rollback if needed.
             raise # Re-raise the exception to signal failure
        end_time = time.perf_counter()
        return end_time - start_time

    # --- Method 2: psycopg2.copy_expert() with CSV file ---
    def load_with_copy_expert_file(conn, cur, tbl_name, file_path):
        """Loads data using psycopg2.copy_expert() directly from file and returns time taken."""
        start_time = time.perf_counter()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # Skip header row using COPY options
                sql_copy = f"""
                COPY {tbl_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')
                """
                cur.copy_expert(sql=sql_copy, file=f)
            conn.commit() # Commit transaction after successful COPY
        except (Exception, Error) as error:
            print(f"Error in copy_expert (file): {error}")
            conn.rollback() # Rollback on error
            raise
        end_time = time.perf_counter()
        return end_time - start_time

    # --- Method 3: psycopg2.copy_expert() with io.StringIO ---
    def load_with_copy_expert_stringio(conn, cur, df, tbl_name):
        """Loads data using psycopg2.copy_expert() from an in-memory StringIO buffer and returns time taken."""
        start_time = time.perf_counter()
        buffer = io.StringIO()
        # Write dataframe to buffer as CSV, including header
        df.to_csv(buffer, index=False, header=True, sep=',')
        buffer.seek(0) # Rewind buffer to the beginning
        try:
            sql_copy = f"""
            COPY {tbl_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')
            """
            cur.copy_expert(sql=sql_copy, file=buffer)
            conn.commit() # Commit transaction after successful COPY
        except (Exception, Error) as error:
            print(f"Error in copy_expert (StringIO): {error}")
            conn.rollback() # Rollback on error
            raise
        finally:
            buffer.close() # Ensure buffer is closed
        end_time = time.perf_counter()
        return end_time - start_time


    # --- Timing Execution ---
    print("\n--- Starting Data Loading Tests ---")

    # Load DataFrames (only once)
    print("Loading CSV files into Pandas DataFrames...")
    try:
        df_small = pd.read_csv(small_csv_path)
        # The big file might be too large to load fully into Colab memory.
        # If memory errors occur, consider processing it in chunks for methods
        # that support it (like pandas.to_sql with chunksize, or modify batch insert).
        # For COPY methods, memory isn't usually an issue as they stream.
        df_big = pd.read_csv(big_csv_path)
        print(f"Loaded {len(df_small)} rows from {small_csv_path}")
        print(f"Loaded {len(df_big)} rows from {big_csv_path}")
    except MemoryError:
        print("\nERROR: Not enough RAM to load the large CSV file into a Pandas DataFrame.")
        print("Some methods (pandas.to_sql, StringIO, Batch Insert) might fail or be inaccurate.")
        print("The copy_expert (file) method should still work.")
        # We can try to proceed, but note the limitation
        df_big = None # Indicate that the big dataframe couldn't be loaded
    except Exception as e:
        print(f"Error loading CSVs into DataFrames: {e}")
        df_small, df_big = None, None # Stop execution if loading fails


    if df_small is not None: # Proceed only if small DF loaded
        # --- Small File Tests ---
        print(f"\n--- Testing with Small File ({small_csv_path}) ---")

        # Test pandas.to_sql
        try:
            reset_table(connection, cursor, table_name)
            print("Running pandas.to_sql...")
            t = load_with_pandas_to_sql(engine, df_small, table_name)
            timing_results['small_file']['pandas.to_sql'] = t
            print(f"Finished in {t:.4f} seconds.")
        except Exception as e: print(f"pandas.to_sql failed for small file.")

        # Test copy_expert (file)
        try:
            reset_table(connection, cursor, table_name)
            print("Running copy_expert (file)...")
            t = load_with_copy_expert_file(connection, cursor, table_name, small_csv_path)
            timing_results['small_file']['copy_expert (file)'] = t
            print(f"Finished in {t:.4f} seconds.")
        except Exception as e: print(f"copy_expert (file) failed for small file.")

        # Test copy_expert (StringIO)
        try:
            reset_table(connection, cursor, table_name)
            print("Running copy_expert (StringIO)...")
            t = load_with_copy_expert_stringio(connection, cursor, df_small, table_name)
            timing_results['small_file']['copy_expert (StringIO)'] = t
            print(f"Finished in {t:.4f} seconds.")
        except Exception as e: print(f"copy_expert (StringIO) failed for small file.")



    # --- Big File Tests ---
    print(f"\n--- Testing with Big File ({big_csv_path}) ---")

    # Test pandas.to_sql (if df_big loaded)
    if df_big is not None:
        try:
            reset_table(connection, cursor, table_name)
            print("Running pandas.to_sql...")
            t = load_with_pandas_to_sql(engine, df_big, table_name, chunk_size=10000) # Larger chunksize for big file
            timing_results['big_file']['pandas.to_sql'] = t
            print(f"Finished in {t:.4f} seconds.")
        except Exception as e: print(f"pandas.to_sql failed for big file.")
    else:
        print("Skipping pandas.to_sql for big file (DataFrame not loaded).")


    # Test copy_expert (file) - This should work even if df_big didn't load
    try:
        reset_table(connection, cursor, table_name)
        print("Running copy_expert (file)...")
        t = load_with_copy_expert_file(connection, cursor, table_name, big_csv_path)
        timing_results['big_file']['copy_expert (file)'] = t
        print(f"Finished in {t:.4f} seconds.")
    except Exception as e: print(f"copy_expert (file) failed for big file.")


    # Test copy_expert (StringIO) (if df_big loaded)
    if df_big is not None:
        try:
            reset_table(connection, cursor, table_name)
            print("Running copy_expert (StringIO)...")
            t = load_with_copy_expert_stringio(connection, cursor, df_big, table_name)
            timing_results['big_file']['copy_expert (StringIO)'] = t
            print(f"Finished in {t:.4f} seconds.")
        except Exception as e: print(f"copy_expert (StringIO) failed for big file.")
    else:
        print("Skipping copy_expert (StringIO) for big file (DataFrame not loaded).")



    print("\n--- Data Loading Tests Finished ---")


print("\nTiming Results Summary:")
import json
print(json.dumps(timing_results, indent=2))
````

![image](https://github.com/user-attachments/assets/9b695086-ced4-4109-ae43-52cf12a13a5f)

Pandas оказывается самым медленным способом загрузки

![image](https://github.com/user-attachments/assets/c56d10f5-1469-4b59-9171-e0e7d7400ab9)

#Вариант 10 Индивдуальные задания
1.Настройка таблиц. Создать таблицы sales_small, sales_big.
2.Загрузка малых данных. Метод:pandas.to_sql()
3.Загрузка больших данных. Метод: copy_expert (file)
4.SQL: Найти id из sales_small, где total_revenue < 0.1.
5.Python: Извлечь 100 случайных записей из sales_big, построить scatter plot quantity vs cost.

Перед тем как приступить к выполнению заданий убедимся, что все вспомогательные функции из шаблона выполняются, сделаем это перед запуском кода варианта
и пропишем список констант

![image](https://github.com/user-attachments/assets/0d1db53d-102e-4de3-bb3b-c2746f90b2c2)

#Задание 1. Настройка таблиц. Создать таблицы sales_small, sales_big.
````
print("\n--- Задача 1: Создание таблиц ---")
create_table(small_table_name)
create_table(big_table_name)
````
![image](https://github.com/user-attachments/assets/b64bde52-9f80-4a89-9d4f-9142f268adf5)

![image](https://github.com/user-attachments/assets/65a83436-1875-4e02-9208-9376c208056a)

#2.Загрузка малых данных. Метод:pandas.to_sql()

````
import pandas as pd
from sqlalchemy import create_engine

# 1. Загрузка данных из CSV-файла
csv_file_path = r'C:\Users\coco4\Downloads\upload_test_data.csv'   # Путь к вашему CSV-файлу
df = pd.read_csv(csv_file_path)
# 2. Создание подключения к базе данных PostgreSQL
# Замените значения на ваши параметры подключения
username = 'postgres'
password = '1'
host = 'localhost'  # или IP-адрес сервера
port = '5432'       # порт PostgreSQL (по умолчанию 5432)
database = 'Pr_08'

# Создание строки подключения
connection_string = f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'
engine = create_engine(connection_string)

# 3. Загрузка данных в таблицу PostgreSQL
table_name = 'sales_small'  # Имя таблицы в PostgreSQL
df.to_sql(table_name, engine, if_exists='replace', index=False)

print(f"Данные успешно загружены в таблицу '{table_name}'!")
````
![image](https://github.com/user-attachments/assets/dd23ca9b-618b-4015-8531-d5a9cc48ca85)

#3.Загрузка больших данных. Метод: copy_expert (file)
````
print(f"\n--- Задача 3: Загрузка данных из '{big_csv_path}' в '{big_table_name}' (метод file) ---")
if os.path.exists(big_csv_path):
    load_via_copy_file(big_csv_path, big_table_name)
else:
    print(f"ОШИБКА: Файл '{big_csv_path}' не найден. Загрузка не выполнена.")
````
![image](https://github.com/user-attachments/assets/31edb3b1-226a-4718-82bf-41ad8b1ad05b)

#4.SQL: Найти id из sales_small, где total_revenue < 0.1.
````
print("\n--- Задача 4: SQL Анализ таблицы sales_small ---")
sql_query_task4 = f"""
    SELECT id
    FROM sales_small
    WHERE total_revenue < 0.1;
    """
print("Выполнение SQL запроса:")
print(sql_query_task4)
results_task4 = execute_sql(sql_query_task4, fetch=True)

if results_task4 is not None:
        print("\nРезультаты запроса (id, total_revenue):")
        if results_task4:
            for row in results_task4:
                print(row)
        else:
            print("Запрос успешно выполнен, но не вернул строк.")
else:
        print("Ошибка выполнения SQL запроса.")
````

![image](https://github.com/user-attachments/assets/68f52284-81de-4ee7-a786-cb7ec3b3a4cb)

#5.Python: Извлечь 100 случайных записей из sales_big, построить scatter plot quantity vs cost.

````
import psycopg2  # Для подключения к PostgreSQL
import pandas as pd  # Для работы с данными
import matplotlib.pyplot as plt  # Для построения графиков

# Шаг 1: Подключение к базе данных PostgreSQL
try:
   
    query = """
    SELECT quantity, cost
    FROM sales_big
    ORDER BY RANDOM()  -- Случайная сортировка
    LIMIT 100;         -- Ограничение до 100 записей
    """
    data = pd.read_sql_query(query, connection)

except Exception as e:
    print(f"Ошибка подключения или выполнения запроса: {e}")
finally:
    if connection:
        connection.close()
        print("Соединение с базой данных закрыто.")

# Шаг 3: Построение scatter plot
plt.figure(figsize=(10, 6))  # Размер графика
plt.scatter(data['quantity'], data['cost'], color='blue', alpha=0.7)  # Scatter plot
plt.title('Scatter Plot: Quantity vs Cost') 
plt.xlabel('Quantity')  
plt.ylabel('Cost') 
plt.grid(True)  
plt.show()  
````

![image](https://github.com/user-attachments/assets/95aa82cd-e0df-4dfc-bd6c-741bdaf18035)

![image](https://github.com/user-attachments/assets/fac9d8ad-421c-4662-a3c3-fcb6c18406b7)

Вывод
В ходе работы были изучены и найдены наиболее эффективные методы загрузки данных (малых и больших) из CSV-файлов в СУБД PostgreSQL, визуализировали полученные результаты.























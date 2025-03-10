import requests
import json
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import time
import os

# Параметры
TOKEN = 'ваш токен, строка вида y0__xC...' # Инструкция как получить - https://yandex.ru/dev/webmaster/doc/dg/tasks/how-to-get-oauth.html
HOST_ID = {'https:sub1.domain.ru:443': [225], 'https:sub2.domain.ru:443': [1]}  # Массив host_id в формате ЯВМ + массив регионов (https://yandex.ru/dev/webmaster/doc/ru/reference/host-query-analytics#region-ids). Оставьте пустым, если нужно собрать по всем хостам.
EXCLUDED_HOSTS = ["https://sub3.domain.ru", "https://domain777.ru"]  # Исключенные хосты, если надо собрать данные по всем, кроме этих
SLEEP_TIME_API = 2
CONFIG_FILE = 'processed_data.json' # Название служебного файла, используется, чтобы можно было продолжить сбор с того места, где остановились
TEMP_CSV_FILE = f'temp_data_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv'
ALL_TIME_FULL_FILE_CSV = 'for-all-time-full-data-query_stats.csv' # Название выходного файла, если выбран CSV
ALL_TIME_FULL_FILE_DB = 'for-all-time-full-data-query_stats.db' # Название выходного файла, если выбран sqlite
STORAGE_TYPE = 'csv'  # выберите 'csv' или 'sqlite'
COLLECT_ZERO_DEMAND = 1  # 1 - не собирать фразы с нулевым спросом, 0 - собирать все
COLLECT_BY_URL = 0  # 0 - собирать по хосту целиком (быстрее), 1 - собирать по URL (медленнее, но будут данные по страницам)
REGION_IDS = []  # Глобальный массив регионов, по умолчанию пустой

# Инициализация SQLite (если выбрано)
def init_db():
    if STORAGE_TYPE != 'sqlite':
        return
    conn = sqlite3.connect(ALL_TIME_FULL_FILE_DB)
    cursor = conn.cursor()
    key_column = 'host_id' if not COLLECT_BY_URL else 'url'
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS query_stats (
            date TEXT,
            {key_column} TEXT,
            query TEXT,
            position TEXT,
            clicks TEXT,
            ctr TEXT,
            demand TEXT,
            impressions TEXT,
            region_ids TEXT,
            PRIMARY KEY (date, {key_column}, query)
        )
    ''')
    conn.commit()
    conn.close()

# Получение ID пользователя
def get_user_id():
    url = 'https://api.webmaster.yandex.net/v4/user'
    headers = {'Authorization': f'OAuth {TOKEN}'}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Ошибка получения user_id: {response.status_code} - {response.text}")
        return None
    return response.json()['user_id']

# Получение списка сайтов
def get_hosts(user_id):
    url = f'https://api.webmaster.yandex.net/v4/user/{user_id}/hosts'
    headers = {'Authorization': f'OAuth {TOKEN}'}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Ошибка получения хостов: {response.status_code} - {response.text}")
        return []
    hosts = response.json()['hosts']
    return hosts

# Получение списка URL
def get_urls(user_id, host_id, date_from, date_to, region_ids):
    urls = []
    offset = 0
    while True:
        body = {
            "date_from": date_from,
            "date_to": date_to,
            "limit": 500,
            "offset": offset,
            "text_indicator": "URL"
        }
        if region_ids:
            body["region_ids"] = region_ids
        response = get_query_analytics(user_id, host_id, body)
        current_batch = response.get('text_indicator_to_statistics', [])
        
        for item in current_batch:
            urls.append(item['text_indicator']['value'])
        
        if len(current_batch) < 500:
            break
        
        offset += 500
        time.sleep(SLEEP_TIME_API)
    
    return urls

# Получение данных через API с обработкой 429
def get_query_analytics(user_id, host_id, body):
    url = f'https://api.webmaster.yandex.net/v4/user/{user_id}/hosts/{host_id}/query-analytics/list'
    headers = {
        'Authorization': f'OAuth {TOKEN}',
        'Content-Type': 'application/json; charset=UTF-8'
    }
    while True:
        response = requests.post(url, headers=headers, json=body)
        if response.status_code == 429:
            print(f"Превышен лимит запросов (429). Ожидаем до следующего часа...")
            now = datetime.now()
            next_hour = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
            wait_seconds = (next_hour - now).total_seconds()
            print(f"Ожидание {wait_seconds} секунд...")
            time.sleep(wait_seconds)
            continue
        elif response.status_code != 200:
            print(f"Ошибка API: {response.status_code} - {response.text}")
            return {}
        return response.json()

# Преобразование host_id в читаемый формат для вывода
def format_host_for_output(host):
    return host.replace('https:', 'https://').replace(':443', '')

# Чтение и запись конфига
def load_processed_data():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if content:
                    return json.loads(content)
                else:
                    print(f"Файл {CONFIG_FILE} пустой. Используем пустой словарь.")
                    return {}
        except json.JSONDecodeError as e:
            print(f"Ошибка при чтении {CONFIG_FILE}: {e}. Используем пустой словарь.")
            return {}
        except Exception as e:
            print(f"Неизвестная ошибка при чтении {CONFIG_FILE}: {e}. Используем пустой словарь.")
            return {}
    print(f"Файл {CONFIG_FILE} не существует. Используем пустой словарь.")
    return {}

def save_processed_data(processed_data):
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        json.dump(processed_data, f, indent=2, ensure_ascii=False)

# Загрузка существующих данных
def load_all_time_full_data():
    columns = ['date', 'host_id' if not COLLECT_BY_URL else 'url', 'query', 'position', 'clicks', 'ctr', 'demand', 'impressions', 'region_ids']
    if STORAGE_TYPE == 'csv':
        if os.path.exists(ALL_TIME_FULL_FILE_CSV):
            try:
                df = pd.read_csv(ALL_TIME_FULL_FILE_CSV)
                return df[columns] if not df.empty else pd.DataFrame(columns=columns)
            except Exception as e:
                print(f"Ошибка при чтении {ALL_TIME_FULL_FILE_CSV}: {e}. Используем пустой DataFrame.")
                return pd.DataFrame(columns=columns)
        return pd.DataFrame(columns=columns)
    elif STORAGE_TYPE == 'sqlite':
        conn = sqlite3.connect(ALL_TIME_FULL_FILE_DB)
        try:
            df = pd.read_sql_query("SELECT * FROM query_stats", conn)
            return df[columns] if not df.empty else pd.DataFrame(columns=columns)
        except Exception as e:
            print(f"Ошибка при чтении {ALL_TIME_FULL_FILE_DB}: {e}. Используем пустой DataFrame.")
            return pd.DataFrame(columns=columns)
        finally:
            conn.close()

# Обновление данных
def update_all_time_full_data(new_data):
    if not new_data:
        return
    
    df_new = pd.DataFrame(new_data)
    columns = ['date', 'host_id' if not COLLECT_BY_URL else 'url', 'query', 'position', 'clicks', 'ctr', 'demand', 'impressions', 'region_ids']
    df_new = df_new[columns]
    
    if STORAGE_TYPE == 'csv':
        df_all_time = load_all_time_full_data()
        df_combined = pd.concat([df_all_time, df_new]).drop_duplicates(subset=['date', 'host_id' if not COLLECT_BY_URL else 'url', 'query'])
        df_combined.to_csv(ALL_TIME_FULL_FILE_CSV, index=False, encoding='utf-8-sig')
        print(f"Данные добавлены в {ALL_TIME_FULL_FILE_CSV}")
    elif STORAGE_TYPE == 'sqlite':
        conn = sqlite3.connect(ALL_TIME_FULL_FILE_DB)
        try:
            df_new.to_sql('query_stats', conn, if_exists='append', index=False, method='multi')
            print(f"Данные добавлены в {ALL_TIME_FULL_FILE_DB}")
        except Exception as e:
            print(f"Ошибка при обновлении {ALL_TIME_FULL_FILE_DB}: {e}")
        finally:
            conn.close()

# Проверка, полностью ли собран период
def is_period_fully_collected(df_all_time, key, date_from, date_to, region_ids=None):
    key_column = 'host_id' if not COLLECT_BY_URL else 'url'
    df_key = df_all_time[df_all_time[key_column] == key]
    if df_key.empty:
        return False
    
    # Если region_ids задан, проверяем совпадение регионов
    if region_ids:
        df_key = df_key[df_key['region_ids'] == str(region_ids)]
        if df_key.empty:
            return False
    
    dates_collected = pd.to_datetime(df_key['date']).dt.strftime('%Y-%m-%d').unique()
    date_from_dt = datetime.strptime(date_from, '%Y-%m-%d')
    date_to_dt = datetime.strptime(date_to, '%Y-%m-%d')
    expected_dates = [(date_from_dt + timedelta(days=x)).strftime('%Y-%m-%d') 
                      for x in range((date_to_dt - date_from_dt).days + 1)]
    
    return set(expected_dates).issubset(set(dates_collected))

# Основная функция
def main():
    global USER_ID, HOST_ID
    
    # Инициализация SQLite, если выбрано
    init_db()
    
    processed_data = load_processed_data()
    df_all_time = load_all_time_full_data()
    
    USER_ID = get_user_id()
    if not USER_ID:
        print("Не удалось получить USER_ID. Завершение программы.")
        return
    
    hosts = get_hosts(USER_ID)
    available_host_ids = [h['host_id'] for h in hosts]
    print("Доступные хосты из API:", available_host_ids)
    
    # Обработка HOST_ID в зависимости от его типа
    if isinstance(HOST_ID, dict):
        host_dict = {k: v for k, v in HOST_ID.items()}  # Оставляем как есть для API
        host_ids = []
        print("Хосты из HOST_ID:", list(HOST_ID.keys()))
        for host in host_dict.keys():
            if host in available_host_ids:
                host_ids.append(host)
            else:
                print(f"Хост {host} не найден в доступных хостах.")
        if not host_ids:
            print("Ни один хост из HOST_ID не найден в доступных хостах.")
            return
        print(f"Обрабатываем хосты из словаря HOST_ID с регионами: {host_dict}")
        print(f"Найденные host_ids для обработки: {host_ids}")
    elif isinstance(HOST_ID, list) and HOST_ID:
        host_dict = {h: REGION_IDS for h in HOST_ID}
        host_ids = [h for h in HOST_ID if h in available_host_ids]
        if not host_ids:
            print(f"Указанные в HOST_ID хосты не найдены. Завершение программы.")
            return
        print(f"Обрабатываем только указанные хосты: {HOST_ID} с регионами {REGION_IDS}")
    else:
        host_ids = [h['host_id'] for h in hosts if h['host_id'] not in EXCLUDED_HOSTS]
        host_dict = {h['host_id']: REGION_IDS for h in hosts}
        if not host_ids:
            print(f"После исключения хостов из EXCLUDED_HOSTS ничего не осталось. Завершение программы.")
            return
        print(f"Обрабатываем все хосты, кроме исключенных: {EXCLUDED_HOSTS} с регионами {REGION_IDS}")
    
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=13)
    date_from = start_date.strftime("%Y-%m-%d")
    date_to = end_date.strftime("%Y-%m-%d")
    
    all_data = []
    
    for host_id in host_ids:
        # Форматируем хост для вывода
        host_key = format_host_for_output(host_id)
        region_ids = host_dict.get(host_id, REGION_IDS)  # Используем оригинальный host_id для поиска регионов
        
        if COLLECT_BY_URL:
            print(f"Получение списка URL для {host_key} с регионами {region_ids}...")
            urls = get_urls(USER_ID, host_id, date_from, date_to, region_ids)
            print(f"Найдено URL: {len(urls)}")
            keys_to_process = [(url, f"{host_key}{url}") for url in urls]
        else:
            keys_to_process = [(None, host_key)]
        
        for url, key in keys_to_process:
            if key in processed_data and date_from in processed_data[key]:
                existing_data = processed_data[key][date_from]
                if isinstance(existing_data, dict) and 'date_to' in existing_data and existing_data['date_to'] == date_to:
                    if not region_ids or (region_ids and 'region_ids' in existing_data and existing_data['region_ids'] == region_ids):
                        print(f"Данные для {key} за период {date_from} - {date_to} с регионами {region_ids} уже собраны в {CONFIG_FILE}. Пропускаем.")
                        continue
            
            if is_period_fully_collected(df_all_time, key, date_from, date_to, region_ids):
                print(f"Данные для {key} за период {date_from} - {date_to} с регионами {region_ids} уже собраны. Пропускаем.")
                if key not in processed_data:
                    processed_data[key] = {}
                processed_data[key][date_from] = {'date_to': date_to}
                if region_ids:
                    processed_data[key][date_from]['region_ids'] = region_ids
                save_processed_data(processed_data)
                print(f"Конфиг синхронизирован для {key}")
                continue
            
            print(f"Обработка: {key} с регионами {region_ids}")
            offset = 0
            batch_count = 0
            total_batches = None
            key_data = []
            
            while True:
                body = {
                    "date_from": date_from,
                    "date_to": date_to,
                    "limit": 500,
                    "offset": offset,
                    "text_indicator": "QUERY"
                }
                if url:
                    body["filters"] = {
                        "text_filters": [
                            {"text_indicator": "URL", "operation": "TEXT_MATCH", "value": url}
                        ]
                    }
                if region_ids:
                    body["region_ids"] = region_ids
                
                data = get_query_analytics(USER_ID, host_id, body)
                current_batch = data.get('text_indicator_to_statistics', [])
                batch_count += 1
                
                if not current_batch:
                    print("Нет данных в text_indicator_to_statistics или ошибка в запросе.")
                    break
                
                if total_batches is None and current_batch:
                    total_batches = max(1, batch_count) if len(current_batch) < 500 else None
                
                if total_batches:
                    print(f"Обработка батча: {batch_count} из {total_batches}")
                else:
                    print(f"Обработка батча: {batch_count} (всего пока неизвестно)")
                
                for item in current_batch:
                    query = item.get('text_indicator', {}).get('value', 'N/A')
                    stats = item.get('statistics', [])
                    
                    stats_by_date = {}
                    for stat in stats:
                        date = stat.get('date', 'N/A')
                        field = stat.get('field')
                        value = stat.get('value', 'N/A')
                        if date not in stats_by_date:
                            stats_by_date[date] = {
                                'date': date,
                                'host_id' if not COLLECT_BY_URL else 'url': key,
                                'query': query,
                                'position': 'N/A',
                                'clicks': 'N/A',
                                'ctr': 'N/A',
                                'demand': 'N/A',
                                'impressions': 'N/A',
                                'region_ids': str(region_ids) if region_ids else 'N/A'
                            }
                        if field == 'POSITION':
                            stats_by_date[date]['position'] = value
                        elif field == 'CLICKS':
                            stats_by_date[date]['clicks'] = value
                        elif field == 'CTR':
                            stats_by_date[date]['ctr'] = value
                        elif field == 'DEMAND':
                            stats_by_date[date]['demand'] = value
                        elif field == 'IMPRESSIONS':
                            stats_by_date[date]['impressions'] = value
                    
                    # Фильтрация по COLLECT_ZERO_DEMAND
                    if COLLECT_ZERO_DEMAND:
                        stats_by_date = {
                            k: v for k, v in stats_by_date.items() 
                            if v['demand'] != 'N/A' and float(v['demand']) != 0
                        }
                    
                    key_data.extend(stats_by_date.values())
                
                if len(current_batch) < 500:
                    if total_batches is None:
                        total_batches = batch_count
                    print(f"Обработка батча: {batch_count} из {total_batches} (завершено)")
                    break
                
                offset += 500
                time.sleep(SLEEP_TIME_API)
            
            if key_data:
                all_data.extend(key_data)
                update_all_time_full_data(key_data)
            
            if key not in processed_data:
                processed_data[key] = {}
            processed_data[key][date_from] = {'date_to': date_to}
            if region_ids:
                processed_data[key][date_from]['region_ids'] = region_ids
            save_processed_data(processed_data)
            print(f"Конфиг обновлен для {key}")
    
    print(f"Собрано записей: {len(all_data)}")
    print("Пример данных:", all_data[:5] if all_data else "Нет данных")
    
    if not all_data:
        print("Нет данных для сохранения. Завершение программы.")
        return
    
    df_full = pd.DataFrame(all_data)
    columns = ['date', 'host_id' if not COLLECT_BY_URL else 'url', 'query', 'position', 'clicks', 'ctr', 'demand', 'impressions', 'region_ids']
    df_full = df_full[columns]
    df_full.to_csv(TEMP_CSV_FILE, index=False, encoding='utf-8-sig')
    print(f"Все данные за период {date_from} - {date_to} сохранены в файл {TEMP_CSV_FILE}")

if __name__ == "__main__":
    main()

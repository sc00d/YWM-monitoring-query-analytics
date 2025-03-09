#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# Параметры
TOKEN = 'тут ваш токен'
HOST_ID = ''  # Пустая строка для сбора по всем хостам или массив доменов с протоколом, например: ["https://domain.ru", "https://sub999.domain.ru"]
EXCLUDED_HOSTS = ["https://sub1.domain.ru", "https://sub2.domain.ru"]  # Исключенные хосты
SLEEP_TIME_API = 2
CONFIG_FILE = 'processed_data.json'
TEMP_CSV_FILE = f'temp_data_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.csv'  # Временный файл с уникальным именем
ALL_TIME_FULL_FILE = 'for-all-time-full-data-query_stats.csv'

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
    return response.json()['hosts']

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

# Нормализация host_id только для отображения
def normalize_host_id_for_display(host_id):
    return f"https://{host_id.replace('https:', '').replace(':443', '').replace(':', '/')}"

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

# Загрузка существующих данных из all-time-full-data
def load_all_time_full_data():
    if os.path.exists(ALL_TIME_FULL_FILE):
        try:
            df = pd.read_csv(ALL_TIME_FULL_FILE)
            return df
        except Exception as e:
            print(f"Ошибка при чтении {ALL_TIME_FULL_FILE}: {e}. Используем пустой DataFrame.")
            return pd.DataFrame(columns=['date', 'host_id', 'query', 'position', 'clicks', 'ctr', 'demand', 'impressions'])
    return pd.DataFrame(columns=['date', 'host_id', 'query', 'position', 'clicks', 'ctr', 'demand', 'impressions'])

# Обновление all-time-full-data
def update_all_time_full_data(new_data):
    df_new = pd.DataFrame(new_data)
    df_new = df_new[['date', 'host_id', 'query', 'position', 'clicks', 'ctr', 'demand', 'impressions']]
    
    df_all_time = load_all_time_full_data()
    df_combined = pd.concat([df_all_time, df_new]).drop_duplicates(subset=['date', 'host_id', 'query'])
    
    df_combined.to_csv(ALL_TIME_FULL_FILE, index=False, encoding='utf-8-sig')
    print(f"Данные добавлены в {ALL_TIME_FULL_FILE}")

# Проверка, полностью ли собран период для host_id
def is_period_fully_collected(df_all_time, host_id, date_from, date_to):
    df_host = df_all_time[df_all_time['host_id'] == host_id]
    if df_host.empty:
        return False
    
    dates_collected = pd.to_datetime(df_host['date']).dt.strftime('%Y-%m-%d').unique()
    date_from_dt = datetime.strptime(date_from, '%Y-%m-%d')
    date_to_dt = datetime.strptime(date_to, '%Y-%m-%d')
    expected_dates = [(date_from_dt + timedelta(days=x)).strftime('%Y-%m-%d') 
                      for x in range((date_to_dt - date_from_dt).days + 1)]
    
    return set(expected_dates).issubset(set(dates_collected))

# Основная функция
def main():
    global USER_ID, HOST_ID
    
    # Загрузка данных о предыдущих обработках
    processed_data = load_processed_data()
    df_all_time = load_all_time_full_data()
    
    # Получение USER_ID
    USER_ID = get_user_id()
    if not USER_ID:
        print("Не удалось получить USER_ID. Завершение программы.")
        return
    
    hosts = get_hosts(USER_ID)
    print("Доступные хосты:", hosts)
    
    # Определяем host_ids для обработки
    if isinstance(HOST_ID, list) and HOST_ID:
        normalized_host_ids = [normalize_host_id_for_display(h['host_id']) for h in hosts]
        host_ids = [h['host_id'] for h in hosts if normalized_host_ids[hosts.index(h)] in HOST_ID]
        if not host_ids:
            print(f"Указанные в HOST_ID хосты не найдены в списке доступных хостов. Завершение программы.")
            return
        print(f"Обрабатываем только указанные хосты: {HOST_ID}")
    else:
        host_ids = [h['host_id'] for h in hosts if normalize_host_id_for_display(h['host_id']) not in EXCLUDED_HOSTS]
        if not host_ids:
            print(f"После исключения хостов из EXCLUDED_HOSTS ничего не осталось для обработки. Завершение программы.")
            return
        print(f"Обрабатываем все хосты, кроме исключенных: {EXCLUDED_HOSTS}")
    
    # Период данных (последние 14 дней)
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=13)
    date_from = start_date.strftime("%Y-%m-%d")
    date_to = end_date.strftime("%Y-%m-%d")
    
    all_data = []
    
    for host_id in host_ids:
        host_key = normalize_host_id_for_display(host_id)
        
        # Проверка через processed_data.json (приоритетная)
        if host_key in processed_data and date_from in processed_data[host_key] and processed_data[host_key][date_from] == date_to:
            print(f"Данные для {host_key} за период {date_from} - {date_to} уже отмечены как собранные в {CONFIG_FILE}. Пропускаем.")
            continue
        
        # Дополнительная проверка через all-time-full-data, если в конфиге нет записи
        if is_period_fully_collected(df_all_time, host_key, date_from, date_to):
            print(f"Данные для {host_key} за период {date_from} - {date_to} уже полностью собраны в {ALL_TIME_FULL_FILE}. Пропускаем.")
            # Добавляем запись в конфиг, чтобы синхронизировать его с CSV
            if host_key not in processed_data:
                processed_data[host_key] = {}
            processed_data[host_key][date_from] = date_to
            save_processed_data(processed_data)
            print(f"Конфиг синхронизирован для {host_key}")
            continue
        
        print(f"Обработка хоста: {host_id}")
        normalized_host_id = normalize_host_id_for_display(host_id)
        
        offset = 0
        batch_count = 0
        total_batches = None
        host_data = []
        
        while True:
            body = {
                "date_from": date_from,
                "date_to": date_to,
                "limit": 500,
                "offset": offset,
                "text_indicator": "QUERY"
            }
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
                            'host_id': normalized_host_id,
                            'query': query,
                            'position': 'N/A',
                            'clicks': 'N/A',
                            'ctr': 'N/A',
                            'demand': 'N/A',
                            'impressions': 'N/A'
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
                
                host_data.extend(stats_by_date.values())
            
            if len(current_batch) < 500:
                if total_batches is None:
                    total_batches = batch_count
                print(f"Обработка батча: {batch_count} из {total_batches} (завершено)")
                break
            
            offset += 500
            time.sleep(SLEEP_TIME_API)
        
        if host_data:
            all_data.extend(host_data)
            update_all_time_full_data(host_data)
        
        # Обновление конфига после обработки хоста
        if host_key not in processed_data:
            processed_data[host_key] = {}
        processed_data[host_key][date_from] = date_to
        save_processed_data(processed_data)
        print(f"Конфиг обновлен для {host_key}")
    
    print(f"Собрано записей: {len(all_data)}")
    print("Пример данных:", all_data[:5] if all_data else "Нет данных")
    
    if not all_data:
        print("Нет данных для сохранения в CSV. Завершение программы.")
        return
    
    # Сохранение всех данных в один временный CSV
    df_full = pd.DataFrame(all_data)
    print("Столбцы в DataFrame:", df_full.columns.tolist())
    df_full = df_full[['date', 'host_id', 'query', 'position', 'clicks', 'ctr', 'demand', 'impressions']]
    df_full.to_csv(TEMP_CSV_FILE, index=False, encoding='utf-8-sig')
    print(f"Все данные за период {date_from} - {date_to} сохранены в файл {TEMP_CSV_FILE}")

if __name__ == "__main__":
    main()


# In[ ]:





# Импортируем необходимые библиотеки и функции
from dotenv import load_dotenv  # Для загрузки переменных окружения из .env файла
import os  # Для работы с операционной системой, получения переменных окружения
import requests  # Для отправки HTTP-запросов
import pandas as pd  # Для работы с данными в формате таблиц (DataFrame)
from functions import generate_dadata_query, response_dadata, response_processing, saving_csv, resultation

try:
    # Загружаем переменные окружения из файла .env
    load_dotenv()

    # Получаем API-ключ из переменной окружения
    API_KEY = os.getenv("DADATA_API_KEY")

    if not API_KEY:
        raise ValueError("API_KEY не найден в .env файле.")

    # URL API для получения данных по юрлицам
    API_URL = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/party_by"

    # Заголовки для HTTP-запроса (с сайта Dadata)
    headers = {
        "Content-Type": "application/json",  # Указываем, что отправляемые данные будут в формате JSON
        "Accept": "application/json",        # Ожидаем получить ответ в формате JSON
        "Authorization": f"Token {API_KEY}"  # Передаём API-ключ для авторизации
    }


    # Генерация запросов для каждого статуса
    query = "ООО"
    data_active = generate_dadata_query(query, "ACTIVE")
    data_liquidating = generate_dadata_query(query, "LIQUIDATING")
    data_liquidated = generate_dadata_query(query, "LIQUIDATED")
    data_bankrupt = generate_dadata_query(query, "BANKRUPT")
    data_suspended = generate_dadata_query(query, "SUSPENDED")
    data_reorganizing = generate_dadata_query(query, "REORGANIZING")

    # Получение данных для каждого статуса
    response_active = response_dadata(data_active)
    response_liquidating = response_dadata(data_liquidating)
    response_liquidated = response_dadata(data_liquidated)
    response_bankrupt = response_dadata(data_bankrupt)
    response_suspended = response_dadata(data_suspended)
    response_reorganizing = response_dadata(data_reorganizing)
    
    # Обработка ответов для каждого статуса
    df_active = response_processing(response_active)
    df_liquidating = response_processing(response_liquidating)
    df_liquidated = response_processing(response_liquidated)
    df_bankrupt = response_processing(response_bankrupt)
    df_suspended = response_processing(response_suspended)
    df_reorganizing = response_processing(response_reorganizing)

    # Сохранение DataFrame для каждого статуса в соответствующие файлы
    saving_csv(df_active, 'companies_active')
    saving_csv(df_liquidating, 'companies_liquidating')
    saving_csv(df_liquidated, 'companies_liquidated')
    saving_csv(df_bankrupt, 'companies_bankrupt')
    saving_csv(df_suspended, 'companies_suspended')
    saving_csv(df_reorganizing, 'companies_reorganizing')

    # Список городов для поиска
    cities = ["Минск", "Витебск", "Могилев", "Гомель", "Брест", "Гродно"]
    # Список областей для поиска
    regions = ["Брестская", "Витебская", "Гомельская", "Гродненская", "Минская", "Могилёвская"]

    # Применение функции resultation для каждого DataFrame
    resultation(df_active, 'companies_active_result')
    resultation(df_liquidating, 'companies_liquidating_result')
    resultation(df_liquidated, 'companies_liquidated_result')
    resultation(df_bankrupt, 'companies_bankrupt_result')
    resultation(df_suspended, 'companies_suspended_result')
    resultation(df_reorganizing, 'companies_reorganizing_result')


except requests.exceptions.RequestException as e:
    print(f"Ошибка запроса: {e}")  # Обработка ошибок запросов

except KeyError as e:
    print("Ключ не найден в данных:", e)  # Обработка ошибки, если в данных отсутствует ключ

except ValueError as e:
    print("Ошибка при парсинге JSON:", e)  # Обработка ошибки при парсинге JSON

else:
    print('Программа выполнена успешно')  # Сообщение об успешном завершении программы

finally:
    print('Процесс окончен')  # Сообщение о завершении процесса





# Импортируем необходимые библиотеки
from dotenv import load_dotenv  # Для загрузки переменных окружения из .env файла
import os  # Для работы с операционной системой, получения переменных окружения
import requests  # Для отправки HTTP-запросов
import pandas as pd  # Для работы с данными в формате таблиц (DataFrame)
from requests.exceptions import RequestException

try:
    # Загружаем переменные окружения из файла .env
    load_dotenv()

    # Получаем API-ключ из переменной окружения
    API_KEY = os.getenv("DADATA_API_KEY")

    # URL API для получения данных по юрлицам
    API_URL = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/party_by"

    # Заголовки для HTTP-запроса (с сайта Dadata)
    headers = {
        "Content-Type": "application/json",  # Указываем, что отправляемые данные будут в формате JSON
        "Accept": "application/json",        # Ожидаем получить ответ в формате JSON
        "Authorization": f"Token {API_KEY}"  # Передаём API-ключ для авторизации
    }

    # Данные, которые мы отправляем в запросе
    data = {
        "query": "0",  # Запрос, данный параметр обязательный
        "count": 20,  # Ограничение на количество возвращаемых результатов, Dadata по данному запросу выводим максимум 20 результатов
        "filters": [
            {"type": "LEGAL"}  # Фильтруем только юридические лица
        ]
    }

    # Отправляем POST-запрос с данными и заголовками
    response = requests.post(API_URL, headers=headers, json=data) # json ответ, но результат в виде строки

    # Проверяем, если запрос успешен (статус код 200)
    if response.status_code == 200:
        # Получаем данные в формате JSON, поскольку response - это строка
        response_data = response.json()

        # Список для хранения отфильтрованных данных
        filtered_data = []

        # Проходим по всем предложениям в ответе
        for suggestion in response_data['suggestions']:
            # Извлекаем данные из каждого предложения
            data = suggestion['data']
            
            # Добавляем нужные данные в список
            filtered_data.append({
                'value': suggestion['value'],  # Название компании
                'unp': data['unp'],  # УНП (уникальный номер предприятия)
                'registration_date': data['registration_date'],  # Дата регистрации
                'removal_date': data['removal_date'],  # Дата исключения из реестра
                'status': data['status'],  # Статус компании
                'full_name_ru': data['full_name_ru'],  # Полное название на русском
                'trade_name_ru': data['trade_name_ru'],  # Торговое название на русском
                'address': data['address'],  # Адрес компании
                'oked': data['oked'],  # ОКЭД (код экономической деятельности)
                'oked_name': data['oked_name']  # Описание ОКЭД
            })

        # Преобразуем отфильтрованные данные в DataFrame
        df = pd.DataFrame(filtered_data)

        # Сохраняем DataFrame в CSV-файл (без индекса и с правильной кодировкой)
        df.to_csv('companies.csv', index=False, encoding='utf-8-sig')



        # Список городов для поиска
        cities = ["Минск", "Витебск", "Могилев", "Гомель", "Брест", "Гродно"]

        # Список областей для поиска
        regions = ["Брестская", "Витебская", "Гомельская", "Гродненская", "Минская", "Могилёвская"]

        # Функция для извлечения города или области из адреса
        def extract_city_or_region(address):
            found_region = None
            found_city = None
            
            # Сначала ищем область
            for region in regions:
                if region in address:
                    found_region = f"{region} область"
                    break  # Прерываем цикл, так как область уже найдена

            # Если область не найдена, ищем город
            for city in cities:
                if city in address:
                    found_city = city
                    break  # Прерываем цикл, так как город найден

            # Если и город, и область найдены, решаем, что из этого возвращать
            if found_region and found_city:
                return found_city 
            elif found_region:
                return found_region
            elif found_city:
                return found_city

            return "Не определено"  # Если ни город, ни область не найдены

        # Применяем функцию к каждому значению в столбце "address" для извлечения города или области
        df["city_or_region"] = df["address"].apply(extract_city_or_region)

        # Подсчитываем количество зарегистрированных юрлиц по городам и областям
        result = df["city_or_region"].value_counts()

        # Записываем результат в файл 'result.txt'
        result.to_csv('result.csv', header=True, index=True, sep='\t')


except requests.RequestException as e:
    # Ловим ошибки связанные с запросом (нет сети, таймаут и т.д.)
    print("Ошибка:", e)

except KeyError as e:
    # Ловим ошибки доступа к несуществующему ключу в данных
    print("Ключ не найден в данных:", e)

except ValueError as e:
    # Ловим ошибки декодирования JSON
    print("Ошибка при парсинге JSON:", e)

else:
    print('Программа выполнена успешно')

finally:
    print('Процесс окончен')



# Функция для генерации запроса для Dadata по статусу юрлица
def generate_dadata_query(query, status, count=20):
    """
    Генерирует данные запроса для Dadata по заданному статусу.

    param query: Запрос (обязательный параметр)
    param status: Статус юридического лица (ACTIVE, LIQUIDATING, LIQUIDATED, BANKRUPT, SUSPENDED, REORGANIZING)
    param count: Количество возвращаемых результатов (по умолчанию 20)
    return: Словарь с параметрами запроса
    """
    # Список допустимых статусов
    valid_statuses = ["ACTIVE", "LIQUIDATING", "LIQUIDATED", "BANKRUPT", "SUSPENDED", "REORGANIZING"]
        
    # Проверка, что переданный статус правильный
    if status not in valid_statuses:
            raise ValueError(f"Неверный статус: {status}. Доступные статусы: {valid_statuses}")

    # Формируем запрос в виде словаря
    return {
        "query": query,
        "count": count,
        "filters": [{"status": status}]
    }

# Функция для отправки запроса к Dadata API
def response_dadata(data):
    try:
        response = requests.post(API_URL, headers=headers, json=data)  # Отправка POST-запроса
        response.raise_for_status()  # Проверка на успешный ответ (код 2xx)
        return response.json()  # Возвращаем JSON-ответ
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при запросе: {e}")  # В случае ошибки выводим сообщение

# Функция для обработки ответа от Dadata
def response_processing(response_data):
    """
    Обрабатывает ответ Dadata, фильтрует нужные данные и возвращает DataFrame.
        
    param response_data: JSON-ответ от Dadata
    return: DataFrame с отфильтрованными данными
    """
    # Проверка на корректность ответа
    if not response_data or 'suggestions' not in response_data or not response_data['suggestions']:
        print("Ошибка: данных нет в файле")
        return pd.DataFrame()  # Возвращаем пустой DataFrame, если данные некорректны

    filtered_data = []  # Список для хранения отфильтрованных данных

    # Перебираем все предложения и выбираем необходимые поля
    for suggestion in response_data['suggestions']:
        data = suggestion['data']
        filtered_data.append({
            'value': suggestion.get('value', ''),  # Название компании
            'unp': data.get('unp', ''),  # УНП
            'registration_date': data.get('registration_date', ''),  # Дата регистрации
            'removal_date': data.get('removal_date', ''),  # Дата исключения
            'status': data.get('status', ''),  # Статус
            'full_name_ru': data.get('full_name_ru', ''),  # Полное название
            'trade_name_ru': data.get('trade_name_ru', ''),  # Торговое название
            'address': data.get('address', ''),  # Адрес
            'oked': data.get('oked', ''),  # ОКЭД
            'oked_name': data.get('oked_name', '')  # Описание ОКЭД
        })
    return pd.DataFrame(filtered_data)  # Возвращаем DataFrame с данными

# Функция для сохранения DataFrame в CSV
def saving_csv(df, file_name):
    """
    Сохраняет DataFrame в CSV-файл с указанным именем.

    param df: DataFrame для сохранения
    param file_name: Имя файла для сохранения (без расширения)
    """
    df.to_csv(f'/Users/ilonakononovic/PycharmProjects/DaData-project/data/{file_name}.csv', index=False, encoding='utf-8-sig')  # Сохраняем в формате CSV без индекса и с кодировкой UTF-8 для корректного отображения символов
    
# Функция для извлечения города или области из адреса
def extract_city_or_region(address):
    if not address:
        return "Адрес не определен"  # Если адрес пустой, возвращаем сообщение
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
        return found_city  # Если найден и город, и область, возвращаем город
    elif found_region:
        return found_region  # Если найдена только область
    elif found_city:
        return found_city  # Если найден только город

    return "Не определено"  # Если ни город, ни область не найдены

# Функция для подсчета и сохранения количества городов и регионов
def resultation(df_name, file_name):
    if df_name.empty:  # Проверяем, если DataFrame пустой
        print(f"Файл {file_name} пустой. Пропускаем обработку.")  # Если файл пустой, выводим сообщение
        return  # Прерываем выполнение функции, если DataFrame пустой

    df_name["city_or_region"] = df_name["address"].apply(extract_city_or_region)  # Применяем функцию для извлечения города или региона из адреса
    result = df_name["city_or_region"].value_counts()  # Подсчитываем количество уникальных значений для каждого города или региона
    result.to_csv(f'/Users/ilonakononovic/PycharmProjects/DaData-project/data/{file_name}.csv', header=True, index=True, sep='\t')  # Сохраняем результат в CSV файл, разделяя значения табуляцией
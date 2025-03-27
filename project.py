from dotenv import load_dotenv
import os
import requests
import pandas as pd



load_dotenv()  # Загружает переменные из .env
API_KEY = os.getenv("DADATA_API_KEY")  # Получаем ключ
API_URL = "https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/party_by"

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Authorization": f"Token {API_KEY}"  # Авторизация с API-ключом
}

data = {
    "query": "0",
    "count": 10,
    "filters": [
        {"type": "LEGAL"}
    ]
}

response = requests.post(API_URL, headers=headers, json=data)
if response.status_code ==200:
    response_data = response.json()
    filtered_data = []
    for suggestion in response_data['suggestions']:
        data = suggestion['data']
        filtered_data.append({
            'value': suggestion['value'],
            'unp': data['unp'],
            'registration_date': data['registration_date'],
            'removal_date': data['removal_date'],
            'status': data['status'],
            'full_name_ru': data['full_name_ru'],
            'trade_name_ru': data['trade_name_ru'],
            'address': data['address'],
            'oked': data['oked'],
            'oked_name': data['oked_name']
        })
    # Преобразуем в DataFrame
    df = pd.DataFrame(filtered_data)
    df.to_csv('companies.csv', index=False, encoding='utf-8-sig')

    # Печать итогового DataFrame
    print(df)


else:
    print(f"Ошибка: {response.text}")



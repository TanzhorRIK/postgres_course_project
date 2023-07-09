import requests
import json
import os
import time
class HHVacancyAPI:
    """Класс для подключения к API hh.ru"""

    def __init__(self, search_text, region=1202) -> None:
        self.search_text = search_text
        self.region = region

    def connect(self, page: int = 0) -> str:
        """Метод для подключения к API"""

        params = {
            'text': 'NAME:' + self.search_text,
            'area': self.region,
            'page': page,
            'per_page': 100
        }
        try:
            req = requests.get('https://api.hh.ru/vacancies', params)
            data = req.content.decode()
            req.close()
            return data
        except:
            print("failed to connect to the server")

    def download_vacancies(self) -> None:
        """метод для получения вакансий с API в файл"""

        try:
            os.mkdir("../vacanci")
        except:
            pass
        for page in range(0, 20):

            jsObj = json.loads(self.connect(page))

            f = open("../vacanci/hh.json", mode='w', encoding='utf8')

            with open("../vacanci/hh.json", mode='w', encoding='utf8') as f:
                f.write(json.dumps(jsObj, ensure_ascii=False, indent=4,
                                   separators=(",", ":")))

            if (jsObj['pages'] - page) <= 1:
                break

            # Необязательная задержка, но чтобы не нагружать сервисы hh
            time.sleep(0.25)
    def get_vacancies(self):
        """Метод для получения данных по вакансиям из json-файла"""

        with open("../vacanci/hh.json", "r", encoding='utf8') as f:
            data = json.load(f)

        return data

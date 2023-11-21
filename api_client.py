import json
import requests


class HeadHunterApi:
    """Класс для получения вакансий с платформы HeadHunter"""

    file_to_save = 'HH_vacancies.json'
    url = "https://api.hh.ru/vacancies"
    headers = {"User-Agent": "AnyApp/1.0"}

    def __init__(self, employer_id=None):
        self.params = {"employer_id": employer_id, "area": "113", "only_with_vacancies": True}

    def get_vacancies_list(self):
        """создает запрос на платформу и получает список словарей с вакансиями в json-формате"""
        response = requests.get(self.url, params=self.params, headers=self.headers)
        if response.status_code == 200:
            return response.json()['items']
        else:
            print(f'Ошибка запроса к сайту HeadHunter {response.status_code}')

    def add_to_json(self, data, filename=file_to_save):
        """Сохраняет данные из списка словарей в заданном формате """
        with open(filename, 'w', encoding='utf8') as file:
            data_list = []
            for dict_ in data:
                temp_dict = {'company_id': dict_['employer']['id'], 'company': dict_['employer']['name'],
                             'employee': dict_['name'], 'city': dict_['area']['name'],
                             'salary': dict_.get('salary'), 'url': dict_['alternate_url'],
                             'requirement': dict_['snippet']['requirement']}
                data_list.append(temp_dict)
            json.dump(data_list, file, indent=2, ensure_ascii=False)

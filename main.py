import psycopg2
from api_client import HeadHunterApi
from db_manager import DBManager
import json
from config import config


"""Задаем стартовые параметры соединения, имя базы данных и таблицы"""

params = config()
db_name = 'hh_vacancies'

connection = psycopg2.connect(**params)
cur = connection.cursor()
connection.autocommit = True

"""Создаем базу данных с заданным именем, разрываем соединение"""

cur.execute(f'DROP DATABASE IF EXISTS {db_name};')
cur.execute(
    f'CREATE DATABASE {db_name} WITH OWNER = postgres ENCODING = "utf8" CONNECTION LIMIT = -1 IS_TEMPLATE = False;')
connection.commit()
cur.close()
connection.close()
params.update(database=db_name)


"""Создаем экземпляр класса DBManager для подключения к БД и работы с ней"""

db = DBManager(**params)
db.create_companies_table()
db.create_vacancies_table()

"""Список id заранее выбраных компаний"""

employer_ides = ['9498120', '592442', '15478', '1272486', '816144', '39305', '1491512', '2180', '797671', '9418714']

"""Для каждого id получаем 20 (по умолчанию) открытых вакансий  и добавляем их в таблицу"""

for emp_id in employer_ides:
    hh_vacancies = HeadHunterApi(emp_id)
    data = hh_vacancies.get_vacancies_list()
    hh_vacancies.add_to_json(data)
    id_list = []    #  Список для проверки уникальности id компаний
    with open(hh_vacancies.file_to_save, encoding='utf8') as f:
        data = json.load(f)
        for dict_ in data:
            """Выполняется проверка на значения salary.
            Если данные не указаны работодателем - значения приравниваются к 0
            для возможности дальнейшего сравнения"""
            if not dict_['salary']:
                salary_from = 0
                salary_to = 0
                currency = 'нет'
            else:
                salary_from = dict_['salary']['from'] if dict_['salary']['from'] else 0
                salary_to = dict_['salary']['to'] if dict_['salary']['to'] else 0
                currency = dict_['salary']['currency']

            company_id = dict_['company_id']

            """Создаем временные списки для добавления данных в таблицы"""
            if company_id not in id_list:
                temp_company_list = [company_id, dict_['company']]
                db.fill_companies_table(temp_company_list)
                id_list.append(company_id)

            temp_vacancy_list = [company_id, dict_['employee'], dict_['city'], salary_from, salary_to, currency,
                                 dict_['url'], dict_['requirement']]
            db.fill_vacancies_table(temp_vacancy_list)


def data_base_usage(db_object):
    """Функция для выполнения запросов к базе данных
    В качестве аргумента получает объект класса DBManager"""

    while True:
        print()
        print("Выберите действие:")
        print("1 - Получить список всех компаний и количество вакансий у каждой компании")
        print("2 - Получить список всех вакансий")
        print("3 - Получить среднюю зарплату по вакансиям")
        print("4 - Получить список вакансий с зарплатой выше средней по всем вакансиям")
        print("5 - Получить вакансии по ключевому слову")
        print("0 - Завершение работы")

        answer = input()
        if answer == '0':
            print('Работа программы завершена')
            break
        elif answer == '1':
            db_object.get_companies_and_vacancies_count()
        elif answer == '2':
            db_object.get_all_vacancies()
        elif answer == '3':
            db_object.get_avg_salary()
        elif answer == '4':
            db_object.get_vacancies_with_higher_salary()
        elif answer == '5':
            keyword = input('Введите ключевое слово\n')
            db_object.get_vacancies_with_keyword(keyword)
        else:
            print('Некорректный ввод')


data_base_usage(db)
db.connection.close()


"""Нормализовать таблицу вакансий с помощью создания отдельной таблицы с компаниями и установки зависимостей:


SELECT DISTINCT company_id, company_name INTO companies FROM vacancies;

ALTER TABLE companies ADD PRIMARY KEY (company_id);

ALTER TABLE vacancies ADD CONSTRAINT fk_vacancies_company_id FOREIGN KEY (company_id)
REFRENCES companies (company_id);

ALTER TABLE vacancies DROP COLUMN company_name;  (или не удалять, чтобы запросы не переписывать) """




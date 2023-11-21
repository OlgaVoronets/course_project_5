import psycopg2


class DBManager:

    """Класс для работы с базой данных вакансий.
       По умолчанию заданы БД hh_vacancies и таблица vacancies. """

    def __init__(self, database='hh_vacancies', host='localhost', user='postgres', password='12345', port='5432'):
        self.connection = psycopg2.connect(host=host, database=database, user=user, password=password, port=port)
        self.cur = self.connection.cursor()
        self.sql = ''


    def execute_query(self):
        """Выполняет запрос"""
        self.cur.execute(self.sql)
        self.connection.commit()

    def print_query_result(self):
        """Вывовдит запрос на печать """
        rows = self.cur.fetchall()
        if rows:
            for row in rows:
                print(*row)
        else:
            print("По вашему запросу ничего не найдено")

    def create_vacancy_table(self) -> None:
        """Создает таблицу вакансий"""

        self.sql = (f'CREATE TABLE vacancies '
                    f'(company_id varchar,'
                    f'company_name varchar,'
                    f'employee varchar,'
                    f'city varchar, '
                    f'salary_from int,'
                    f'salary_to varchar(10),'
                    f'currency varchar(5),'
                    f'url varchar,'
                    f'description text);')
        self.execute_query()

        """ with connection.cursor() as cursor:
        cursor.execute('CREATE TABLE companies('
                       'company_id serial PRIMARY KEY,'
                       'company_name varchar(50) NOT NULL,'
                       'description text,'
                       'link varchar(200) NOT NULL,'
                       'url_vacancies varchar(200) NOT NULL)')

        cursor.execute('CREATE TABLE vacancies('
                       'vacancy_id serial PRIMARY KEY,'
                       'company_id int REFERENCES companies (company_id) NOT NULL,'
                       'title_vacancy varchar(150) NOT NULL,'
                       'salary int,'
                       'link varchar(200) NOT NULL,'
                       'description text,'
                       'experience varchar(70))')"""


    def fill_vacancy_table(self, params):
        self.sql = "INSERT INTO vacancies VALUES(%s, '%s', '%s', '%s', %s, %s, '%s', '%s', '%s')" % tuple(params)
        self.execute_query()

    def get_companies_and_vacancies_count(self):
        """Получает список всех компаний и количество вакансий у каждой компании"""
        self.sql = f'SELECT company_name, COUNT(*) FROM vacancies GROUP BY company_name;'
        self.execute_query()
        self.print_query_result()

    def get_all_vacancies(self):
        """Получает список всех вакансий с указанием названия компании, названия
        вакансии и зарплаты и ссылки на вакансию"""
        self.sql = f'SELECT * FROM vacancies;'
        self.execute_query()
        self.print_query_result()

    def get_avg_salary(self):
        """Получает среднюю зарплату по вакансиям.
           Расчет идет по минимальной зарплате (столбец salary_from)"""
        self.sql = 'SELECT AVG(salary_from) FROM vacancies;'
        self.execute_query()
        self.print_query_result()

    def get_vacancies_with_higher_salary(self):
        """Получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
           Расчет идет по минимальной зарплате (столбец salary_from)"""
        self.sql = 'SELECT * FROM vacancies WHERE salary_from > (SELECT AVG(salary_from) FROM vacancies);'
        self.execute_query()
        self.print_query_result()

    def get_vacancies_with_keyword(self, keyword: str):
        """Получает список всех вакансий, в названии которых содержатся переданные в метод слова"""
        self.sql = f"SELECT * FROM vacancies WHERE employee LIKE '%{keyword}%';"
        self.execute_query()
        self.print_query_result()

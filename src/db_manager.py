import psycopg2


class DBManager:
    def __init__(self, host="localhost", database="data_hh", user="postgres",
                 password="1234567"):
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.connection = None

    def create_database(self):
        """Метод для создания БД"""

        try:
            connection = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            connection.autocommit = True
            cursor = connection.cursor()

            # Создание новой базы данных
            cursor.execute(f"CREATE DATABASE {self.database}")

            # Закрытие соединения с базой данных "postgres"
            cursor.close()
            connection.close()
            print(f"Database '{self.database}' created successfully!")

        except psycopg2.Error as e:
            print(f"Error creating database: {e}")

    def delete_database(self, database_name):
        """Метод для удаления БД"""

        try:
            connection = psycopg2.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
            connection.autocommit = True
            cursor = connection.cursor()

            # Проверка существования базы данных
            cursor.execute(
                "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s",
                (database_name,))
            exists = cursor.fetchone()

            if exists:
                # Удаление базы данных
                cursor.execute(f"DROP DATABASE {database_name}")
                print(f"Database '{database_name}' deleted successfully!")
            else:
                print(f"Database '{database_name}' does not exist!")

            # Закрытие соединения с базой данных "postgres"
            cursor.close()
            connection.close()

        except psycopg2.Error as e:
            print(f"Error deleting database: {e}")

    def connect(self):
        """Метод для установления соединения с БД"""

        try:
            self.connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password
            )
            print("Connected to the database!")

        except psycopg2.Error as e:
            print(f"Error connecting to the database: {e}")

    def disconnect(self):
        """Метод для разрыва соединения с БД"""

        if self.connection:
            self.connection.close()
            print("Disconnected from the database!")

    def execute_query(self, query):
        """Метод для выполнения запроса к БД"""

        if self.connection:
            try:
                cursor = self.connection.cursor()
                cursor.execute(query)
                self.connection.commit()
                cursor.close()
            except psycopg2.Error as e:
                print(f"Error executing query: {e}")

        else:
            print("Not connected to the database!")

    def create_tables(self):
        """Метод для создания двух таблиц в БД"""

        # Создание таблицы employers
        create_employers_table_query = '''
        CREATE TABLE employers (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL)
        '''
        self.execute_query(create_employers_table_query)

        # Создание таблицы vacancies
        create_vacancies_table_query = '''
        CREATE TABLE vacancies (
            id SERIAL PRIMARY KEY,
			vacansies_name varchar(255) NOT NULL,
            employer_id int NOT NULL,
            url text,
            salary_min int,
			salary_max int,
            FOREIGN KEY (employer_id) REFERENCES employers (id))
        '''
        self.execute_query(create_vacancies_table_query)

        print("Tables created successfully!")

    def drop_tables(self):
        """Метод для удаления таблиц из БД"""

        # Удаление таблиц employers и vacancies
        drop_employers_table_query = '''
        DROP TABLE IF EXISTS employers CASCADE
        '''
        self.execute_query(drop_employers_table_query)

        drop_vacancies_table_query = '''
        DROP TABLE IF EXISTS vacancies CASCADE
        '''
        self.execute_query(drop_vacancies_table_query)

        print("Tables dropped successfully!")

    def add_data_to_db(self, data):
        """Метод для добавления данных в таблицы БД"""

        try:
            cursor = self.connection.cursor()
            for item in data['items']:
                employer = item['employer']
                cursor.execute(
                    "INSERT INTO employers (id, name) "
                    "VALUES (%s, %s) "
                    "ON CONFLICT (id) DO NOTHING",
                    (employer['id'], employer['name']))
                self.connection.commit()

                if item.get('salary') is not None:
                    salary_from = item['salary'].get('from')
                    salary_to = item['salary'].get('to')
                else:
                    salary_from = None
                    salary_to = None

                cursor.execute(
                    "INSERT INTO vacancies (id, vacansies_name, employer_id, salary_min, salary_max, url) "
                    "VALUES (%s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (id) DO NOTHING",
                    (item['id'], item['name'], item['employer']['id'],
                     salary_from,
                     salary_to, item['url']))
                self.connection.commit()

            print("Data added to the database successfully!")

        except psycopg2.Error as e:
            print(f"Error adding data to the database: {e}")

    def get_companies_and_vacancies_count(self) -> list:
        """метод для получения списка всех компаний и количество вакансий у каждой компании"""

        query = "SELECT vacansies_name, COUNT(*) FROM vacancies GROUP BY vacansies_name"
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result
        return None

    def get_all_vacancies(self) -> list:
        """ Метод для получения списка всех вакансий с указанием названия компании,
                названия вакансии и зарплаты и ссылки на вакансию"""

        query = '''SELECT employers.name, vacansies_name, salary_max, url
                                     FROM vacancies
                                     JOIN employers ON employers.id=vacancies.employer_id
                                     WHERE salary_max IS NOT NULL
                                     ORDER BY salary_max DESC, vacansies_name'''

        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result
        return None

    def get_avg_salary(self) -> int:
        """ Метод для получения средней зарплаты по вакансиям"""

        query = '''SELECT ROUND(AVG(salary_max)) as average_salary
                                     FROM vacancies'''
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            return result[0]

        return None

    def get_vacancies_with_higher_salary(self) -> list:
        """ Метод для получения списка всех вакансий, у которых зарплата выше средней по всем вакансиям"""

        avg_salary = self.get_avg_salary()
        if avg_salary:
            query = '''SELECT vacansies_name, salary_max 
                    FROM vacancies 
                    WHERE salary_max > (SELECT AVG(salary_max) FROM vacancies) 
                    ORDER BY salary_max DESC, vacansies_name'''
            if self.connection:
                cursor = self.connection.cursor()
                cursor.execute(query)
                result = cursor.fetchall()
                cursor.close()
                return result

        return None

    def get_vacancies_with_keyword(self, keyword: str) -> list:
        """Метод для получения списка всех вакансий, в названии которых содержатся переданные в метод слова"""
        query = f"SELECT vacansies_name FROM vacancies WHERE vacansies_name ILIKE '%{keyword}%' ORDER BY vacansies_name"
        if self.connection:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result
        return None

from db_manager import DBManager
from hh_api import HHVacancyAPI

def main():
    """Основная функция"""

    print("Welcome to the Vacancy Management System!")
    region_name = input(
        "In which region will we search?(Москва, Санкт-Петербург, Новосибирск):")
    search_text = input("Enter the search text:")
    if region_name == "москва":
        region = 1
    elif region_name == "Санкт-Петербург":
        region = 2
    else:
        region = 1202

    # подключаемся к api и получаем данные из json-файла
    api = HHVacancyAPI(search_text, region)
    api.connect()
    data = api.get_vacancies()

    # создаём экземпляр-менеджер БД класса
    db = DBManager()

    # создаём базу данных(по-умолчанию имя "data_hh")
    db.create_database()

    # подключаемся к БД
    db.connect()
    db.drop_tables()
    # создаём таблицы
    db.create_tables()

    # добавляем данные в таблицы
    db.add_data_to_db(data)

    # получаем список всех компаний и количество вакансий у каждой компании и выводим в консоль
    print(db.get_companies_and_vacancies_count())

    # получаем список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию и выводим в консоль
    print(db.get_all_vacancies())

    # получаем среднюю зарплату по вакансиям и выводим в консоль
    print(db.get_avg_salary())

    # получаем список всех вакансий, у которых зарплата выше средней по всем вакансиям и выводим в консоль
    print(db.get_vacancies_with_higher_salary())

    # получаем список всех вакансий, в названии которых содержатся переданные в метод слова  и выводим в консоль
    print(db.get_vacancies_with_keyword("python"))

    # разрываем соединение с БД
    db.disconnect()


if __name__ == "__main__":
    main()
--создание базы данных
CREATE DATABASE data_hh;

--удаление базы данных + проверка на существование БД
DROP DATABASE data_hh;
SELECT 1 FROM pg_catalog.pg_database WHERE datname = data_hh;


--создание таблиц
CREATE TABLE employers (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL);
CREATE TABLE vacancies (
            id SERIAL PRIMARY KEY,
			vacansies_name varchar(255) NOT NULL,
            employer_id int NOT NULL,
            url text,
            salary_min int,
			salary_max int,
            FOREIGN KEY (employer_id) REFERENCES employers(id);


--удаление таблиц
DROP TABLE IF EXISTS employers CASCADE;
DROP TABLE IF EXISTS vacancies CASCADE;


--companies_and_vacancies_count
SELECT vacansies_name, COUNT(*) FROM vacancies GROUP BY vacansies_name;

--all_vacancies
SELECT employers.name, vacansies_name, salary_max, url
                                     FROM vacancies
                                     JOIN employers ON employers.id=vacancies.employer_id
                                     WHERE salary_max IS NOT NULL
                                     ORDER BY salary_max DESC, vacansies_name;
--avg_salary
SELECT ROUND(AVG(salary_max)) as average_salary
                                     FROM vacancies;

--vacancies_with_higher_salary
SELECT vacansies_name, salary_max
                    FROM vacancies
                    WHERE salary_max > (SELECT AVG(salary_max) FROM vacancies)
                    ORDER BY salary_max DESC, vacansies_name

--vacancies_with_keyword
SELECT vacansies_name FROM vacancies WHERE vacansies_name ILIKE '%{keyword}%' ORDER BY vacansies_name
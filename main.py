from sqlalchemy import ForeignKey, create_engine, Column, Integer, String, Date, NVARCHAR, Float, func, desc
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
import datetime
from sqlalchemy import NVARCHAR, text  # Добавьте text сюда
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import select
# Подключение к Б
database_url =     "mssql+pyodbc://sa:!Qwerty12!@15-2-441-9\\REXS88/?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes&PersistSecurityInfo=yes&Pooling=no&MultipleActiveResultSets=no"
engine = create_engine(database_url, echo=True)


class Base(DeclarativeBase):
    pass


class Stud(Base):
    __tablename__ = "stud"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    last_name: Mapped[str] = mapped_column(NVARCHAR(25), nullable=False)
    f_name: Mapped[str] = mapped_column(NVARCHAR(25), nullable=False)
    s_name: Mapped[str | None] = mapped_column(NVARCHAR(25))
    form: Mapped[str] = mapped_column(NVARCHAR(10), server_default=text("'очная'"))
    faculty: Mapped[str] = mapped_column(NVARCHAR(10), server_default=text("'ФПМ'"))
    year: Mapped[int] = mapped_column(server_default=text("1"))
    all_h: Mapped[int | None] = mapped_column()
    inclass_h: Mapped[int | None] = mapped_column()
    br_date: Mapped[datetime.date | None] = mapped_column()
    in_date: Mapped[datetime.date | None] = mapped_column()
    exm: Mapped[float | None] = mapped_column()


class Teach(Base):
    __tablename__ = "teach"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    last_name: Mapped[str] = mapped_column(NVARCHAR(25), nullable=False)
    f_name: Mapped[str] = mapped_column(NVARCHAR(25), nullable=False)
    s_name: Mapped[str | None] = mapped_column(NVARCHAR(25))
    subj: Mapped[str | None] = mapped_column(NVARCHAR(150))
    form: Mapped[str] = mapped_column(NVARCHAR(10), nullable=False, server_default=text("'очная'"))
    faculty: Mapped[str] = mapped_column(NVARCHAR(10), nullable=False, server_default=text("'ФПМ'"))
    year: Mapped[int] = mapped_column(nullable=False, server_default=text("1"))
    hours: Mapped[int | None] = mapped_column()
    br_date: Mapped[datetime.date | None] = mapped_column()
    start_work_date: Mapped[datetime.date | None] = mapped_column()


#Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


def insert_students():
    student_list = [
        ['Стрынгель', 'К', None, 'заочная', 'ФПК', 1, 300, 100, '1983-12-12', '2016-09-01', 8],
        ['Козлова', 'Д', 'Е', 'заочная', 'ФПК', 2, 300, 100, '1983-10-12', '2015-09-01', 8.4],
        ['Федоров', 'Н', 'Н', 'заочная', 'ФПК', 3, 300, 100, '1981-12-07', '2014-09-01', 7],
        ['Рингель', 'П', 'О', 'заочная', 'ФПК', 3, 300, 100, '1973-02-15', '2016-09-01', 8],
        ['Бежик', 'Н', 'Н', 'вечерняя', 'ФПК', 1, 500, 400, '1993-12-11', '2016-09-01', 4.5],
        ['Осипчик', 'Н', 'Н', 'вечерняя', 'ФПК', 1, 500, 400, '1983-12-16', '2015-09-01', 7.7],
        ['Белый', 'С', 'С', 'вечерняя', 'ФПК', 2, 450, 370, '1987-06-27', '2015-09-01', 6.7],
        ['Ботяновский', 'А', 'С', 'вечерняя', 'ФПК', 2, 450, 370, '1987-07-23', '2015-09-01', 7.6],
        ['Слободницкий', 'С', 'А', 'вечерняя', 'ФПК', 2, 450, 370, '1987-08-03', '2015-09-01', 6.7],
        ['Рогатка', 'П', 'Р', 'очная', 'ФПМ', 1, 500, 450, '1986-10-27', '2016-09-01', 7.4],
        ['Федоренко', 'П', 'Р', 'очная', 'ФПМ', 1, 500, 450, '1995-04-26', '2016-09-01', 5.6],
        ['Зингель', 'П', 'В', 'очная', 'ФПМ', 2, 500, 450, '1990-04-25', '2015-09-01', 3.4],
        ['Михеенок', 'Л', 'Н', 'очная', 'ФПМ', 2, 500, 450, '1989-03-13', '2015-09-01', 5.3],
        ['Савицкая', 'Л', 'Н', 'очная', 'ФПМ', 3, 450, 400, '1995-07-05', '2014-09-01', 7.7],
        ['Ковальчук', 'О', 'Е', 'заочная', 'ФПМ', 1, 350, 100, '1964-05-23', '2016-09-01', 7.6],
        ['Заболотная', 'Л', 'И', 'заочная', 'ФПМ', 1, 350, 100, '1986-09-14', '2016-09-01', 4.7],
        ['Ковриго', 'И', None, 'заочная', 'ФПМ', 2, 360, 120, '1992-03-01', '2015-09-01', 7.7],
        ['Шарапо', 'М', None, 'заочная', 'ФПМ', 2, 360, 120, '1997-03-25', '2015-09-01', 8.7],
        ['Сафроненко', 'Н', 'Л', 'заочная', 'ФПМ', 3, 370, 130, '1992-05-25', '2014-09-01', 7.7],
        ['Зайцева', 'Т', 'Я', 'заочная', 'ФПМ', 3, 370, 130, '1994-07-25', '2014-09-01', 5.6]
    ]

    for last, first, dad, forms, facultys, years, all_h, inclass_hh, birth, in_dates, exms in student_list:
        new_student = Stud(last_name=last, f_name=first,s_name=dad,form=forms,faculty=facultys,year=years,all_h=all_h,inclass_h=inclass_hh, br_date = datetime.date.fromisoformat(birth),in_date=datetime.date.fromisoformat(in_dates), exm=exms)
        session.add(new_student)

    session.commit()

import datetime
from sqlalchemy import text

def insert_teachers():
    teachers_data = [
        ('Скворцов', 'К', None, 'Дифференциальные исчисления', 'очная', 'ФПМ', 1, 150, '1983-12-12', '2016-09-01'),
        ('Скворцов', 'К', None, 'Геометрия и алгебра', 'очная', 'ФПМ', 1, 200, '1983-12-12', '2016-09-01'),
        ('Сидоренко', 'Л', 'К', 'Теория вероятности', 'очная', 'ФПМ', 1, 100, '1983-12-12', '2016-09-01'),
        ('Скворцов', 'К', None, 'Дифференциальные исчисления', 'заочная', 'ФПМ', 1, 34, '1983-12-12', '2016-09-01'),
        ('Сидоренко', 'Л', 'К', 'Геометрия и алгебра', 'заочная', 'ФПМ', 1, 50, '1983-12-12', '2016-09-01'),
        ('Сидоренко', 'Л', 'К', 'Теория вероятности', 'заочная', 'ФПМ', 1, 16, '1983-12-12', '2016-09-01'),
        ('Круглов', 'К', 'Д', 'Теория множеств', 'очная', 'ФПМ', 2, 150, '1986-08-25', '2014-09-01'),
        ('Круглов', 'К', 'Д', 'Методы численного анализа', 'очная', 'ФПМ', 2, 150, '1986-08-25', '2014-09-01'),
        ('Загорова', 'К', 'Д', 'Прикладная статистика', 'очная', 'ФПМ', 2, 150, '1979-10-05', '2010-09-01'),
        ('Круглов', 'К', 'Д', 'Теория множеств', 'заочная', 'ФПМ', 2, 40, '1986-08-25', '2014-09-01'),
        ('Круглов', 'К', 'Д', 'Методы численного анализа', 'заочная', 'ФПМ', 2, 40, '1986-08-25', '2014-09-01'),
        ('Загорова', 'К', 'Д', 'Прикладная статистика', 'заочная', 'ФПМ', 2, 40, '1979-10-05', '2010-09-01'),
        ('Зуров', 'П', None, 'Философия', 'очная', 'ФПМ', 3, 50, '1978-07-12', '2016-09-01'),
        ('Зуров', 'П', None, 'Социология', 'очная', 'ФПМ', 3, 50, '1978-07-12', '2016-09-01'),
        ('Сидоренко', 'Л', 'К', 'Методы машинного обучения', 'очная', 'ФПМ', 3, 150, '1983-12-12', '2016-09-01'),
        ('Журков', 'К', None, 'Методы выпуклой оптимизации', 'очная', 'ФПМ', 3, 150, '1986-11-16', '2015-09-01'),
        ('Курт', 'П', 'Р', 'Философия', 'заочная', 'ФПМ', 3, 20, '1978-07-12', '2016-09-01'),
        ('Курт', 'П', 'Р', 'Социология', 'заочная', 'ФПМ', 3, 20, '1978-07-12', '2016-09-01'),
        ('Сидоренко', 'Л', 'К', 'Методы машинного обучения', 'заочная', 'ФПМ', 3, 50, '1983-12-12', '2016-09-01'),
        ('Журков', 'К', None, 'Методы выпуклой оптимизации', 'заочная', 'ФПМ', 3, 40, '1986-11-16', '2015-09-01'),
        ('Скворцов', 'К', None, 'Основы алгоритмизации', 'заочная', 'ФПК', 1, 30, '1978-02-12', '2016-09-01'),
        ('Скворцов', 'К', None, 'Основы операционных систем', 'заочная', 'ФПК', 1, 20, '1978-02-12', '2016-09-01'),
        ('Сидоренко', 'Л', 'К', 'Объектно-ориенторованное программирование', 'заочная', 'ФПК', 1, 50, '1983-12-12', '2016-09-01'),
        ('Скворцов', 'К', None, 'Основы алгоритмизации', 'вечерняя', 'ФПК', 1, 100, '1978-02-12', '2016-09-01'),
        ('Скворцов', 'К', None, 'Основы операционных систем', 'вечерняя', 'ФПК', 1, 100, '1978-02-12', '2016-09-01'),
        ('Сидоренко', 'Л', 'К', 'Объектно-ориенторованное программирование', 'вечерняя', 'ФПК', 1, 200, '1983-12-12', '2016-09-01'),
        ('Кипеня', 'Д', 'А', 'Компонентное программирование', 'заочная', 'ФПК', 2, 30, '1984-01-09', '2013-09-01'),
        ('Зорков', 'К', 'А', 'Средства визуального программирования', 'заочная', 'ФПК', 2, 40, '1989-12-12', '2016-09-01'),
        ('Иридова', 'Т', 'К', 'Объектно-ориенторованное программирование', 'заочная', 'ФПК', 1, 50, '1983-04-12', '2016-09-01'),
        ('Кипеня', 'Д', 'А', 'Компонентное программирование', 'вечерняя', 'ФПК', 2, 130, '1984-01-09', '2013-09-01'),
        ('Зорков', 'К', 'А', 'Средства визуального программирования', 'вечерняя', 'ФПК', 2, 140, '1989-12-12', '2016-09-01'),
        ('Иридова', 'Т', 'К', 'Объектно-ориенторованное программирование', 'вечерняя', 'ФПК', 2, 110, '1983-04-12', '2016-09-01'),
        ('Курт', 'П', 'Р', 'Философия', 'заочная', 'ФПК', 3, 20, '1978-07-12', '2016-09-01'),
        ('Курт', 'П', 'Р', 'Социология', 'заочная', 'ФПК', 3, 20, '1978-07-12', '2016-09-01'),
        ('Иридова', 'Т', 'К', 'Современные языки программирования', 'заочная', 'ФПК', 3, 30, '1983-04-12', '2016-09-01'),
        ('Иридова', 'Т', 'К', 'Тестирование программного обеспечения', 'заочная', 'ФПК', 3, 30, '1983-04-12', '2016-09-01'),
        ('Курт', 'П', 'Р', 'Философия', 'вечерняя', 'ФПК', 3, 40, '1978-07-12', '2016-09-01'),
        ('Курт', 'П', 'Р', 'Социология', 'вечерняя', 'ФПК', 3, 40, '1978-07-12', '2016-09-01'),
        ('Иридова', 'Т', 'К', 'Современные языки программирования', 'вечерняя', 'ФПК', 3, 150, '1983-04-12', '2016-09-01'),
        ('Иридова', 'Т', 'К', 'Тестирование программного обеспечения', 'вечерняя', 'ФПК', 3, 160, '1983-04-12', '2016-09-01')
    ]

    for last, first, dad, subjc, forms, fac, yr, hrs, birth, start in teachers_data:
        teacher = Teach(last_name=last, f_name=first, s_name=dad, subj=subjc, form=forms, faculty=fac, year=yr, hours=hrs, br_date=datetime.date.fromisoformat(birth), start_work_date=datetime.date.fromisoformat(start))
        session.add(teacher)
    
    session.commit()


# 1 пункт
def select_students_union():
    stmt1 = select(Stud.last_name).where(Stud.last_name.ilike('%б%'))
    stmt2 = select(Stud.last_name).where(Stud.last_name.ilike('%о%'))

    query = stmt1.union(stmt2)

    results = session.execute(query).scalars().all()

    for name in results:
        print(name)


# 2 пункт
def print_foreign_k_students():
    stmt = select(Stud).where(Stud.last_name.ilike('К%'),Stud.s_name.is_(None))

    results = session.execute(stmt).scalars().all()

    if not results:
        print("Студенты не найдены.")
        return
    
    header = f"{'ID':<3} | {'Фамилия':<12} | {'Имя':<5} | {'Форма':<8} | {'Факультет':<10} | {'Курс':<4} | {'Экз'}"
    print(header)
    print("-" * len(header))
    for s in results:
        print(f"{s.id:<3} | {s.last_name:<12} | {s.f_name:<5} | {s.form:<8} | {s.faculty:<10} | {s.year:<4} | {s.exm}")


# 3 пункт
def select_long_lastnames():
    query = select(Stud).where(func.len(Stud.last_name) >= 8)
    
    results = session.execute(query).scalars().all()
    
    if not results:
        print("Студенты с длинными фамилиями не найдены.")
        return

    header = f"{'ID':<3} | {'Фамилия':<12} | {'Имя':<5} | {'Форма':<8} | {'Факультет':<10} | {'Курс':<4} | {'Экз'}"
    print(header)
    print("-" * len(header))
    for s in results:
        print(f"{s.id:<3} | {s.last_name:<12} | {s.f_name:<5} | {s.form:<8} | {s.faculty:<10} | {s.year:<4} | {s.exm}")



# 4 пункт
def select_specific_students():
    stmt = select(Stud).where(Stud.last_name.ilike('%а%'))
    results = session.execute(stmt).scalars().all()

    filtered = []
    for s in results:
        if len(s.last_name) != 7:
            filtered.append(s)

    if not filtered:
        print("Студенты с длинными фамилиями не найдены.")
        return

    header = f"{'ID':<3} | {'Фамилия':<12} | {'Имя':<5} | {'Отчество':<10} | {'Форма':<8} | {'Факультет':<10} | {'Курс':<4} | {'Экз'}"
    print(header)
    print("-" * len(header))
    for s in filtered:
        print(f"{s.id:<3} | {s.last_name:<12} | {s.f_name:<5}| {s.s_name} | {s.form:<8} | {s.faculty:<10} | {s.year:<4} | {s.exm}")


# 5 пункт
def select_fpm_students():

    stmt = (select(Stud).where(Stud.faculty == 'ФПМ', Stud.form == 'очная', Stud.year.in_([1, 2])).order_by(Stud.s_name))

    results = session.execute(stmt).scalars().all()

    if not results:
        print("Студенты с длинными фамилиями не найдены.")
        return

    header = f"{'ID':<3} | {'Фамилия':<12} | {'Имя':<5} | {'Отчество':<10} | {'Форма':<8} | {'Факультет':<10} | {'Курс':<4} | {'Экз'}"
    print(header)
    print("-" * len(header))
    for s in results:
        print(f"{s.id:<3} | {s.last_name:<12} | {s.f_name:<5} | {s.s_name:<10} | {s.form:<8} | {s.faculty:<10} | {s.year:<4} | {s.exm}")


# 6 пункт
def select_fpk_top_students():

    stmt = (select(Stud).where(Stud.faculty == 'ФПК', Stud.form == 'заочная', Stud.exm > 6).order_by(desc(Stud.exm)))
    results = session.execute(stmt).scalars().all()

    if not results:
        print("Студенты с длинными фамилиями не найдены.")
        return

    header = f"{'ID':<3} | {'Фамилия':<12} | {'Имя':<5} | {'Отчество':<10} | {'Форма':<8} | {'Факультет':<10} | {'Курс':<4} | {'Экз'}"
    print(header)
    print("-" * len(header))
    for s in results:
        print(f"{s.id:<3} | {s.last_name:<12} | {s.f_name:<5} | {s.s_name or '':<10} | {s.form:<8} | {s.faculty:<10} | {s.year:<4} | {s.exm}")


# 7 пункт
def select_fpk_teachers():

    stmt = (select(Teach).where(Teach.faculty == 'ФПК').order_by(Teach.form, Teach.last_name))

    results = session.execute(stmt).scalars().all()

    if not results:
        print("Преподаватели на ФПК не найдены.")
        return

    print(f"{'ID':<3} | {'Форма':<10} | {'Фамилия':<15} | {'Имя':<5} | {'Предмет'}")
    print("-" * 70)
    for t in results:
        print(f"{t.id:<3} | {t.form:<10} | {t.last_name:<15} | {t.f_name:<5} | {t.subj or '':<20}")


# 8 пункт
def select_fpm_teachers_long_hours():

    stmt = (select(Teach).where(Teach.faculty == 'ФПМ',Teach.year == 1,Teach.hours > 100))

    results = session.execute(stmt).scalars().all()

    if not results:
        print("Преподаватели, подходящие под условия, не найдены.")
        return

    print(f"{'Фамилия':<12} | {'Предмет':<30} | {'Часы':<5}")
    print("-" * 55)
    for t in results:
        print(f"{t.last_name:<12} | {t.subj or '':<30} | {t.hours}")

# 9 пункт

def select_foreign_experienced_teachers():

    three_years_ago = datetime.date.today() - datetime.timedelta(days=3 * 365)

    stmt = (select(Teach).where(Teach.s_name.is_(None),Teach.start_work_date <= three_years_ago))

    results = session.execute(stmt).scalars().all()

    if not results:
        print("Преподаватели-иностранцы со стажем более 3 лет не найдены.")
        return

    print(f"{'Фамилия':<12} | {'Имя':<5} | {'Начал работу':<12} | {'Предмет'}")
    print("-" * 60)
    for t in results:
        print(f"{t.last_name:<12} | {t.f_name:<5} | {t.start_work_date} | {t.subj or ''}")

# 10 пункт

def select_fpk_third_year_subjects():

    stmt = (select(Teach).where(Teach.faculty == 'ФПК', Teach.year == 3))

    results = session.execute(stmt).scalars().all()

    if not results:
        print("Дисциплины для 3 курса ФПК не найдены.")
        return

    print(f"{'Дисциплина':<40} | {'Форма':<10} | {'Преподаватель'}")
    print("-" * 70)
    for t in results:
        full_name = f"{t.last_name} {t.f_name}."
        print(f"{t.subj or 'Не указана':<40} | {t.form:<10} | {full_name}")

# 11 пункт

def select_fpk_long_subjects():

    stmt = (select(Teach).where(Teach.faculty == 'ФПК', Teach.hours > 100))

    results = session.execute(stmt).scalars().all()

    if not results:
        print("Дисциплины с нагрузкой > 100 часов на ФПК не найдены.")
        return

    print(f"{'Курс':<5} | {'Форма':<10} | {'Дисциплина':<40} | {'ФИО преподавателя'}")
    print("-" * 85)
    for t in results:
        fio = f"{t.last_name} {t.f_name}. {t.s_name or ''}".strip()
        print(f"{t.year:<5} | {t.form:<10} | {t.subj or '---':<40} | {fio}")

# 12 пункт

def select_foreign_teachers_info():

    stmt = (select(Teach).where(Teach.s_name.is_(None)))

    results = session.execute(stmt).scalars().all()

    if not results:
        print("Преподаватели-иностранцы не найдены.")
        return


    print(f"{'Факультет':<10} | {'Курс':<5} | {'Форма':<10} | {'ФИО преподавателя'}")
    print("-" * 65)
    for t in results:
        fio = f"{t.last_name} {t.f_name}."
        print(f"{t.faculty:<10} | {t.year:<5} | {t.form:<10} | {fio}")

# 13 пункт

def select_teachers_older_30():

    current_year_start = datetime.date(datetime.date.today().year, 1, 1)
    threshold_date = current_year_start.replace(year=current_year_start.year - 30)

    stmt = (select(Teach).where(Teach.br_date < threshold_date))

    results = session.execute(stmt).scalars().all()

    if not results:
        print("Преподаватели старше 30 лет не найдены.")
        return

    print(f"{'Фамилия':<15} | {'Имя':<5} | {'Дата рожд.':<12} | {'Возраст на 01.01'}")
    print("-" * 55)
    
    for t in results:
        age = current_year_start.year - t.br_date.year
        print(f"{t.last_name:<15} | {t.f_name:<5} | {t.br_date} | {age} лет")

# 14 пункт

def select_teachers_35_to_40():

    today = datetime.date.today()
    date_40_years_ago = today.replace(year=today.year - 40)
    date_35_years_ago = today.replace(year=today.year - 35)

    stmt = (select(Teach).where(Teach.br_date >= date_40_years_ago,Teach.br_date <= date_35_years_ago).order_by(Teach.last_name))

    results = session.execute(stmt).scalars().all()

    if not results:
        print("Преподаватели в возрасте от 35 до 40 лет не найдены.")
        return

    print(f"{'Фамилия':<15} | {'Имя':<5} | {'Дата рожд.':<12} | {'Возраст'}")
    print("-" * 50)
    for t in results:
        age = today.year - t.br_date.year - ((today.month, today.day) < (t.br_date.month, t.br_date.day))
        print(f"{t.last_name:<15} | {t.f_name:<5} | {t.br_date} | {age} лет")







# insert_students()
#insert_teachers()
#select_students_union()
#print_foreign_k_students()
#select_long_lastnames()
#select_specific_students()
# select_fpm_students()
#select_fpk_top_students()
#select_fpk_teachers()
#select_fpm_teachers_long_hours()
#select_foreign_experienced_teachers()
#select_fpk_third_year_subjects()
# select_fpk_long_subjects()
# select_foreign_teachers_info()
# select_teachers_older_30()

session.close()


from sqlalchemy import ForeignKey, create_engine, Column, Integer, String, Date, NVARCHAR, Float, func, desc
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
import datetime
from sqlalchemy import NVARCHAR, text  # Добавьте text сюда
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import select
# Подключение к Б
database_url =    r"mssql+pyodbc://sa:Hitgid_fortik228@DESKTOP-EAOH5UE\BOBRIK_USB/DUPLO?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes&PersistSecurityInfo=yes&Pooling=no&MultipleActiveResultSets=no"
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


# Base.metadata.create_all(engine)

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

def select_students_1():
    stmt1 = select(Stud.last_name).where(Stud.last_name.ilike('%б%'))
    stmt2 = select(Stud.last_name).where(Stud.last_name.ilike('%о%'))

    query = stmt1.union(stmt2)

    results = session.execute(query).scalars().all()
    print('ПУНКТ1')
    for name in results:
        print(name)


# 2 пункт

def select_students_2():
    stmt = select(Stud).where(Stud.last_name.ilike('К%'),Stud.s_name.is_(None))

    results = session.execute(stmt).scalars().all()
    print('ПУНКТ2')
    for s in results:
        print(f"Студент: {s.last_name} {s.f_name}, Курс: {s.year}, Оценка: {s.exm}")



# 3 пункт
def select_students_3():
    query = select(Stud).where(func.len(Stud.last_name) >= 8)
    
    results = session.execute(query).scalars().all()
    print('ПУНКТ3')
    for s in results:
        print(f"Студент: {s.last_name} {s.f_name}, Курс: {s.year}, Оценка: {s.exm}")



# 4 пункт
def select_students_4():
    stmt = select(Stud).where(Stud.last_name.ilike('%а%'))
    results = session.execute(stmt).scalars().all()

    filtered = []
    for s in results:
        if len(s.last_name) != 7:
            filtered.append(s)

    print('ПУНКТ4')
    for s in results:
        print(f"Студент: {s.last_name} {s.f_name}, Курс: {s.year}, Оценка: {s.exm}")


# 5 пункт
def select_students_5():

    stmt = (select(Stud).where(Stud.faculty == 'ФПМ', Stud.form == 'очная', Stud.year.in_([1, 2])).order_by(Stud.s_name))

    results = session.execute(stmt).scalars().all()

    print('ПУНКТ5')
    for s in results:
        print(f"Студент: {s.last_name} {s.f_name}, Курс: {s.year}, Оценка: {s.exm}")


# 6 пункт
def select_students_6():

    stmt = (select(Stud).where(Stud.faculty == 'ФПК', Stud.form == 'заочная', Stud.exm > 6).order_by(desc(Stud.exm)))
    results = session.execute(stmt).scalars().all()
    print('ПУНКТ6')
    for s in results:
        print(f"Студент: {s.last_name} {s.f_name}, Курс: {s.year}, Оценка: {s.exm}")

# 7 пункт
def select_teachers_7():

    stmt = (select(Teach).where(Teach.faculty == 'ФПК').order_by(Teach.form, Teach.last_name))

    results = session.execute(stmt).scalars().all()
    print('ПУНКТ7')
    for t in results:
        print(f"Преподаватель {t.last_name} Предмет: {t.subj or 'н/д'} ({t.hours} ч.)")


# 8 пункт
def select_teachers_8():

    stmt = (select(Teach).where(Teach.faculty == 'ФПМ',Teach.year == 1,Teach.hours > 100))

    results = session.execute(stmt).scalars().all()
    print('ПУНКТ8')
    for t in results:
        print(f"Преподаватель {t.last_name} Предмет: {t.subj or 'н/д'} ({t.hours} ч.)")


# 9 пункт

def select_teachers_9():

    three_years_ago = datetime.date.today() - datetime.timedelta(days=3 * 365)

    stmt = (select(Teach).where(Teach.s_name.is_(None),Teach.start_work_date <= three_years_ago))

    results = session.execute(stmt).scalars().all()
    print('ПУНКТ9')
    for t in results:
        print(f" {t.last_name} {t.f_name} Дата начала: {t.start_work_date} Предмет: {t.subj or 'не указан'}")

# 10 пункт

def select_info_10():

    stmt = (select(Teach).where(Teach.faculty == 'ФПК', Teach.year == 3))

    results = session.execute(stmt).scalars().all()
    print('ПУНКТ10')
    for t in results:
        print(f"Предмет: {t.subj or '---'} {t.form}  {t.last_name} {t.f_name}.")

# 11 пункт

def select_info_11():

    stmt = (select(Teach).where(Teach.faculty == 'ФПК', Teach.hours > 100))

    results = session.execute(stmt).scalars().all()

    print('ПУНКТ11')
    for t in results:
        fio = f"{t.last_name} {t.f_name[0]}.{t.s_name[0] if t.s_name else ''}."
        print(f"{t.year} курс  {t.form:<8} {t.subj[:20]:<20} {fio}")

# 12 пункт

def select_info_12():

    stmt = (select(Teach).where(Teach.s_name.is_(None)))

    results = session.execute(stmt).scalars().all()


    print('ПУНКТ12')
    for t in results:
        print(f"[{t.faculty}] {t.year} курс, {t.form}: {t.last_name} {t.f_name}.")
# 13 пункт

def select_teachers_13():

    current_year_start = datetime.date(datetime.date.today().year, 1, 1)
    threshold_date = current_year_start.replace(year=current_year_start.year - 30)

    stmt = (select(Teach).where(Teach.br_date < threshold_date))

    results = session.execute(stmt).scalars().all()
    print('ПУНКТ13')
    for t in results:
        print(f"{t.last_name} {t.f_name}  Дата: {t.br_date}")


# 14 пункт

def select_teachers_14():

    today = datetime.date.today()
    date_40_years_ago = today.replace(year=today.year - 40)
    date_35_years_ago = today.replace(year=today.year - 35)

    stmt = (select(Teach).where(Teach.br_date >= date_40_years_ago,Teach.br_date <= date_35_years_ago).order_by(Teach.last_name))

    results = session.execute(stmt).scalars().all()
    print('ПУНКТ14')
    for t in results:
        print(f"{t.last_name} {t.f_name} Дата: {t.br_date}")

# 15 пункт

def select_teachers_15():

    statement = (select(Teach).where(Teach.br_date.cast(String).like('%-10-%')).order_by(Teach.br_date))

    results = session.execute(statement).scalars().unique().all()
    print('ПУНКТ15')
    for t in results:
        print(f"ID {t.id}: {t.last_name} {t.f_name} Дата рождения: {t.br_date}")






# insert_students()
# insert_teachers()
select_students_1()
select_students_2()
select_students_3()
select_students_4()
select_students_5()
select_students_6()
select_teachers_7()
select_teachers_8()
select_teachers_9()
select_info_10()
select_info_11()
select_info_12()
select_teachers_13()
select_teachers_14()
select_teachers_15()


session.close()


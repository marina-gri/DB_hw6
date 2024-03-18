import json
import os
import sqlalchemy as sq
from sqlalchemy import func
from sqlalchemy.orm import sessionmaker
from models import create_tables, Publisher, Shop, Book, Stock, Sale

db_name = os.getenv('db_name')
login = os.getenv("db_login")
password = os.getenv("db_password")

DSN = f"postgresql://{login}:{password}@localhost:5432/{db_name}"
engine = sq.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('fixtures/tests_data.json', 'r') as fd:
    data = json.load(fd)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale,
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()


def find_purchases(publisher_name):
    if publisher_name.isdigit():
        for c in session.query(Book.title, Shop.name, Sale.price, func.to_char(Sale.date_sale, 'DD-MM-YYYY')
                               ).join(Publisher).join(Stock).join(Shop).join(Sale).filter(
                               Publisher.id == publisher_name).all():
            print(f'{c[0]} | {c[1]} | {c[2]} | {c[3]}')
    else:
        for c in session.query(Book.title, Shop.name, Sale.price, func.to_char(Sale.date_sale, 'DD-MM-YYYY')
                               ).join(Publisher).join(Stock).join(Shop).join(Sale).filter(
                               Publisher.name.ilike(f'%{publisher_name}%')).all():
            print(f'{c[0]} | {c[1]} | {c[2]} | {c[3]}')


session.close()


if __name__ == "__main__":
    name = input(f"Введите имя или id издателя: ")
    find_purchases(name)

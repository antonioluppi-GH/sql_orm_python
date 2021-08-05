import json
import requests
from requests.api import get
import csv
import sqlite3
import sqlalchemy
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = sqlalchemy.create_engine("sqlite:///articulos_mercadolibre.db")
base = declarative_base()

def create_schema():
    
    base.metadata.drop_all(engine)
    base.metadata.create_all(engine)

class Articulo(base):
    __tablename__ = 'articulo'
    id = Column(String, primary_key=True, autoincrement=False)
    site_id = Column(String)
    title = Column(String)
    price = Column(Integer)
    currency_id = Column(String)
    initial_quantity = Column(Integer)
    available_quantity = Column(Integer)
    sold_quantity = Column(Integer)

    def __repr__(self):
        return f"""Articulo\nId:{self.id}, \nsite_id:{self.site_id}, \ntitle:{self.title}, \nprice:{self.price}\
        \ncurrency_id:{self.currency_id}, \ninitial_quantity:{self.initial_quantity}\
        \navailable_quantity:{self.available_quantity}, \nsold_quantity:{self.sold_quantity}"""

def fill():
    Session = sessionmaker(bind=engine)
    session = Session()
    
    with open('meli_technical_challenge_data.csv') as csvfile:
        lista = list(csv.DictReader(csvfile))
        print(lista)
        for i in lista:
            site_id = i['site']+ i['id']
            url = 'https://api.mercadolibre.com/items?ids={}'.format(site_id)
            
            try:    
                response = requests.get(url)
                data = response.json
                
                producto = Articulo(site_id=site_id, title=data["title"], 
                        price=data["price"],
                         currency_id=data["currency_id"],
                         initial_quantity=data["initial_quantity"],
                         available_quantity=data["available_quantity"],
                          sold_quantity=data["sold_quantity"])
            except:
                pass
    session.add(producto)
    session.commit()
            
            
            
            
            
            
    
if __name__ == "__main__":

    create_schema() #Crear DB
    fill() # Completar la DB con el CSV

    # Leer filas
    #fetch('MLA845041373')
    #fetch('MLA717159516')'''
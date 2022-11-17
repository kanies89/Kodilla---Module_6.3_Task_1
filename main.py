from sqlalchemy import Table, Column, Integer, String, MetaData, Float
from sqlalchemy import create_engine
import csv

engine = create_engine('sqlite:///database.db', echo=True)

meta = MetaData()

def read_table(x):
    result_list = []
    with open(f'{x}.csv') as csv_file:
        csv_reader = csv.DictReader(csv_file, delimiter=",")
        for row in csv_reader:
            result_list.append(dict(row))
    return result_list

clean_measure_table = Table(
   'clean_measure', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('date', String),
   Column('precip', String),
   Column('tobs', Integer)
)

clean_stations_table = Table(
   'clean_stations', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('latitude', Float),
   Column('longitude', Float),
   Column('elevation', Float),
   Column('name', String),
   Column('country', String),
   Column('state', String)
)


meta.create_all(engine)
print(engine.table_names())

conn = engine.connect()
ins = clean_measure_table.insert()
clean_measure = read_table('clean_measure')
conn.execute(ins, clean_measure)
ins = clean_stations_table.insert()
clean_stations = read_table('clean_stations')
conn.execute(ins, clean_stations)

conn.close()


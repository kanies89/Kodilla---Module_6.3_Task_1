import sqlalchemy.sql
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
from sqlalchemy import create_engine, select
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


if __name__ == '__main__':
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

    query = clean_measure_table.select().where(clean_measure_table.c.station == 'USC00519397')
    result = conn.execute(query)
    for row in result:
        print(row)

    query = clean_measure_table.select().where(clean_measure_table.c.date == '2017-07-25')
    print(f'Rekord {conn.execute(query).fetchall()} został usunięty.')
    delete = clean_measure_table.delete().where(clean_measure_table.c.date == '2017-07-25')
    conn.execute(delete)

    upt = clean_measure_table.update().where(clean_measure_table.c.id == 2).values(station='NOWA')
    conn.execute(upt)
    print(conn.execute(clean_measure_table.select().where(clean_measure_table.c.station == "NOWA")).fetchall())
    print(conn.execute("SELECT * FROM clean_stations LIMIT 5").fetchall())

    conn.close()

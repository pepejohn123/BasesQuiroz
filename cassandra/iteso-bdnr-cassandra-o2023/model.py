#!/usr/bin/env python3
import logging
import os
import pandas as pd

# Set logger
log = logging.getLogger()

script_directory = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(script_directory, 'tools/all_flights_per_month.csv')
df = pd.read_csv(csv_file_path)
airports = df['to'].unique().tolist()



CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

CREATE_DEFAULT_CAR_RENTAL = """
    CREATE TABLE IF NOT EXISTS default_car_rental (
        number INT,
        airline TEXT,
        origin TEXT,
        destination TEXT,
        day INT,
        month INT,
        year INT,
        age INT,
        gender TEXT,
        reason TEXT,
        stay TEXT,
        transit TEXT,
        connection BOOLEAN,
        wait INT,
        PRIMARY KEY (month, reason, stay, transit,number)
    )
"""

CREATE_MOST_CAR_RENTAL_POTENTIAL_RATIO = """
    CREATE TABLE IF NOT EXISTS most_car_rental_potential_ratio (
        airport TEXT,
        month INT,
        flights INT,
        potential_clients INT,
        ratio DECIMAL,
        PRIMARY KEY (airport,month)
    )
"""


CREATE_MOST_CAR_RENTAL_POTENTIAL_PLUS_CLIENTS_RATIO = """
    CREATE TABLE IF NOT EXISTS most_car_rental_potential_plus_clients_ratio (
        airport TEXT,
        month INT,
        flights INT,
        actual_clients INT,
        potential_clients INT,
        ratio DECIMAL,
        PRIMARY KEY  (airport,month)
    )
"""

SELECT_FLIGHTS = """
  SELECT * 
  FROM default_car_rental 

"""

SELECT_FLIGHTS_BY_MONTH = """
  SELECT * 
  FROM default_car_rental 
  WHERE month = ? AND
  reason = ? AND
  stay = ? AND
  transit = ?;

"""

SELECT_BEST_MONTH_POTENTIAL_RATIO= """
  SELECT * 
  FROM most_car_rental_potential_ratio 
  WHERE airport = ?
"""

SELECT_BEST_MONTH_ACTUAL_CLIENTS= """
  SELECT * 
  FROM most_car_rental_potential_plus_clients_ratio
  WHERE airport = ? 
"""



def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))


def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_DEFAULT_CAR_RENTAL)
    session.execute(CREATE_MOST_CAR_RENTAL_POTENTIAL_RATIO)

    session.execute(CREATE_MOST_CAR_RENTAL_POTENTIAL_PLUS_CLIENTS_RATIO)


def get_flights_by_month(session,args):
    log.info(f"Retrieving FLIGHTS")
    stmt = session.prepare(SELECT_FLIGHTS_BY_MONTH)
    rows = session.execute(stmt, [args[0],args[1],args[2],args[3]])
    for row in rows:
        print(f"=== date: {row.day}/{row.month}/{row.year} ===")
        print(f"- airline: {row.airline}")
        print(f"- from: {row.origin} to {row.destination}")
        if(row.connection== False):
           print("- this flight was not a connection") 
        else:
           print(f"- this flight was a connection, the wait time was {row.wait} ") 
        print(f"- customer is {row.gender}, is {row.age} yo, his/her flight reason was {row.reason} and moved out from the airport using: {row.transit}\n")

def get_flights(session):
    log.info(f"Retrieving FLIGHTS")

    stmt = session.prepare(SELECT_FLIGHTS)
    rows = session.execute(stmt)
    for row in rows:
        print(f"=== date: {row.day}/{row.month}/{row.year} ===")
        print(f"- airline: {row.airline}")
        print(f"- from: {row.origin} to {row.destination}")
        if(row.connection== False):
           print("- this flight was not a connection") 
        else:
           print(f"- this flight was a connection, the wait time was {row.wait} ") 
        print(f"- customer is {row.gender}, is {row.age} yo, his/her flight reason was {row.reason} and moved out from the airport using: {row.transit}")

def get_potential_clients(session, airport):
    log.info(f"Retrieving potential clients")
    stmt = session.prepare(SELECT_BEST_MONTH_POTENTIAL_RATIO)

    
    rows = session.execute(stmt, [airport])

    
    for row in rows:
        print(f"=== month:{row.month} ===")
        print(f"- airport: {row.airport}")
        print(f"- flights: {row.flights}")
        print(f"- potential clients: {row.potential_clients}")
        print(f"- ratio: {row.ratio}")


def get_actual_clients(session,airport):
    log.info(f"Retrieving potential clients")
    stmt = session.prepare(SELECT_BEST_MONTH_ACTUAL_CLIENTS)
    rows = session.execute(stmt, [airport])

    
    for row in rows:
        print(f"=== month:{row.month} ===")
        print(f"- airport: {row.airport}")
        print(f"- flights: {row.flights}")
        print(f"- potential clients: {row.potential_clients}")
        print(f"- actual clients: {row.actual_clients}")
        print(f"- ratio: {row.ratio}")

def biggest_ratio(session, option):
    log.info(f"Retrieving best month of every airport")
    rows = []
    max_ratio_rows = []
    stmt = session.prepare(SELECT_BEST_MONTH_ACTUAL_CLIENTS)
    for item in airports:
        rows.append(session.execute(stmt,[item]))
    for i in range(0, len(rows)):
        max_ratio = float('-inf')
        chosen_row = ""
        for j in range(0, 12):
            if(rows[i][j].ratio> max_ratio):
                chosen_row = rows[i][j]
                max_ratio = rows[i][j].ratio
        max_ratio_rows.append(chosen_row)
    if(option ==1):
        info_actual(max_ratio_rows)
    else:
        info_potential(max_ratio_rows)



def info_potential(max_ratio_rows):
    for i in range(0, len(max_ratio_rows)):
        row = max_ratio_rows[i]
        print(f"=== month:{row.month} ===")
        print(f"- airport: {row.airport}")
        print(f"- flights: {row.flights}")
        print(f"- potential clients: {row.potential_clients}")
        print(f"- ratio: {row.ratio}")

def info_actual(max_ratio_rows):
    for i in range(0, len(max_ratio_rows)):
        row = max_ratio_rows[i]
        print(f"=== month:{row.month} ===")
        print(f"- airport: {row.airport}")
        print(f"- flights: {row.flights}")
        print(f"- actual clients: {row.actual_clients}")
        print(f"- potential clients: {row.potential_clients}")
        print(f"- ratio: {row.ratio}")
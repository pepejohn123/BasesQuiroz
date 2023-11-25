#!/usr/bin/env python3
import pandas as pd
import os
from monthly_info_generator import process_flight_data

from zero_value_setter import insert_missing_months

process_flight_data()

insert_missing_months("iteso-bdnr-cassandra-o2023/tools/all_flights_per_month.csv")
insert_missing_months("iteso-bdnr-cassandra-o2023/tools/clients_flights_per_month.csv")
insert_missing_months("iteso-bdnr-cassandra-o2023/tools/filtered_flights_per_month.csv")

script_directory = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(script_directory, 'flight_passengers.csv')
csv_all_flights = os.path.join(script_directory, 'all_flights_per_month.csv')
csv_client_flights = os.path.join(script_directory, 'clients_flights_per_month.csv')
csv_filtered_flights = os.path.join(script_directory, 'filtered_flights_per_month.csv')

df = pd.read_csv(csv_file_path)
all_flights_df = pd.read_csv(csv_all_flights)
client_flights_df = pd.read_csv(csv_client_flights)
filtered_flights_df = pd.read_csv(csv_filtered_flights)


CQL_FILE = 'iteso-bdnr-cassandra-o2023/tools/data.cql'


def cql_stmt_generator():
    default_stmt = "INSERT INTO default_car_rental (number, airline, origin,destination, day, month,year, age, gender, reason,stay, transit, connection,wait) VALUES ({},'{}', '{}', '{}', {}, {},{}, {}, '{}', '{}','{}', '{}', {}, {});"
    potential_ratio_stmt = "INSERT INTO most_car_rental_potential_ratio(airport,month, flights, potential_clients,ratio) VALUES ('{}',{}, {}, {}, {});"
    actual_ratio_stmt = "INSERT INTO most_car_rental_potential_plus_clients_ratio (airport,month, flights,actual_clients, potential_clients,ratio) VALUES('{}',{}, {}, {}, {}, {});"


    with open(CQL_FILE, "w") as fd:
        # Generate accounts by user
        i = 0
        for index, row in df.iterrows():
            # Access individual elements in the row
            airline = row['airline']
            from_location = row['from']
            to_location = row['to']
            day = row['day']
            month = row['month']
            year = row['year']
            age = row['age']
            gender = row['gender']
            reason = row['reason']
            stay = row['stay']
            transit = row['transit']
            connection = row['connection']
            wait = row['wait']
             # Perform operations with the row data
            fd.write(default_stmt.format(i,airline, from_location, to_location, day, month, year, age, gender, reason, stay, transit, connection, wait) + '\n')
            i = i+1
        fd.write('\n\n')

        # Genetate possitions by account
        for (index1, row1), (index2, row2), (index3, row3) in zip(all_flights_df.iterrows(), client_flights_df.iterrows(), filtered_flights_df.iterrows()):
            airport = row1['to']
            month = row1['month']
            flights = row1['num_flights']
            potential_clients = row3['num_flights']
            actual_clients = row2['num_flights']
            
            potential_ratio = round( potential_clients/flights, 2) 
            actual_clients_ratio = round((actual_clients+potential_clients)/flights,2)
            fd.write(potential_ratio_stmt.format(airport,month, flights, potential_clients, potential_ratio) + '\n')
            fd.write(actual_ratio_stmt.format(airport,month, flights, actual_clients,potential_clients, actual_clients_ratio) + '\n')
            fd.write('\n\n')
"""         for i in range(1, 13):
            potential_clients = len(filtered_monthly_dfs.get(i, pd.DataFrame()))
            flights = len(UNFILTERED_monthly_dfs.get(i, pd.DataFrame()))
            ratio= round(potential_clients/flights, 2)
            print("month:",i,"\tflights:",flights,"\n")
            count = flights + count
            fd.write(potential_ratio_stmt.format(i, flights, potential_clients, ratio) + '\n')
            fd.write(potential_clients_stmt.format(i, flights, potential_clients, ratio) + '\n')


            actual_clients = len(actual_monthly_clients_dfs.get(i, pd.DataFrame()))
            ratio= round((potential_clients+actual_clients)/flights, 2)
            fd.write(actual_ratio_stmt.format(i, flights, actual_clients,potential_clients, ratio) + '\n')
            fd.write(actual_potclients_stmt.format(i, flights, actual_clients,potential_clients, ratio) + '\n')
            fd.write(actual_plusclients_stmt.format(i, flights, actual_clients,potential_clients, ratio) + '\n')
 
        fd.write('\n\n')
        """

    
        

def main():
    cql_stmt_generator()


if __name__ == "__main__":
    main()
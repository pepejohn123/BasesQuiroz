import pandas as pd
import os

def process_flight_data():
    # Get the absolute path to the CSV file
    script_directory = os.path.dirname(os.path.abspath(__file__))
    csv_file_path = os.path.join(script_directory, 'flight_passengers.csv')
    

    df = pd.read_csv(csv_file_path)

    # All flights per month
    flights_per_month = df.groupby(['to', 'month']).size().reset_index(name='num_flights')
    flights_per_month.to_csv('iteso-bdnr-cassandra-o2023/tools/all_flights_per_month.csv', index=False)

    # Filtered flights
    filtered_df = df[(df['connection'] == False) & 
                     (df['reason'] != "Back Home") & 
                     (df['transit'].isin(["Own car", "Car rental"]) == False) & 
                     (df['stay'] != "Home")]

    filtered_flights_per_month = filtered_df.groupby(['to', 'month']).size().reset_index(name='num_flights')
    filtered_flights_per_month.to_csv('iteso-bdnr-cassandra-o2023/tools/filtered_flights_per_month.csv', index=False)

    # Actual clients' flights per month
    actual_clients_df = df[df['transit'] == "Car rental"].copy()
    clients_flights_per_month = actual_clients_df.groupby(['to', 'month']).size().reset_index(name='num_flights')
    clients_flights_per_month.to_csv('iteso-bdnr-cassandra-o2023/tools/clients_flights_per_month.csv', index=False)

import pandas as pd
import os

# Get the absolute path to the CSV file
script_directory = os.path.dirname(os.path.abspath(__file__))
csv_file_path = os.path.join(script_directory, 'flight_passengers.csv')

df = pd.read_csv(csv_file_path)


UNFILTERED_df = df.copy()
filtered_df = df[df['connection'] == False]
filtered_df = filtered_df[filtered_df['reason'] != "Back Home"]
filtered_df = filtered_df[filtered_df['transit'] != "Own car"]
actual_clients_df = filtered_df[filtered_df['transit'] == "Car rental"].copy()
filtered_df = filtered_df[filtered_df['transit'] != "Car rental"]
filtered_df = filtered_df[filtered_df['stay'] != "Home"]



flight_counts_fil = filtered_df['to'].value_counts()
flight_counts_fil = flight_counts_fil.sort_index()
print(flight_counts_fil)

actual_clients_count = actual_clients_df['to'].value_counts()
actual_clients_count = actual_clients_count.sort_index()

print(flight_counts_fil)


filtered_monthly_dfs = {}
UNFILTERED_monthly_dfs = {}
actual_monthly_clients_dfs = {}
for month in range(1, 13):
    df_month = filtered_df[filtered_df['month'] == month]
    filtered_monthly_dfs[month] = df_month

    df_month = UNFILTERED_df[UNFILTERED_df['month'] == month]
    UNFILTERED_monthly_dfs[month] = df_month

    df_month = actual_clients_df[actual_clients_df['month'] == month]
    actual_monthly_clients_dfs[month] = df_month



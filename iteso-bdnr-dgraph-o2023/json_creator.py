import csv
from collections import defaultdict
from itertools import tee
import json

# Path to the new CSV file
output_csv_path = "dgraph_flights.csv"


def count_flights_per_airline():
    mutations = []
    # Open the new CSV file for reading
    with open(output_csv_path, newline="", encoding="utf-8") as csvfile:
        # Read the CSV file
        csv_reader, peeker = tee(csv.DictReader(csvfile))

        # Create a defaultdict to store the count of flights for each airline
        flights_per_airline = defaultdict(int)

        # Iterate through each row in the CSV file
        airline_data={}
        next_row = next(peeker)
        for row in csv_reader:
            flights_per_airline[row["airline"]] += 1
            if(flights_per_airline[row["airline"]]==1):
                airline_data={
                    "name": row["airline"],
                    "dgraph.type": "Airline",
                    "schedules": []
                }
            flight_data={
                "ID": row["ID"],
                "dgraph.type": "Flight",
                "wait": row["wait"]
            }
            airline_data["schedules"].append(flight_data)

            # Peek at the next row without consuming it
            try:
                next_row = next(peeker)
                if next_row["airline"] != row["airline"]:
                    mutations.append(airline_data)
            except StopIteration:
                mutations.append(airline_data)
                # Handle the end of the file
                break

    # Print the count of flights for each airline
    json_string = json.dumps(mutations, indent=2)

    file_path = "flights.json"
    with open(file_path, 'w') as file:
    # Write the string to the file
        file.write(json_string)
    print("json created at ",file_path,"\n")
    return file_path

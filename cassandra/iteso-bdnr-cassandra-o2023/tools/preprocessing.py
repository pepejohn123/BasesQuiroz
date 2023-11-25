
from collections import defaultdict

import csv
  # Replace with your actual column names

# Path to the existing CSV file
input_file  = "flight_passengers.csv"

# Path to the new CSV file
output_file  = "ordered_flight.csv"

import csv

# Read the CSV file into a list of dictionaries
with open(input_file, 'r') as file:
    reader = csv.DictReader(file)
    data = list(reader)

# Sort the data based on the "airline" column
data.sort(key=lambda x: x['to'])

fieldnames = ["airline", "from", "to","day","month","year","age","gender","reason","stay","transit","connection","wait"]
# Write the sorted data back to a new CSV file
with open(output_file, 'w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    
    # Write the header
    writer.writeheader()
    
    # Write the sorted rows
    writer.writerows(data)

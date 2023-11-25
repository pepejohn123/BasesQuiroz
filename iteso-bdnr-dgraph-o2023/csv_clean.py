
from collections import defaultdict

print("alo")
import csv

# Path to the existing CSV file
input_csv_path = "flight_passengers.csv"

# Path to the new CSV file
output_csv_path = "dgraph_flights.csv"

def modify_and_write_csv():
    # Open the existing CSV file for reading
    with open(input_csv_path, newline="", encoding="utf-8") as csvfile:
        # Read the CSV file
        csv_reader = csv.DictReader(csvfile)

        # Create a list to store modified rows
        modified_rows = []

        # Iterate through each row in the existing CSV file
        for row in csv_reader:
            # Keep only the desired columns
            modified_row = {
                "airline": row["airline"],
                "connection": row["connection"],
                "wait": row["wait"]
            }

            # Add the modified row to the list if "connection" is True
            if row["connection"].lower() == "true":
                modified_rows.append(modified_row)

        # Sort the rows based on the "airline" column
        modified_rows.sort(key=lambda x: x["airline"])

        # Add the sequential numerical IDs
        for index, modified_row in enumerate(modified_rows):
            modified_row["ID"] = str(index + 1)

    # Define the header for the new CSV file
    fieldnames = ["ID", "airline", "connection", "wait"]

    # Open the new CSV file for writing
    with open(output_csv_path, mode="w", newline="", encoding="utf-8") as csvfile:
        # Write the modified data to the new CSV file
        csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write the header
        csv_writer.writeheader()

        # Write the sorted and filtered rows with IDs
        csv_writer.writerows(modified_rows)
    print("Filtered csv created:",output_csv_path,"\n")

""" if __name__ == "__main__":
    modify_and_write_csv() """

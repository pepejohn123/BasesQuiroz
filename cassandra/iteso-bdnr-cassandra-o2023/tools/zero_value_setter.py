import csv

def insert_missing_months(file_path):
    # Read the CSV file into a list of dictionaries
    with open(file_path, 'r') as file:
        reader = csv.DictReader(file)
        data = list(reader)

    # Identify unique destinations and months
    destinations = set(row['to'] for row in data)
    months = set(int(row['month']) for row in data)

    # Create a new list to store the modified data
    new_data = []

    # Iterate over each destination and month, insert missing rows
    for destination in destinations:
        for month in months:
            found = False
            for row in data:
                if row['to'] == destination and int(row['month']) == month:
                    new_data.append(row)
                    found = True
                    break
            if not found:
                # Insert a row with the corresponding month and a value of 0
                new_row = {'to': destination, 'month': str(month), 'num_flights': '0'}
                new_data.append(new_row)

    # Sort the new data alphabetically by the 'to' column
    new_data.sort(key=lambda x: x['to'])

    # Write the modified and sorted data back to the CSV file
    with open(file_path, 'w', newline='') as file:
        fieldnames = ['to', 'month', 'num_flights']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(new_data)

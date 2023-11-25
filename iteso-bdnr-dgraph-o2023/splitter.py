import json

def split_json(input_file, output_file1, output_file2):
    with open(input_file, 'r') as f:
        data = json.load(f)

    # Calculate the split point as half the length of the list
    split_point = len(data) // 2

    # Split the list into two parts
    part1 = data[:split_point]
    part2 = data[split_point:]

    # Write the parts to separate output files
    with open(output_file1, 'w') as f1:
        json.dump(part1, f1, indent=2)

    with open(output_file2, 'w') as f2:
        json.dump(part2, f2, indent=2)

# Example usage
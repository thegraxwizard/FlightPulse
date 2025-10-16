import pandas as pd
import os
import time
import json

# The folder we will write our stream files to
output_folder = 'streaming_input'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Read the source data
df = pd.read_csv('flights.csv')

print(f"Starting to simulate stream. Dropping files into '{output_folder}'...")

# Loop through each row and write it as a new JSON file
for index, row in df.iterrows():
    message = row.to_dict()

    # Create a unique filename for each event
    file_path = os.path.join(output_folder, f"flight_{int(time.time() * 1000)}.json")

    # Write the JSON data to the file
    with open(file_path, 'w') as f:
        json.dump(message, f)

    print(f"Generated file: {file_path}")

    # Wait for 1 second before creating the next file
    time.sleep(1)

print("Stream simulation complete.")
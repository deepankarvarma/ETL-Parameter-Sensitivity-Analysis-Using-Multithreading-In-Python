import pandas as pd
import time
import numpy as np
import os
import threading
from queue import Queue
import matplotlib.pyplot as plt

# Define the function to extract data from file
def extract(file):
    data = pd.read_csv(file)
    return data

# Define the function to transform data
def transform(data):
    # Compute the mean and standard deviation of each column in the data
    mean = data.mean(numeric_only=True)
    std = data.std(numeric_only=True)

    # Subtract the mean from each column and divide by the standard deviation
    transformed_data = (data - mean) / std

    return transformed_data

# Define the function to load data into database
def load(data):
    # Load the data into the database
    # In this example, we're just printing the number of records and returning the data
    print(f"Number of records: {len(data)}")
    return data

# Define the function to split files into smaller chunks
def split_file(file, chunk_size):
    # Create a folder for the split files
    folder_name = f"split/{os.path.splitext(file)[0]}"
    os.makedirs(folder_name, exist_ok=True)

    # Read the original file and split it into chunks
    data = pd.read_csv(file)
    chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

    # Write each chunk to a separate file
    for i, chunk in enumerate(chunks):
        chunk.to_csv(f"{folder_name}/chunk_{i}.csv", index=False)

    print(f"File {file} split into {len(chunks)} chunks")

# Define the function to perform the ETL process on a single chunk
def etl_process(chunk, transformed_chunks):
    transformed_chunk = transform(chunk)
    transformed_chunks.put(transformed_chunk)

# Define the function to apply the ETL process through multithreading pipeline
def apply_etl(file):
    # Read the list of chunks from the folder
    folder_name = f"split/{os.path.splitext(file)[0]}"
    chunk_files = sorted([f"{folder_name}/{f}" for f in os.listdir(folder_name) if f.endswith(".csv")])

    # Initialize a queue to store transformed chunks
    transformed_chunks = Queue()

    # Create a thread for each chunk
    threads = []
    for chunk_file in chunk_files:
        chunk = pd.read_csv(chunk_file)
        thread = threading.Thread(target=etl_process, args=(chunk, transformed_chunks))
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Merge the transformed chunks into a single dataframe
    transformed_data = pd.concat(list(transformed_chunks.queue), ignore_index=True)

    # Load the transformed data into the database
    transformed_data = load(transformed_data)

    return transformed_data

# Define a list of files with different sizes
files = ["file1.csv", "file2.csv", "file3.csv", "file4.csv", "file5.csv", "file6.csv"]

# Initialize lists to store the execution time and number of records for each file
execution_time = []
num_records = []

# Split each file into smaller chunks and apply the ETL process through multithreading pipeline
import pandas as pd
from IPython.display import display
results = pd.DataFrame(columns=['File', 'Split Size', 'Execution Time'])

for file in files:
    execution_time_file = []
    for chunk_size in range(50, 550, 50):
        start_time = time.time() # Start the timer
        split_file(file, chunk_size) # Split the file
        transformed_data = apply_etl(file) # Apply the ETL process through multithreading pipeline
        end_time = time.time() # Stop the timer

        # Compute the execution time and number of records for the transformed data
        execution_time_file.append(end_time - start_time)

        # Store the results in the DataFrame
        results = results.append({'File': file, 'Split Size': chunk_size, 'Execution Time': end_time - start_time}, ignore_index=True)
        results.to_csv('results.csv', index=False)

    # Plot the execution time for the current file
    plt.plot(range(50, 550, 50), execution_time_file)
    plt.xlabel('Split Size')
    plt.ylabel('Execution Time (seconds)')
    plt.title(f'Execution Time vs Split Size for {file}')
    plt.show()


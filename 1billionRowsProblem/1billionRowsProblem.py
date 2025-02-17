import random
from multiprocessing import Pool
import csv
import pyarrow as pa
import pyarrow.parquet as pq
import time

class GenerateInputFile:
    def __init__(self, 
                 num_of_rows=1000000000, 
                 batch_size=1000000,
                 cities=("New York", 
                         "London", 
                         "Tokyo", 
                         "Paris", 
                         "Berlin",
                         "Moscow", 
                         "Sydney", 
                         "Los Angeles", 
                         "Chicago", 
                         "Toronto",
                         "Seoul", 
                         "Mumbai", 
                         "Mexico City", 
                         "Sao Paulo", 
                         "Cairo",
                         "Istanbul", 
                         "Beijing", 
                         "Shanghai", 
                         "Jakarta", 
                         "Delhi"),
                 tmp=(-14.5, 120)):
        self.num_of_rows = num_of_rows
        self.cities = cities
        self.batch_size = batch_size
        self.min_tmp, self.max_tmp = tmp

    def city_to_tmp_mapper(self):
        """Return a tuple containing a random city and a random temperature."""
        city = random.choice(self.cities)
        temperature = round(random.uniform(self.min_tmp, self.max_tmp), 2)
        return (city, temperature)
    
    def generate_weather_data(self, batch_rows):
        """
        Generate weather data for a batch of rows.
        Returns a list of tuples, each tuple being (city, temperature).
        """
        output_list = []
        for _ in range(batch_rows):
            output_list.append(self.city_to_tmp_mapper())
        return output_list

if __name__ == '__main__':
    start_time = time.time()

    # Configuration: number of rows and batch size
    batch_size = 1000000
    num_of_rows = 250500000  # Adjust as needed
    input_file_generator = GenerateInputFile(num_of_rows, batch_size)

    # Determine how many batches we need
    num_of_batches = num_of_rows // batch_size
    remainder = num_of_rows % batch_size
    batches = [batch_size] * num_of_batches
    if remainder:
        batches.append(remainder)

    # Generate data in parallel for all batches
    with Pool() as pool:
        results = pool.map(input_file_generator.generate_weather_data, batches)

    # # -------------------------------
    # # Write to TEXT file
    # # -------------------------------
    filename = '1billionRowInput.txt'
    with open(filename, 'w', newline='') as file:
        batch_counter = 0
        for batch in results:
            batch_counter += 1
            print(f'Writing batch number (TXT) - {batch_counter}')
            batch = [f'{item[0]}:{item[1]}\n' for item in batch]
            batch_str = ''.join(batch)
            file.write(batch_str)     
    ## Took 1080.6874585151672 secs for 250500000 rows - file size 3.49 GB

    # # -------------------------------
    # # Write to CSV file
    # # -------------------------------
    csv_filename = '1billionRowInput.csv'
    with open(csv_filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # Write header row
        writer.writerow(["City", "Temperature"])
        batch_counter = 0
        for batch in results:
            batch_counter += 1
            print(f'Writing batch number (CSV) - {batch_counter}')
            # Write all rows in this batch
            writer.writerows(batch)

    # # -------------------------------
    # # Write to Parquet file using PyArrow
    # # -------------------------------
    # parquet_filename = '1billionRowInput.parquet'
    # # Define the schema for the Parquet file
    # schema = pa.schema([
    #     ('City', pa.string()),
    #     ('Temperature', pa.float64())
    # ])
    # writer_obj = None
    # try:
    #     writer_obj = pq.ParquetWriter(parquet_filename, schema)
    #     batch_counter = 0
    #     for batch in results:
    #         batch_counter += 1
    #         print(f'Writing batch number (Parquet) - {batch_counter}')
    #         # Convert the batch (a list of tuples) into a dictionary with separate lists.
    #         cities = [row[0] for row in batch]
    #         temperatures = [row[1] for row in batch]
    #         data_dict = {'City': cities, 'Temperature': temperatures}
    #         table = pa.Table.from_pydict(data_dict, schema=schema)
    #         writer_obj.write_table(table)
    # finally:
    #     if writer_obj is not None:
    #         writer_obj.close()
    # ## Took 916.4327349662781 secs for 250500000 rows - file size 598 MB

    elapsed_time = time.time() - start_time
    print(f'Total time -- {elapsed_time} secs')

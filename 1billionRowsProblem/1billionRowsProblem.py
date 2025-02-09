import random
from multiprocessing import Pool

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
                 tmp=(-14.5, 120),
                 input_file='1billionRowInput.txt'):
        self.num_of_rows = num_of_rows
        self.cities = cities
        self.batch_size = batch_size
        self.min_tmp, self.max_tmp = tmp
        self.input_file = input_file

    def city_to_tmp_mapper(self):
        return f"{random.choice(self.cities)} : {round(random.uniform(self.min_tmp, self.max_tmp), 2)} \n"
    
    def generate_weather_data(self, batch_rows):
        output_list = []
        for _ in range(batch_rows):
            output_list.append(self.city_to_tmp_mapper())
        return ''.join(output_list)

if __name__ == '__main__':
    import time 
    start_time = time.time()

    batch_size=1000000
    num_of_rows = 25050000
    input_file_generator = GenerateInputFile(num_of_rows, batch_size)

    num_of_batches = num_of_rows // batch_size
    remainder = num_of_rows % batch_size
    batches = []
    for _ in range(num_of_batches):
        batches.append((batch_size))
    if remainder:
        batches.append((remainder))

    with Pool() as pool:
        results = pool.map(input_file_generator.generate_weather_data, batches)

    # Write all generated data to the file sequentially.
    with open('1billionRowInput.txt', 'w') as f:
        for batch_data in results:
            # Since we've checked above, this should be safe.
            f.write(batch_data)

    print(f'Total time -- {start_time - time.time()}')

# Writing to file for 25050000 rows ( Total time -- -13.699316501617432 secs )


    
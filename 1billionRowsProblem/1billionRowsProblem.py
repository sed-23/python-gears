import random
class GenerateInputFile:
    def __init__(self, 
                 num_of_rows=1000000000, 
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
        self.min_tmp, self.max_tmp = tmp
        self.input_file = input_file

    def city_to_tmp_mapper(self):
        return f"{random.choice(self.cities)} : {round(random.uniform(self.min_tmp, self.max_tmp), 2)} \n"
    
    def write_input_file(self):
        with open(self.input_file, 'a') as append_file:
            for iteration in range(self.num_of_rows):
                print(f'Iteration :: {iteration}')
                append_file.write(self.city_to_tmp_mapper())

if __name__ == '__main__':
    import time 
    start_time = time.time()
    input_file_generator = GenerateInputFile(1000000)
    input_file_generator.write_input_file()
    print(f'Total time -- {start_time - time.time()}')


    
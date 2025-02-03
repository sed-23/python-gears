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
            count = 0
            output_list = []
            for iteration in range(self.num_of_rows):
                count += 1
                output_list.append(self.city_to_tmp_mapper())
                if count > 1000000:
                    print(f'Writing batch :: {iteration}')
                    append_file.write(''.join(output_list))
                    count = 0
                    output_list = []
            append_file.write(''.join(output_list))

if __name__ == '__main__':
    import time 
    start_time = time.time()
    input_file_generator = GenerateInputFile()
    input_file_generator.write_input_file()
    print(f'Total time -- {start_time - time.time()}')

# Writing to file for 10000 rows ( Total time -- -3640.038066148758 secs )


    
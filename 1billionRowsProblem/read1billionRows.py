class ProcessBillionRows:
    def __init__(self):
        self.updated_data_map = {}

    def calc_min_max_avg(self, data_dict):
        for tmp_data in data_dict:
            for city, tmp in tmp_data.items():
                if city in self.updated_data_map:
                    city_data = self.updated_data_map[city]
                    count = city_data['count']
                    city_data['count'] += 1
                    print(f'city_data - {city_data} -- {tmp}')
                    city_data['avg'] = (city_data['avg']*count + tmp) / city_data['count']
                    if city_data['min'] > tmp:
                        city_data['min'] = tmp
                    elif city_data['max'] < tmp:
                        city_data['max'] = tmp 
                else:
                    city_data = {
                        'min': tmp,
                        'max': tmp,
                        'avg': tmp, 
                        'count': 1
                    }
                self.updated_data_map[city] = city_data
    
    def read_input_data(self):
        import csv
        with open('1billionRowInput.csv') as file:
            file_data = csv.DictReader(file)
        
        self.calc_min_max_avg(file_data)

    def get_tmp_data(self):
        temparature_data = {}
        for city, tmp_data in self.updated_data_map.items():
            temparature_data[city] = {key: round(value, 2) for key, value in tmp_data.items() if key != 'count'}
        return temparature_data

if __name__ == '__main__':
    print('Processing 1 billion rows')
    process_bilrows = ProcessBillionRows()
    with open('1billionRowInput.txt') as file:
        data_dict = []
        for line in file.readlines():
            line = line.strip().split(':')
            data_dict.append({line[0]: float(line[1])})
    process_bilrows.calc_min_max_avg(data_dict)
    print(f'Min Max and Avg data values - {process_bilrows.get_tmp_data()}')
class ProcessBillionRows:
    def __init__(self):
        self.updated_data_map = {}

    def calc_min_max_avg(self, data_dict):
        for city, tmp in data_dict.items():
            if city in self.updated_data_map:
                city_data = self.updated_data_map['city']
                city_data['count'] += 1
                city_data['avg'] = (city_data['avg'] + tmp) / city_data['count']
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
            self.updated_data_map['city'] = city_data


if __name__ == '__main__':
    print('Processing 1 billion rows')
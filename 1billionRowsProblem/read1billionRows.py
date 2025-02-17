import multiprocessing
import time
import sys
import math

def process_chunk(chunk):
    """
    Process a chunk of data (a list of dict items) and return aggregated results.
    """
    local_map = {}
    for data in chunk:
        for city, tmp in data.items():
            if city in local_map:
                city_data = local_map[city]
                count = city_data['count']
                city_data['count'] += 1
                city_data['avg'] = (city_data['avg'] * count + tmp) / city_data['count']
                city_data['min'] = min(city_data['min'], tmp)
                city_data['max'] = max(city_data['max'], tmp)
            else:
                local_map[city] = {'min': tmp, 'max': tmp, 'avg': tmp, 'count': 1}
    return local_map

def merge_results(map1, map2):
    """
    Merge two aggregated result dictionaries.
    """
    for city, data in map2.items():
        if city in map1:
            data1 = map1[city]
            total_count = data1['count'] + data['count']
            new_avg = ((data1['avg'] * data1['count']) + (data['avg'] * data['count'])) / total_count
            map1[city] = {
                'min': min(data1['min'], data['min']),
                'max': max(data1['max'], data['max']),
                'avg': new_avg,
                'count': total_count
            }
        else:
            map1[city] = data
    return map1

def read_input_file_in_chunks(filename, chunk_size=100000):
    """
    Generator that yields chunks (lists) of dictionaries from the file.
    Each line in the file should be in the format: City:Value
    """
    with open(filename, 'r') as f:
        chunk = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(':')
            if len(parts) != 2:
                continue
            city, tmp = parts
            try:
                data = {city.strip(): float(tmp.strip())}
            except ValueError:
                continue
            chunk.append(data)
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

def count_lines(filename):
    """
    Counts the total number of lines in the input file.
    """
    with open(filename, 'r') as f:
        return sum(1 for _ in f)

def get_final_data(updated_data_map):
    """
    Rounds the aggregated values to two decimal places (excluding 'count').
    """
    temperature_data = {}
    for city, tmp_data in updated_data_map.items():
        temperature_data[city] = {key: round(value, 2) for key, value in tmp_data.items() if key != 'count'}
    return temperature_data

if __name__ == '__main__':
    filename = '1billionRowInput.txt'
    chunk_size = 100000
    start_time = time.time()

    # Count total lines and calculate the number of chunks for progress reporting.
    total_lines = count_lines(filename)
    total_chunks = math.ceil(total_lines / chunk_size)
    chunks_processed = 0

    final_map = {}
    num_processes = multiprocessing.cpu_count()
    print(f'{num_processes=}')
    pool = multiprocessing.Pool(processes=num_processes)

    # Process each chunk in parallel using imap
    for partial_map in pool.imap(process_chunk, read_input_file_in_chunks(filename, chunk_size)):
        final_map = merge_results(final_map, partial_map)
        chunks_processed += 1
        percentage = (chunks_processed / total_chunks) * 100
        # Update progress on the same line
        sys.stdout.write("\rProgress: {:.2f}%".format(percentage))
        sys.stdout.flush()

    pool.close()
    pool.join()

    # Final rounding for the aggregated results
    final_data = get_final_data(final_map)
    print("\nFinal aggregated data:")
    print(final_data)
    print("Processed in: {:.2f} seconds".format(time.time() - start_time))

# Processed in: 372.03 seconds

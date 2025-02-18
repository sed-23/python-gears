use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::sync::mpsc;
use std::time::Instant;

use num_cpus;
use threadpool::ThreadPool;

#[derive(Debug, Clone)]
struct CityData {
    min: f64,
    max: f64,
    avg: f64,
    count: u64,
}

/// Processes a chunk of lines (each line in the format "City:Value")
/// and returns a HashMap of aggregated data.
fn process_chunk(chunk: Vec<String>) -> HashMap<String, CityData> {
    let mut local_map = HashMap::new();

    for line in chunk {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // Use split_once to avoid collecting into a Vec
        if let Some((city_part, value_part)) = line.split_once(':') {
            let city = city_part.trim().to_string();
            let tmp: f64 = match value_part.trim().parse() {
                Ok(val) => val,
                Err(_) => continue,
            };

            local_map
                .entry(city)
                .and_modify(|data: &mut CityData| {
                    let new_count = data.count + 1;
                    data.avg = (data.avg * (data.count as f64) + tmp) / (new_count as f64);
                    data.min = data.min.min(tmp);
                    data.max = data.max.max(tmp);
                    data.count = new_count;
                })
                .or_insert(CityData {
                    min: tmp,
                    max: tmp,
                    avg: tmp,
                    count: 1,
                });
        }
    }
    local_map
}

/// Merges the data from `map2` into `map1`
fn merge_results(map1: &mut HashMap<String, CityData>, map2: HashMap<String, CityData>) {
    for (city, data2) in map2 {
        if let Some(data1) = map1.get_mut(&city) {
            let total_count = data1.count + data2.count;
            let new_avg = ((data1.avg * (data1.count as f64))
                + (data2.avg * (data2.count as f64)))
                / (total_count as f64);
            data1.min = data1.min.min(data2.min);
            data1.max = data1.max.max(data2.max);
            data1.avg = new_avg;
            data1.count = total_count;
        } else {
            map1.insert(city, data2);
        }
    }
}

/// Counts the total number of lines in the given file.
fn count_lines(filename: &str) -> io::Result<usize> {
    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    Ok(reader.lines().count())
}

fn main() -> io::Result<()> {
    let filename = "1billionRowInput.txt";
    let chunk_size = 100_000;
    let start_time = Instant::now();

    // Count total lines to determine number of chunks for progress reporting.
    let total_lines = count_lines(filename)?;
    let total_chunks = (total_lines + chunk_size - 1) / chunk_size;
    let mut chunks_processed = 0;

    // Use a thread pool to limit concurrent threads.
    let num_threads = num_cpus::get();
    let pool = ThreadPool::new(num_threads);
    let (tx, rx) = mpsc::channel();

    // Open the file for reading.
    let file = File::open(filename)?;
    let reader = BufReader::new(file);

    let mut current_chunk = Vec::with_capacity(chunk_size);

    for line in reader.lines() {
        let line = line?;
        current_chunk.push(line);
        if current_chunk.len() >= chunk_size {
            let chunk = current_chunk;
            current_chunk = Vec::with_capacity(chunk_size);
            let tx_clone = tx.clone();
            pool.execute(move || {
                let result = process_chunk(chunk);
                tx_clone.send(result).expect("Failed to send result");
            });
        }
    }
    // Process any remaining lines.
    if !current_chunk.is_empty() {
        let chunk = current_chunk;
        let tx_clone = tx.clone();
        pool.execute(move || {
            let result = process_chunk(chunk);
            tx_clone.send(result).expect("Failed to send result");
        });
    }
    // Drop the original transmitter to signal the receiver when all tasks are done.
    drop(tx);

    let mut final_map: HashMap<String, CityData> = HashMap::new();

    // Receive results from the thread pool and merge them.
    for partial_map in rx {
        merge_results(&mut final_map, partial_map);
        chunks_processed += 1;
        let percentage = (chunks_processed as f64 / total_chunks as f64) * 100.0;
        print!("\rProgress: {:.2}%", percentage);
        io::stdout().flush().unwrap();
    }

    println!("\rProgress: 100.00%");
    println!("\nFinal aggregated data:");
    for (city, data) in &final_map {
        println!("{}: min: {:.2}, max: {:.2}, avg: {:.2}", city, data.min, data.max, data.avg);
    }
    println!("Processed in: {:.2} seconds", start_time.elapsed().as_secs_f64());
    Ok(())
}

// Processed in: 53.83 seconds
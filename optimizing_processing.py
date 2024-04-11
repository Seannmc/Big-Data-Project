import time
import matplotlib.pyplot as plt
from taskdask import dask_task1ap1, dask_task1ap2, dask_task1b
from dask.distributed import Client, LocalCluster
from dask.dataframe import read_csv
import datetime

def execute_loading_of_distance_dataset(filepath, blocksize):
    stime = time.time()
    df = read_csv(filepath, blocksize=blocksize)
    etime = time.time()-stime
    return df, etime

def execute_parallel_tasks(processor, distance_path, trips_path, loc_directory, cluster, client):
    # Calculating time to run parallel tasks
    start_time_parallel = time.time()
    dask_task1ap1(distance_path)
    dask_task1ap2(trips_path)
    dask_task1b(distance_path)
    parallel_time = time.time() - start_time_parallel
    
    # Returning parallel time
    return parallel_time

def plot_execution_times(n_processors, n_processors_time, fastest_processor):
    processors_str = [str(p) for p in n_processors]
    plt.bar(processors_str, n_processors_time.values(), label='Parallel Time')
    
    # Highlighting the fastest processor bar in red
    plt.bar(str(fastest_processor), n_processors_time[fastest_processor], color='red', label='Fastest Processor Number')
    
    plt.xlabel('Number of Processors')
    plt.ylabel('Time (seconds)')
    plt.title('Execution Time Comparison')
    plt.legend()
    plt.grid(True)
    plt.show()

def optimize_loading_of_distance_dataset():
    filepath = "Trips_by_Distance.csv"
    blocksize_increment = 1000000  # Increment by 1 million each pass
    min_blocksize = 300000000
    max_blocksize = 400000000
    block_sizes = list(range(min_blocksize, max_blocksize + 1, blocksize_increment))
    execution_times = [0] * len(block_sizes)  # Initialize list for accumulated times

    for _ in range(100):  # Run the loop 20 times
        print(f"PASS {_}")
        for i, blocksize in enumerate(block_sizes):
            print(f"Loading dataset with blocksize: {blocksize}")
            _, etime = execute_loading_of_distance_dataset(filepath, blocksize)
            execution_times[i] += etime
        print("\n")

    # Calculate the average execution times
    execution_times = [time / 100 for time in execution_times]

    # Finding the index of the fastest execution time
    best_index = execution_times.index(min(execution_times))
    best_blocksize = block_sizes[best_index]

    # Plotting block sizes and execution times
    plt.figure(figsize=(10, 6))
    plt.plot(block_sizes, execution_times, color='blue', label='Average Execution Time')
    plt.scatter(best_blocksize, min(execution_times), color='red', label=f'Fastest Block Size: {best_blocksize}')
    plt.xlabel('Block Size')
    plt.ylabel('Execution Time (seconds)')
    plt.title('Average Execution Time vs. Block Size')
    plt.grid(True)

    plt.legend()
    plt.show()
   
def optimize_parallel_tasks(local_dir):
    whole_start_time = time.time()
    # Change local directory if needed
    local_directory = local_dir
    distance_path = "Trips_by_Distance.csv"
    trips_path = "Trips_Full Data.csv"
    
    # Define the range of processor numbers
    n_processors = list(range(1, 50))
    # Repeat the simulation 'n' times
    n = 10  # Set 'n' to the desired number of repetitions
    
    # Initialize cluster and client outside the loop
    cluster = LocalCluster(local_directory=local_directory, memory_limit='4GB')
    client = Client(cluster)
    
    n_processors_time = {}

    for i in range(n):
        print(f"PASS {i+1}")
        for processor in n_processors:
            if processor not in n_processors_time:
                n_processors_time[processor] = []
            parallel_time = execute_parallel_tasks(processor, distance_path, trips_path, local_directory, cluster, client)
            n_processors_time[processor].append(parallel_time)
            print(f"Parallel execution time for {processor} processor(s): {parallel_time} seconds")
        print("\n")
    # Calculate the average parallel time for each processor
    for processor, times in n_processors_time.items():
        n_processors_time[processor] = sum(times) / len(times)
        
    # Finding the fastest processor
    fastest_processor = min(n_processors_time, key=n_processors_time.get)
    fastest_time = n_processors_time[fastest_processor]
    print(f"\nFastest processor: {fastest_processor} processors, Average execution time: {fastest_time} seconds")

    print("\nAverage Parallel Times:")
    for processor, time_taken in n_processors_time.items():
        print(f"Number of Processors: {processor}, Average Parallel Time: {time_taken}")

    whole_runtime = time.time() - whole_start_time
    # Converting to h/m/s format
    whole_runtime = datetime.timedelta(seconds=whole_runtime)
    print(f"Total run time for program: {whole_runtime}")

    plot_execution_times(n_processors, n_processors_time, fastest_processor)

if __name__ == '__main__':
    whole_start_time = time.time()
    # Change local directory if needed
    local_directory = r'C:\Users\seanm\Desktop\Uni Work\Year 2\Term 2\Big data project\Assesment\Code'
    distance_path = "Trips_by_Distance.csv"
    trips_path = "Trips_Full Data.csv"
    #optimize_loading_of_distance_dataset()
    optimize_parallel_tasks(local_directory)
    


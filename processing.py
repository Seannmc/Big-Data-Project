import time
import matplotlib.pyplot as plt
from taskdask import dask_task1ap1, dask_task1ap2, dask_task1b
from taskpandas import pandas_task1ap1, pandas_task1ap2, pandas_task1b
from dask.distributed import Client, LocalCluster
import datetime

def execute_parallel_tasks(processor, distance_path, trips_path, loc_directory, cluster, client):
    # Calculating time to run parallel tasks
    start_time_parallel = time.time()
    dask_task1ap1(distance_path)
    dask_task1ap2(trips_path)
    dask_task1b(distance_path)
    parallel_time = time.time() - start_time_parallel
    
    # Returning parallel time
    return parallel_time
    

def plot_execution_times(n_processors, n_processors_time, serial_time, fastest_processor):
    plt.bar(n_processors, n_processors_time.values(), label='Parallel Time')
    plt.bar(fastest_processor, n_processors_time[fastest_processor], color='red', label='Fastest Processor')
    plt.axhline(y=serial_time, color='r', label='Serial Time')
    plt.xlabel('Number of Processors')
    plt.ylabel('Time (seconds)')
    plt.title('Execution Time Comparison')
    plt.legend()
    plt.grid(True)
    plt.show()

if __name__ == '__main__':
    whole_start_time = time.time()
    # Change local directory if needed
    local_directory = r'C:\Users\seanm\Desktop\Uni Work\Year 2\Term 2\Big data project\Assesment\Code'
    distance_path = "Trips_by_Distance.csv"
    trips_path = "Trips_Full Data.csv"
    
    # Define the range of processor numbers
    n_processors = [10, 20]
    # Repeat the simulation 'n' times
    n = 5  # Set 'n' to the desired number of repetitions
    
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
    
    print("Executing tasks Serially")
    start_time_serial = time.time()
    pandas_task1ap1_result = pandas_task1ap1(distance_path)
    pandas_task1ap2_result = pandas_task1ap2(trips_path)
    pandas_task1b_result = pandas_task1b(distance_path)
    serial_time = time.time() - start_time_serial
        
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

    plot_execution_times(n_processors, n_processors_time, serial_time, fastest_processor)



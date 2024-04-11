import dask.dataframe as dd
import matplotlib.pyplot as plt
import dask
import dask.config
def display_10_entries_dask(*dfs):
    '''Function for peeking at DataFrames'''
    for i, df in enumerate(dfs):
        print(f"First 10 entries for DataFrame {i+1}:")
        print(df.head(10))

def dask_task1ap1(filepath, blocksize=322000000):
    newDataTypes = {'County Name': 'object',
                    'Number of Trips': 'float64',
                    'Number of Trips 1-3': 'float64',
                    'Number of Trips 10-25': 'float64',
                    'Number of Trips 100-250': 'float64',
                    'Number of Trips 25-50': 'float64',
                    'Number of Trips 250-500': 'float64',
                    'Number of Trips 3-5': 'float64',
                    'Number of Trips 5-10': 'float64',
                    'Number of Trips 50-100': 'float64',
                    'Number of Trips <1': 'float64',
                    'Number of Trips >=500': 'float64',
                    'Population Not Staying at Home': 'float64',
                    'Population Staying at Home': 'float64',
                    'State Postal Code': 'object',
                    'Week': 'int64', 
                    'Month': 'int64'}

    # Read the dataset into a Dask DataFrame
    df1 = dd.read_csv(filepath, dtype=newDataTypes, blocksize=blocksize)

    # Filtering by National level
    df1 = df1[df1['Level'] == 'National']

    # Removing duplicate values
    df1 = df1.drop_duplicates(subset=['Week'])

    # Removing null values
    df1 = df1.dropna(subset=['Week'])
    df1 = df1.dropna(subset=['Population Staying at Home'])

    # Sorting by 'Week' column in ascending order
    df1 = df1.sort_values(by='Week')

    # Grouping by week and calculating the average number of people staying at home
    mean_population_at_home_by_week = df1.groupby('Week')['Population Staying at Home'].mean()

    # Sort this average in ascending order
    sorted_mean_population_at_home_by_week = mean_population_at_home_by_week.compute().sort_values()

    return mean_population_at_home_by_week, sorted_mean_population_at_home_by_week

def dask_task1ap2(filepath):
    # Read the dataset into a Dask DataFrame
    df2 = dd.read_csv(filepath)

    # Extract distances from the 'Trips' columns
    distance_columns = [col for col in df2.columns if 'Trips' in col and col != 'Trips']

    # Calculate the mean number of people traveling each distance
    mean_people_per_distance = df2[distance_columns].mean()

    # Sort this average in ascending order
    sorted_mean_people_per_distance = mean_people_per_distance.compute().sort_values()

    distance_columns = ['Trips <1 Mile', 'Trips 1-3 Miles', 'Trips 3-5 Miles', 'Trips 5-10 Miles',
                        'Trips 10-25 Miles', 'Trips 25-50 Miles', 'Trips 50-100 Miles',
                        'Trips 100-250 Miles', 'Trips 250-500 Miles', 'Trips 500+ Miles']

    # Compute the average for each distance category
    new_mean_people_per_distance = df2[distance_columns].mean()

    # Sort this average in ascending order
    sorted_new_mean_people_per_distance = new_mean_people_per_distance.compute().sort_values()
    return mean_people_per_distance, sorted_mean_people_per_distance, new_mean_people_per_distance, sorted_new_mean_people_per_distance

def dask_task1b(filepath, blocksize = 322000000):

    dtype={'County Name': 'object',
       'Number of Trips': 'float64',
       'Number of Trips 1-3': 'float64',
       'Number of Trips 10-25': 'float64',
       'Number of Trips 100-250': 'float64',
       'Number of Trips 25-50': 'float64',
       'Number of Trips 250-500': 'float64',
       'Number of Trips 3-5': 'float64',
       'Number of Trips 5-10': 'float64',
       'Number of Trips 50-100': 'float64',
       'Number of Trips <1': 'float64',
       'Number of Trips >=500': 'float64',
       'Population Not Staying at Home': 'float64',
       'Population Staying at Home': 'float64',
       'State Postal Code': 'object'}
    
    # Reading data
    df3 = dd.read_csv(filepath, dtype=dtype, blocksize=blocksize)

    # Filtering by National level
    df3 = df3[df3['Level'] == 'National']

    # Filter the data for trips where more than 10,000,000 people conducted 10-25 trips
    trips_10_25 = df3[df3['Number of Trips 10-25'] > 10000000][['Date', 'Number of Trips 10-25']]

    # Filter the data for trips where more than 10,000,000 people conducted 50-100 trips
    trips_50_100 = df3[df3['Number of Trips 50-100'] > 10000000][['Date', 'Number of Trips 50-100']]

    # Returning values for graphing
    return trips_10_25, trips_50_100

def dask_graph_task1ap1(task1ap1_result):
    mean_population_at_home_by_week, sorted_mean_population_at_home_by_week = task1ap1_result
    # Increasing figure size to prevent overlap
    plt.figure(figsize=(10, 12))

    # Plotting the bar chart
    plt.subplot(3, 1, 1)  # 3 rows, 1 column, subplot 1
    mean_population_at_home_by_week.compute().plot(kind='bar', color='blue', zorder=2)
    plt.title('Average Number of People Staying at Home per Week Using Parallel Processing')
    plt.xlabel('Week')
    plt.ylabel('Average Population Staying at Home')
    plt.xticks(rotation=45)
    plt.grid(True, axis='y')  # Grid only on y-axis

    # Plotting the sorted bar chart
    plt.subplot(3, 1, 2)  # 3 rows, 1 column, subplot 2
    sorted_mean_population_at_home_by_week.plot(kind='bar', color='red', zorder=2)
    plt.title('Sorted average Number of People Staying at Home per Week Using Parallel Processing')
    plt.xlabel('Week')
    plt.ylabel('Average Population Staying at Home')
    plt.grid(True, axis='y')  # Grid only on y-axis

    # Plotting the histogram
    plt.subplot(3, 1, 3)  # 3 rows, 1 column, subplot 3
    plt.hist(mean_population_at_home_by_week.compute(), bins=10, color='blue', zorder=2)
    plt.title('Distribution of Average Number of People Staying at Home per Week Using Parallel Processing')
    plt.xlabel('Average Population Staying at Home')
    plt.ylabel('Frequency')
    plt.grid(True, axis='y')  # Grid only on y-axis

    plt.subplots_adjust(hspace=0.5)

    # Show the plot
    plt.show()

def dask_graph_task1ap2(task1ap2_result):
    mean_people_per_distance, sorted_mean_people_per_distance, new_mean_people_per_distance, sorted_new_mean_people_per_distance = task1ap2_result
    # Creating the subplots
    plt.figure(figsize=(12, 10))

    # Plotting the histogram for mean people per distance
    plt.subplot(2, 2, 1)
    mean_people_per_distance.compute().plot.bar(color='blue', zorder=2)
    plt.title('Mean Number of People Traveling vs. Distance (Without Reorganised Columns) Using Parallel Processing')
    plt.xlabel('Distance')
    plt.ylabel('Mean Number of People')
    plt.xticks(rotation=90)  # Rotate x-axis labels for better visibility
    plt.grid(True, axis='y')  # Grid only on y-axis

    # Plotting the histogram for the sorted mean people per distance
    plt.subplot(2, 2, 2)
    sorted_mean_people_per_distance.plot.bar(color='red', zorder=2)
    plt.title('Mean Number of People Traveling (Without Reorganised Columns) in Order Using Parallel Processing')
    plt.xlabel('Distance')
    plt.ylabel('Mean Number of People')
    plt.xticks(rotation=90)  # Rotate x-axis labels for better visibility
    plt.grid(True, axis='y')  # Grid only on y-axis

    # Plotting the histogram for the mean people per distance with amended columns
    plt.subplot(2, 2, 3)
    new_mean_people_per_distance.compute().plot.bar(color='blue', zorder=2)
    plt.title('Mean Number of People Traveling (With Reorganised Columns)')
    plt.xlabel('Distance')
    plt.ylabel('Mean Number of People')
    plt.xticks(rotation=90)  # Rotate x-axis labels for better visibility
    plt.grid(True, axis='y')  # Grid only on y-axis

    # Plotting the histogram for the mean people per distance with amended columns
    plt.subplot(2, 2, 4)
    sorted_new_mean_people_per_distance.plot.bar(color='red', zorder=2)
    plt.title('Mean Number of People Traveling (With Reorganised Columns) in Order Using Parallel Processing')
    plt.xlabel('Distance')
    plt.ylabel('Mean Number of People')
    plt.xticks(rotation=90)  # Rotate x-axis labels for better visibility
    plt.grid(True, axis='y')  # Grid only on y-axis

    # Adjust layout to prevent label overlap
    plt.tight_layout()

    # Show the plot
    plt.show()

def dask_graph_task1b(trips_10_25, trips_50_100):
    # Create figure and axes
    fig, axs = plt.subplots(1, 2, figsize=(12, 6))

    # Scatter plot for 10-25 mile trips
    dask.delayed(lambda trips, ax: trips.compute().plot.scatter(x='Date', y='Number of Trips 10-25', color='blue', ax=ax))(
        trips_10_25, axs[0])
    axs[0].set_title('10-25 Mile Trips Using Serial Processing')
    axs[0].set_ylabel('Number of Trips')
    axs[0].set_xlabel('Date')
    axs[0].xaxis.set_major_locator(plt.MaxNLocator(12))  # Set maximum number of ticks to 12 for months

    # Scatter plot for 50-100 mile trips
    dask.delayed(lambda trips, ax: trips.compute().plot.scatter(x='Date', y='Number of Trips 50-100', color='red', ax=ax))(
        trips_50_100, axs[1])
    axs[1].set_title('50-100 Mile Trips Using Serial Processing')
    axs[1].set_ylabel('Number of Trips')
    axs[1].set_xlabel('Date')
    axs[1].xaxis.set_major_locator(plt.MaxNLocator(12))  # Set maximum number of ticks to 12 for months

    # Compute the delayed operations in parallel
    dask.compute(*dask.persist(axs))

    # Explicitly compute the delayed computations
    trips_10_25, trips_50_100 = dask.compute(trips_10_25, trips_50_100)

    # Plot the computed data
    trips_10_25.plot.scatter(x='Date', y='Number of Trips 10-25', color='blue', ax=axs[0])
    trips_50_100.plot.scatter(x='Date', y='Number of Trips 50-100', color='red', ax=axs[1])

    # Adjust layout
    plt.tight_layout()

    # Show plot
    plt.show()

if __name__ == "__main__":

    distance_path = "Trips_by_Distance.csv"
    trips_path = "Trips_Full Data.csv"
    blocksize = 20000000

    # Getting returns from part 1 and part 2
    dask_task1ap1_result, dask_task1ap2_result = dask_task1ap1(distance_path, blocksize), dask_task1ap2(trips_path)

    trips_10_25, trips_50_100 = dask_task1b(distance_path)

    dask_graph_task1ap1(dask_task1ap1_result)
    dask_graph_task1ap2(dask_task1ap2_result)
    dask_graph_task1b(trips_10_25, trips_50_100)

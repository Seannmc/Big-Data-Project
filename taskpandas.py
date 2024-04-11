import pandas as pd
import matplotlib.pyplot as plt

def pandas_task1ap1(filepath):
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

    # Read the dataset into a pandas DataFrame
    df1 = pd.read_csv(filepath, dtype=newDataTypes)

    # Filtering by National level and ascending week
    df1= df1[df1['Level'] == 'National']
    df1 = df1.sort_values(by='Week')

    # Filtering unique values for columns needed by removing duplicates
    df1 = df1.drop_duplicates(subset=['Week'])

    # Removing null values
    df1 = df1.dropna(subset=['Week'])
    df1 = df1.dropna(subset=['Population Staying at Home'])

    # Grouping by week and calculating the average number of people staying at home
    mean_population_at_home_by_week = df1.groupby('Week')['Population Staying at Home'].mean()

    # Sort this average in ascending order
    sorted_mean_population_at_home_by_week = mean_population_at_home_by_week.sort_values()

    return mean_population_at_home_by_week, sorted_mean_population_at_home_by_week

def pandas_task1ap2(filepath):

    # Read the dataset into a pandas DataFrame
    df2 = pd.read_csv(filepath)

    # Extract distances from the 'Trips' columns
    distance_columns = [col for col in df2.columns if 'Trips' in col and col != 'Trips']

    # Calculate the mean number of people traveling each distance
    mean_people_per_distance = df2[distance_columns].mean()

    # Sort this average in ascending order
    sorted_mean_people_per_distance = mean_people_per_distance.sort_values()

    distance_columns = ['Trips <1 Mile', 'Trips 1-3 Miles', 'Trips 3-5 Miles', 'Trips 5-10 Miles',
                        'Trips 10-25 Miles', 'Trips 25-50 Miles', 'Trips 50-100 Miles',
                        'Trips 100-250 Miles', 'Trips 250-500 Miles', 'Trips 500+ Miles']

    # Compute the average for each distance category
    new_mean_people_per_distance = df2[distance_columns].mean()

    # Sort this average in ascending order
    sorted_new_mean_people_per_distance = new_mean_people_per_distance.sort_values()

    return mean_people_per_distance, sorted_mean_people_per_distance, new_mean_people_per_distance, sorted_new_mean_people_per_distance

def pandas_task1b(filepath):

    # Reading data
    df3 = pd.read_csv(filepath)

    # Filtering by National level
    df3 = df3[df3['Level'] == 'National']

    # Filter the data for trips where more than 10,000,000 people conducted 10-25 trips
    trips_10_25 = df3[df3['Number of Trips 10-25'] > 10000000][['Date', 'Number of Trips 10-25']]

    # Filter the data for trips where more than 10,000,000 people conducted 50-100 trips
    trips_50_100 = df3[df3['Number of Trips 50-100'] > 10000000][['Date', 'Number of Trips 50-100']]

    # Returning values for graphing
    return trips_10_25, trips_50_100

def pandas_graph_task1ap1(task1ap1_result):
    mean_population_at_home_by_week, sorted_mean_population_at_home_by_week = task1ap1_result

    # Increasing figure size to prevent overlap
    plt.figure(figsize=(10, 12))

    # Plotting the bar chart
    plt.subplot(3, 1, 1)  # 3 rows, 1 column, subplot 1
    mean_population_at_home_by_week.plot(kind='bar', color='blue', zorder=2)
    plt.title('Average Number of People Staying at Home per Week Using Serial Processing')
    plt.xlabel('Week')
    plt.ylabel('Average Population Staying at Home')
    plt.xticks(rotation=45)
    plt.grid(True, axis='y')  # Grid only on y-axis

    # Plotting the sorted bar chart
    plt.subplot(3, 1, 2)  # 3 rows, 1 column, subplot 2
    sorted_mean_population_at_home_by_week.plot(kind='bar', color='red', zorder=2)
    plt.title('Sorted average Number of People Staying at Home per Week Using Serial Processing')
    plt.xlabel('Week')
    plt.ylabel('Average Population Staying at Home')
    plt.grid(True, axis='y')  # Grid only on y-axis

    # Plotting the histogram
    plt.subplot(3, 1, 3)  # 3 rows, 1 column, subplot 3
    plt.hist(mean_population_at_home_by_week, bins=10, color='blue', zorder=2)
    plt.title('Distribution of Average Number of People Staying at Home per Week Using Serial Processing')
    plt.xlabel('Average Population Staying at Home')
    plt.ylabel('Frequency')
    plt.grid(True, axis='y')  # Grid only on y-axis

    plt.subplots_adjust(hspace=0.5)

    # Show the plot
    plt.show()


def pandas_graph_task1ap2(task1ap2_result):
    mean_people_per_distance, sorted_mean_people_per_distance, new_mean_people_per_distance, sorted_new_mean_people_per_distance = task1ap2_result
    # Creating the subplots
    plt.figure(figsize=(12, 10))

    # Plotting the histogram for mean people per distance
    plt.subplot(2, 2, 1)
    plt.bar(mean_people_per_distance.index, mean_people_per_distance.values, color='blue', zorder=2)
    plt.title('Mean Number of People Traveling vs. Distance (Without Reorganised Columns ) Using Serial Processing')
    plt.xlabel('Distance')
    plt.ylabel('Mean Number of People')
    plt.xticks(rotation=90)  # Rotate x-axis labels for better visibility
    plt.grid(True, axis='y')  # Grid only on y-axis

    # Plotting the histogram for the sorted mean people per distance
    plt.subplot(2, 2, 2)
    plt.bar(sorted_mean_people_per_distance.index, sorted_mean_people_per_distance.values, color='red', zorder=2)
    plt.title('Mean Number of People Traveling (Without Reorganised Columns) in Order Using Serial Processing')
    plt.xlabel('Distance')
    plt.ylabel('Mean Number of People')
    plt.xticks(rotation=90)  # Rotate x-axis labels for better visibility
    plt.grid(True, axis='y')  # Grid only on y-axis

    # Plotting the histogram for the mean people per distance with amended columns
    plt.subplot(2, 2, 3)
    plt.bar(new_mean_people_per_distance.index, new_mean_people_per_distance.values, color='blue', zorder=2)
    plt.title('Mean Number of People Traveling (With Reorganised Columns) Using Serial Processing')
    plt.xlabel('Distance')
    plt.ylabel('Mean Number of People')
    plt.xticks(rotation=90)  # Rotate x-axis labels for better visibility
    plt.grid(True, axis='y')  # Grid only on y-axis

    # Plotting the histogram for the mean people per distance with amended columns
    plt.subplot(2, 2, 4)
    plt.bar(sorted_new_mean_people_per_distance.index, sorted_new_mean_people_per_distance.values, color='red', zorder=2)
    plt.title('Mean Number of People Traveling (With Reorganised Columns) in Order Using Serial Processing')
    plt.xlabel('Distance')
    plt.ylabel('Mean Number of People')
    plt.xticks(rotation=90)  # Rotate x-axis labels for better visibility
    plt.grid(True, axis='y')  # Grid only on y-axis

    # Adjust layout to prevent label overlap
    plt.tight_layout()

    # Show the plot
    plt.show()

def pandas_graph_task1b(trips_10_25, trips_50_100):
    # Create figure and axes
    fig, axs = plt.subplots(1, 2, figsize=(12, 6))

    # Scatter plot for 10-25 mile trips
    axs[0].scatter(trips_10_25['Date'], trips_10_25['Number of Trips 10-25'], color='blue')
    axs[0].set_title('10-25 Mile Trips Using Serial Processing')
    axs[0].set_xlabel('Date')
    axs[0].set_ylabel('Number of Trips')

    # Set ticks for every month
    axs[0].xaxis.set_major_locator(plt.MaxNLocator(12))  # Set maximum number of ticks to 12 for months

    # Scatter plot for 50-100 mile trips
    axs[1].scatter(trips_50_100['Date'], trips_50_100['Number of Trips 50-100'], color='red')
    axs[1].set_title('50-100 Mile Trips Using Serial Processing')
    axs[1].set_xlabel('Date')
    axs[1].set_ylabel('Number of Trips')

    # Set ticks for every month
    axs[1].xaxis.set_major_locator(plt.MaxNLocator(12))  # Set maximum number of ticks to 12 for months

    # Adjust layout
    plt.tight_layout()

    # Show plot
    plt.show()

if __name__ == "__main__":
    # Example calls 
    distance_path = "Trips_by_Distance.csv"
    trips_path = "Trips_Full Data.csv"

    # 1a
    pandas_task1ap1_result, pandas_task1ap2_result = pandas_task1ap1(distance_path), pandas_task1ap2(trips_path)
    # 1b
    trips_10_25, trips_50_100 = pandas_task1b(distance_path)
    print(trips_10_25)
    print(trips_50_100)

    # Graphs
    pandas_graph_task1ap1(pandas_task1ap1_result)
    pandas_graph_task1ap2(pandas_task1ap2_result)
    pandas_graph_task1b(trips_10_25, trips_50_100)




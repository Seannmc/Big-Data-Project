import dask.dataframe as dd
import matplotlib.pyplot as plt

# Reading dataset
df = dd.read_csv('Trips_Full Data.csv')


# Extracting distances from the 'Trips' columns
distance_columns = [col for col in df.columns if 'Trips' in col and col != 'Trips']


# Defining new distance columns for mean
distance_columns = ['Trips <1 Mile', 'Trips 1-3 Miles', 'Trips 3-5 Miles', 'Trips 5-10 Miles',
                    'Trips 10-25 Miles', 'Trips 25-50 Miles', 'Trips 50-100 Miles',
                    'Trips 100-250 Miles', 'Trips 250-500 Miles', 'Trips 500+ Miles']

# Compute the average for each distance category
df = df[distance_columns].mean()

# Plotting bar chart
plt.figure(figsize=(10, 6))
df[distance_columns].compute().plot(kind='bar', color='blue')
plt.title('Number of Participants')
plt.xlabel('Days')
plt.ylabel('Number of Participants')
plt.grid(axis='y')
plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels
plt.show()

# Plotting histogram
plt.figure(figsize=(10, 6))
plt.hist(df[distance_columns].compute(), bins=10, color='red', edgecolor='black')
plt.title('Histogram of Number of Participants')
plt.xlabel('Number of Participants')
plt.ylabel('Frequency')
plt.grid(axis='y')
plt.show()
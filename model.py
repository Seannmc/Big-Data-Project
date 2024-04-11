import dask.array as da
import dask.dataframe as dd
from sklearn.linear_model import LinearRegression 
from dask_ml.model_selection import train_test_split
from dask.diagnostics import ProgressBar
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt

dtype = {'County Name': 'object',
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

# Importing datasets using dask
trips_by_distance = dd.read_csv('Trips_by_Distance.csv', dtype=dtype)
trips_full_data = dd.read_csv('Trips_Full Data.csv')

# Converting Date in Trips_by_Distance to m/d/y format
trips_by_distance['Date'] = dd.to_datetime(trips_by_distance['Date'])

# Filtering the dataset by year = 2019, week = 32, level = National
filtered_distance_data = trips_by_distance[
    (trips_by_distance['Date'].dt.year == 2019) &
    (trips_by_distance['Week'] == 32) &
    (trips_by_distance['Level'] == 'National')
]

# Defining columns for distance
distance_columns = [
    'Number of Trips 1-3',
    'Number of Trips 3-5',
    'Number of Trips 5-10',
    'Number of Trips 10-25'
]
# Defining columns for trips
trips_columns = ['Trips 1-25 Miles']

# Filtering datasets based on defined columns
x= filtered_distance_data[distance_columns]
y = trips_full_data[trips_columns]

# Training model
model = LinearRegression() 
model.fit(x, y) 
y_pred = model.predict(x) 

# Calculating model information
r_sq = model.score(x, y) 
intercept = model.intercept_
coefficients = model.coef_[0]
equation = f'y = {intercept:}'
for i, coef in enumerate(coefficients):
    equation += f' + {coef:} * x{i+1}'

# Displaying model information
print(f"Coefficient of determination: {r_sq}") 
print(f"Intercept: {intercept}") 
print(f"Coefficients: {coefficients}") 
print(f"Equation: {equation}")
print(f"predicted response:\n{y_pred}") 

# Plotting the predicted response against the actual values
plt.scatter(y.compute(), y_pred, color='black', marker='x')  
plt.plot(y.compute(), y.compute(), color='red', linewidth=3) 
plt.xlabel('Actual Trips 1-25 Miles')
plt.ylabel('Predicted Trips 1-25 Miles')
plt.title('Actual vs Predicted Trips 1-25 Miles')
plt.show()
# Data Engineering Technical Challenge

This repository contains a Python data pipeline for transforming and cleansing battery time series data. The data pipeline performs data cleansing, calculates total grid_purchase and grid_feedin for each hour of the day, and create new dataframe to contain a column indicating the hour with the highest grid_feedin of the day.


## Instructions

### Running the Data Pipeline Locally

1. Make sure you have Docker installed on your system.

2. Clone this repository to your local machine:

3. Place your dataset in the following directory :  
"data/raw/measurements.csv"

4. Run application : `python data_pipeline.py`

5. After the pipeline completes, you will find the transformed csv file in the `data/processed/` folder.


### Running Docker image

1. Build the Docker image:
`docker build -t data_pipeline:wontak .`

2. Run the Docker container to execute the data pipeline:
`docker run -it data_pipeline:wontak .`


### Data Pipeline Details

The `data_pipeline.py` script performs the following tasks:

1. Loads the `measurements.csv` CSV file.
2. Cleanses the data by replacing incorrect values with null values, converting columns to appropriate data types, and removing duplicates.
3. Calculates the total `grid_purchase` and `grid_feedin` for each hour of the day.
4. create new dataframe to save the result of hourly aggregated data and add a column to indicate the highest `sum_grid_feedin` of the day.
5. Saves the `result.csv` CSV file to data/processed folder.


### Dependencies

The data pipeline script uses the pandas library for data manipulation. The Docker image includes all the required dependencies.
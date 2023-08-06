import os
import time
import pandas as pd

def extract_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, delimiter=';')
    return df

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    # process_data function is to check numeric columns and
    # update them properly if missing or incorrect data exist.
    
    # In order to change data type of grid_purchase and grid_feedin properly,
    # Dev test' on both columns needs to be updated to NaN.
    df['grid_purchase'].replace('Dev test', pd.NA, inplace=True)
    df['grid_feedin'].replace('Dev test', pd.NA, inplace=True)
    
    # data type for grid_purchase and grid_feedin to numeric(float64)
    df['grid_purchase'] = pd.to_numeric( df['grid_purchase'])
    df['grid_feedin'] = pd.to_numeric(df['grid_feedin'])
    
    # 'timestamp' and 'data' columns to timestamp data type
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['date'] = pd.to_datetime(df['date'])
    
    # remove duplications
    df.drop_duplicates(inplace=True)
    
    return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    # hourly sum of grid_purchase and grid_feedin data are aggregated into hourly_df dataframe
    hourly_df = df.groupby(by=df['timestamp'].dt.hour).agg({'grid_purchase': 'sum', 'grid_feedin': 'sum'}).reset_index()
    
    hourly_df.columns = ['hour', 'sum_grid_purchase', 'sum_grid_feedin']
    
    # by ranking sum_grid_feedin, we can do further analysis later, for example the worst hour or top 5 hours etc.
    hourly_df['grid_feedin_rank'] = hourly_df['sum_grid_feedin'].rank(ascending=False)
    hourly_df['is_max'] = False
    hourly_df.loc[hourly_df['grid_feedin_rank']==1, 'is_max'] = True
    
    print('='*80)
    print(f"transformed dataframe is: \n\n{hourly_df}")
    print('='*80)
    
    return hourly_df

def save_data(df: pd.DataFrame, result_dir_path: str) -> None:
    result_csv_path = os.path.join(result_dir_path, 'result.csv')
    
    try:
        df.to_csv(result_csv_path, index=False, mode='x')
    except FileExistsError:
        df.to_csv(os.path.join(result_dir_path, f'result-{time.strftime("%Y%m%d-%H%M%S")}.csv'))
        
    print('\n')      
    print(f"transformed data is now saved at {result_csv_path}")
    print('='*80)
    
def main_flow():
    # update csv path to absolute path
    raw_csv_path = os.path.join(os.getcwd(), 'data', 'raw', 'measurements.csv')
    result_dir_path = os.path.join(os.getcwd(), 'data', 'processed')
    
    # Dataset ETL start
    # check dataset exists in the folder
    try:
        raw_data = extract_data(raw_csv_path)
    except FileNotFoundError:
        print(f"Dataset is not found. Please check if file exists : {raw_csv_path}")
        return
    
    processed_data = process_data(raw_data)
    transformed_data = transform_data(processed_data)
    save_data(transformed_data, result_dir_path)

if __name__ == '__main__':
    main_flow()

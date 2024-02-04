#Data Loader

        import io
        import pandas as pd
        import requests
        if 'data_loader' not in globals():
            from mage_ai.data_preparation.decorators import data_loader
        if 'test' not in globals():
            from mage_ai.data_preparation.decorators import test
        
        @data_loader
        def load_data_from_api(*args, **kwargs):
            base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_'
            
            taxi_dtypes = {
                'VendorID': pd.Int64Dtype(),
                'passenger_count': pd.Int64Dtype(),
                'trip_distance': float,
                'RatecodeID': pd.Int64Dtype(),
                'store_and_fwd_flag': str,
                'PULocationID': pd.Int64Dtype(),
                'DOLocationID': pd.Int64Dtype(),
                'payment_type': pd.Int64Dtype(),
                'fare_amount': float,
                'extra': float,
                'mta_tax': float,
                'tip_amount': float,
                'tolls_amount': float,
                'improvement_surcharge': float,
                'total_amount': float,
                'congestion_surcharge': float
            }
        
            # Native date parsing 
            parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
        
            # Specify the desired year and months
            year = 2020
            months = [10, 11, 12]
        
            # Read data for the specified months
            final_months_data = []
            for month in months:
                file_url = f'{base_url}{year}-{month:02}.csv.gz'
                month_data = pd.read_csv(file_url, compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
                final_months_data.append(month_data)
        
            data = pd.concat(final_months_data, ignore_index=True)
        
            return data
        
        
        @test
        def test_output(output, *args) -> None:
            """
            Template code for testing the output of the block.
            """
            assert output is not None, 'The output is undefined'



#Transfermer
        
        if 'transformer' not in globals():
            from mage_ai.data_preparation.decorators import transformer
        if 'test' not in globals():
            from mage_ai.data_preparation.decorators import test
        
        import inflection
        
        @transformer
        def transform(data, *args, **kwargs):
            data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
            data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
            data['lpep_dropoff_date'] = data['lpep_dropoff_datetime'].dt.date
        
            original_columns = data.columns.tolist()
            data.columns = [inflection.underscore(col) for col in data.columns]
            renamed_columns_count = sum(original != modified for original, modified in zip(original_columns, data.columns))
            print(f"Renamed {renamed_columns_count} columns from Camel Case to Snake Case.")
        
            unique_vendor_ids = data['vendor_id'].unique()
            print("Unique values of vendor_id:", unique_vendor_ids)
        
            unique_lpep_pickup_date=data['lpep_pickup_date'].nunique()
            print("Unique values of pickup date:", unique_lpep_pickup_date)
        
            return data
        
        
        
        
        @test
        def test_output(output, *args) -> None:
            assert 'vendor_id' in output.columns, 'Assertion Error: vendor_id is not one of the existing values.'
            assert (output['passenger_count'] > 0).all(), 'Assertion Error: passenger_count is not greater than 0.'
            assert (output['trip_distance'] > 0).all(), 'Assertion Error: trip_distance is not greater than 0.'
        
        


#Exporter
        
        import pyarrow as pa
        import pyarrow.parquet as pq 
        import os 
        
        if 'data_exporter' not in globals():
            from mage_ai.data_preparation.decorators import data_exporter
        
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/src/dtc-de-course-410814-5a21403e1c63.json'
        
        
        bucket_name='green_taxi2019'
        project_id = 'dtc-de-course-410814'
        
        table_name = 'nyc_taxi_data_W2'
        
        root_path = f'{bucket_name}/{table_name}'
        
        @data_exporter
        def export_data(data, *args, **kwargs):
            table = pa.Table.from_pandas(data)
            gcs = pa.fs.GcsFileSystem()
            pq.write_to_dataset(
                table,
                root_path = root_path,
                partition_cols=['lpep_pickup_date'],
                filesystem = gcs
            )

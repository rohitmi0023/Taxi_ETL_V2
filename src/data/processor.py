"""
Data processing utilities for Taxi ETL Dashboarding project.
Handles data transformations, type conversions, and memory optimization.
"""
import pandas as pd

from ..utils.exceptions import MemoryOptimizationError
from ..utils.logger import get_logger

class DataProcessor:
    def __init__(self):
        self.logger = get_logger(__name__)

    def convert_datetime_columns(self, df, columns,errors='coerce'):
        self.logger.info(f'Executing function {self.convert_datetime_columns.__name__}....')
        df_copy = df.copy()
        for col in columns:
            if col in df_copy.columns:
                try:
                    df_copy[col] = pd.to_datetime(df_copy[col], errors=errors)
                    total_count = len(df_copy[col])
                    converted_count = df_copy[col].notna().sum()
                    self.logger.info(f'Column {col} converted to datetime for {converted_count} out of {total_count} successful')
                    if errors =='coerce':
                        null_count = df_copy[col].isna().sum()
                        if null_count > 0:
                            self.logger.warning(f'Failed to convert column {col}: {null_count} out of {total_count}')

                except Exception as e:
                    self.logger.warning(f'Datetime conversion failed for column {col}: {e}') 
            else:
                self.logger.warning(f'Column {col} not found in DataFrame!')
        self.logger.error(f'Completed executing function: {self.convert_datetime_columns.__name__}')
        return df_copy
    
    def optimize_data_types(self, df):
        print(f'Executing function {self.optimize_data_types.__name__}...')
        try:
            df_copy = df.copy()
            memory_before = df_copy.memory_usage(deep=True, index=False).sum()/1024/1024
            self.logger.info(f'Initial memory usage: {memory_before:.2f} MB')
            type_changes = []
            for col in df_copy.columns:
                starting_dtype = df_copy[col].dtype
                try:
                    # optimize object columns
                    if df_copy[col].dtype == 'object':
                        df_copy[col] = self._optimize_object_column(df_copy[col])

                    # optimize integer columns

                    # optimize float columns

                    ending_dtype = df_copy[col].dtype
                    # log type change
                    if starting_dtype != ending_dtype:
                        type_changes.append({
                            'column': col,
                            'from': str(starting_dtype),
                            'to': str(ending_dtype)
                        })
                    self.logger.debug(f'Column {col} changed from {starting_dtype} to {ending_dtype}')
                except Exception as e:
                    self.logger.warning(f'Could not optimize column {col}: {e}')
            
            memory_after = df_copy.memory_usage(deep=True, index=False).sum()/1024/1024
            reduction = ((memory_before - memory_after)/memory_before)*100'
            self.logger.info('Memory Usage after Optimization: {memory_after:.2f} MB, reduction of {reduction:.2f}%')
            self.logger.info(f'Type changes: {len(type_changes)} columns optimized')
            self.logger.debug(f'Completed executing function {self.optimize_data_types.__name__}')
            return df_copy

        except Exception as e:
            error_msg = f"Error during data type optimization: {e}"
            self.logger.error(error_msg)
            raise MemoryOptimizationError(error_msg)
        
    def _optimize_object_column(self, series: pd.Series):
        if series.nunique() / len(series) < 0.05:
            return series.astype('category')
        try:
            numeric_series = pd.to_numeric(series, errors='coerce')
            if numeric_series.notna().sum()/len(series) >0.8:
                return numeric_series
        except:
            pass

        try:
            datetime_series = pd.to_datetime(series, errors='coerce')
            if datetime_series.notna().sum()/len(series) > 0.8:
                return datetime_series
        except:
            pass

        return series
    
    def validate_data_quality(self, df, validation_rules):
        validation_results = {
            'passed': True,
            'errors': [],
            'warnings': [],
            'summary': {}
        }
        try:
            if 'min_passenger_count' in validation_rules:
                min_passengers = validation_rules['min_passenger_count']
                invalid_passengers = df[df['passenger_count'] < min_passengers]
                if len(invalid_passengers) > 0:
                    validation_results['warnings'].append(f'Found {len(invalid_passengers)} trips with passenger count < {min_passengers}')
            
            if 'max_passenger_count' in validation_rules:
                max_passengers = validation_rules['max_passenger_count']
                invalid_passengers = df[df['passenger_count'] > max_passengers]
                if len(invalid_passengers) > 0:
                    validation_results['warnings'].append(
                        f"Found {len(invalid_passengers)} trips with passenger_count > {max_passengers}"
                    )

            if 'min_trip_distance' in validation_rules:
                min_distance = validation_rules['min_trip_distance']
                invalid_distance = df[df['trip_distance'] < min_distance]
                if len(invalid_distance) > 0:
                    validation_results['warnings'].append(
                        f"Found {len(invalid_distance)} trips with distance < {min_distance}"
                    )

            if 'max_trip_distance' in validation_rules:
                max_distance = validation_rules['max_trip_distance']
                invalid_distance = df[df['trip_distance'] > max_distance]
                if len(invalid_distance) > 0:
                    validation_results['warnings'].append(
                        f"Found {len(invalid_distance)} trips with distance > {max_distance}"
                    )
            
            if 'min_fare_amount' in validation_rules:
                min_fare = validation_rules['min_fare_amount']
                invalid_fare = df[df['fare_amount'] < min_fare]
                if len(invalid_fare) > 0:
                    validation_results['warnings'].append(
                        f"Found {len(invalid_fare)} trips with fare < {min_fare}"
                    )
            
            critical_columns = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
            for col in critical_columns:
                if col in df.columns:
                    null_count = df[col].isnull().sum()
                    if null_count > 0:
                        validation_results['warnings'].append(
                            f"Column {col} has {null_count} null values"
                        )

            validation_results['summary'] = {
                'total_rows': len(df),
                'total_columns': len(df.columns),
                'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
                'null_counts': df.isnull().sum().to_dict()
            }

        except Exception as e:
            validation_results['passed'] = False
            validation_results['errors'].append(f'Validation error: {e}')
            self.logger.error(f'Error during data validation: {e}')

        return validation_results

        

            




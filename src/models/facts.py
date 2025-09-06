import pandas as pd
from typing import Dict
from ..utils.exceptions import FactCreationError

class FactCreator:
    def __init__(self):
        pass

    def create_fact_trips(self, df, dimensions: Dict[str, pd.DataFrame]):
        print(f'Executing function {self.create_fact_trips.__name__}.....')
        try:
            fact_trips = pd.DataFrame()
            fact_trips['trip_id'] = df.index + 1
            fact_trips = self._add_foreign_keys(fact_trips, df, dimensions)
            fact_trips = self._add_measures(fact_trips, df)
            fact_trips = self._add_calculated_fields(fact_trips, df)
            fact_trips = self._add_degenerate_dimensions(fact_trips, df)
        
        except Exception as e:
            error_msg = f"Error creating fact table: {e}"
            print(error_msg)
            raise FactCreationError(error_msg)
        
    def _add_foreign_keys(self, fact_trips, df, dimensions):
        try:
            if 'dim_vendor' in dimensions:
                fact_trips['dim_vendor_key'] = df['VendorID'].map(
                    dimensions['dim_vendor'].set_index('VendorID')['dim_vendor_key']
                )

            if 'dim_datetime' in dimensions:
                fact_trips['pickup_datetime_key'] = df['tpep_pickup_datetime'].map(
                    dimensions['dim_datetime'].set_index('full_datetime')['dim_datetime_key']
                )
                fact_trips['dropoff_datetime_key'] = df['tpep_dropoff_datetime'].map(
                    dimensions['dim_datetime'].set_index('full_datetime')['dim_datetime_key']
                )

            if 'dim_pickup_location' in dimensions:
                fact_trips['dim_pickup_location_key'] = df[['pickup_latitude', 'pickup_longitude']].apply(tuple, axis=1).map(dimensions['dim_pickup_location'].set_index(['pickup_latitude', 'pickup_longitude'])['dim_pickup_location_key'])

            if 'dim_dropoff_location' in dimensions:
                fact_trips['dim_dropoff_location_key'] = df[['dropoff_latitude', 'dropoff_longitude']].apply(
                    tuple, axis=1
                ).map(
                    dimensions['dim_dropoff_location'].set_index(['dropoff_latitude', 'dropoff_longitude'])['dim_dropoff_location_key']
                )
            
            if 'dim_ratecode' in dimensions:
                fact_trips['dim_ratecode_key'] = df['RatecodeID'].map(
                    dimensions['dim_ratecode'].set_index('RatecodeID')['dim_ratecode_key']
                )

            if 'dim_payment_type' in dimensions:
                fact_trips['dim_payment_type_key'] = df['payment_type'].map(
                    dimensions['dim_payment_type'].set_index('payment_type')['dim_payment_type_key']
                )

            return fact_trips
        except Exception as e:
            error_msg = f'Error adding foreign keys: {e}'
            print(error_msg)
            raise FactCreationError(error_msg)

    def _add_measures(self, fact_trips, df):
        try:
            fact_trips['passenger_count'] = df['passenger_count']
            fact_trips['trip_distance'] = df['trip_distance']
            fact_trips['fare_amount'] = df['fare_amount']
            fact_trips['extra'] = df['extra']
            fact_trips['mta_tax'] = df['mta_tax']
            fact_trips['tip_amount'] = df['tip_amount']
            fact_trips['tolls_amount'] = df['tolls_amount']
            fact_trips['improvement_surcharge'] = df['improvement_surcharge']
            fact_trips['total_amount'] = df['total_amount']
            return fact_trips
        except Exception as e:
            error_msg = f"Error adding measures: {e}"
            print(error_msg)
            raise FactCreationError(error_msg)
        
    def _add_calculated_fields(self, fact_trips, df):
        try:
            trip_duration = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds()/60
            fact_trips['trip_duration_minutes'] = round(trip_duration, 2)
            fact_trips['avg_speed_mph'] = round((df['trip_distance']/(trip_duration/60)).fillna(0), 2)
            fact_trips['fare_per_mile'] = round((df['fare_amount']/df['trip_distance']).fillna(0), 2)
            fact_trips['tip_percentage'] = round((df['tip_amount']/df['total_amount'] * 100).fillna(0), 2)
            return fact_trips
        except Exception as e:
            error_msg = f"Error adding calculated fields: {e}"
            print(error_msg)
            raise FactCreationError(error_msg)

    def _add_degenerate_dimensions(self, fact_trips, df):
        try:
            fact_trips['store_and_fwd_flag'] = df['store_and_fwd_flag']
            fact_trips['is_airport_trip'] = df['RateCodeID'].isin([2,3,4])
            fact_trips['is_weekend_trip'] = df['tpep_pickup_datetime'].dt.weekday.isin([5,6])
            fact_trips['is_peak_hour'] = df['tpep_pickup_datetime'].dt.hour.isin([7,8,9,17,18,19])
            return fact_trips
        except Exception as e:
            error_msg = f"Error adding degenerate dimensions: {e}"
            print(error_msg)
            raise FactCreationError(error_msg)
        
    def validate_fact_table(self, fact_trips, dimensions):
        validation_results = {
            'passed': True, 'errors': [], 'warnings': [], 'summary': {}
        }
        try:
            foreign_key_columns = [col for col in fact_trips.columns if col.endswith('_key')]
            for col in foreign_key_columns:
                null_count = fact_trips[col].isnull().sum()
                if null_count > 0:
                    validation_results['warnings'].append(f'Column {col} has {null_count} null values.')
            
            amount_columns = ['fare_amount', 'tip_amount', 'total_amount']
            for col in amount_columns:
                if col in fact_trips.columns:
                    negative_count = (fact_trips[col] < 0).sum()
                    if negative_count > 0:
                        validation_results['warnings'].append(f'Column {col} has {negative_count} negative values')
            
            validation_results['summary'] = {
                'total_rows': len(fact_trips),
                'total_columns': len(fact_trips.columns),
                'memory_usage_mb': fact_trips.memory_usage(deep=True).sum()/1024/1024,
                'foreign_keys': foreign_key_columns,
                'measure_columns': [col for col in fact_trips.columns if not col.endswith('_key')]
            }
        except Exception as e:
            validation_results['passed'] = False
            validation_results['errors'].append(f'Validation Error: {e}')
            print(f'Error during fact table validation: {e}')
        
        return validation_results


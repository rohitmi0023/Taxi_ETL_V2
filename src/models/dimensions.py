import pandas as pd

from ..utils.exceptions import DimensionCreationError
from ..utils.logger import get_logger

class DimensionCreator:
    def __init__(self):
        self.logger = get_logger(__name__)

        # Dimension mappings
        self.vendor_mapping = {
            1: 'Creative Mobile Technologies, LLC',
            2: 'Curb Mobility, LLC',
            6: 'Myle Technologies Inc',
            7: 'Helix'
        }
        
        self.ratecode_mapping = {
            1: 'Standard',
            2: 'JFK',
            3: 'Newark',
            4: 'LaGuardia',
            5: 'Negotiated Fare',
            6: 'Group ride',
            99: 'Unknown'
        }
        
        self.payment_mapping = {
            0: 'Flex Fare trip',
            1: 'Credit Card',
            2: 'Cash',
            3: 'No charge',
            4: 'Dispute',
            5: 'Unknown',
            6: 'Voided_trip'
        }

    def create_all_dimensions(self, df):
        self.logger.debug(f'Executing function {self.create_all_dimensions.__name__}...')
        try:
            dimensions = {}
            
            dimensions['dim_vendor'] = self._create_vendor_dimension(df)
            dimensions['dim_datetime'] = self._create_datetime_dimension(df)
            dimensions['dim_pickup_location'] = self._create_pickup_location_dimension(df)
            dimensions['dim_dropoff_location'] = self._create_dropoff_location_dimension(df)
            dimensions['dim_ratecode'] = self._create_ratecode_dimension(df)
            dimensions['dim_payment_type'] = self._create_payment_type_dimension(df)

            self.logger.info(f'Successfully created {len(dimensions)} dimensions')
            self.logger.debug(f'Completed executing function {self.create_all_dimensions.__name__}')
            return dimensions

        except Exception as e:
            error_msg = f"Error occurred in function {self.create_all_dimensions.__name__}: {e}"
            self.logger.error(error_msg)
            raise

    def _create_vendor_dimension(self, df) -> pd.DataFrame:
        try:
            dim_vendor = df[['VendorID']].drop_duplicates().reset_index(drop=True)
            dim_vendor.reset_index(names='dim_vendor_key', inplace=True)
            dim_vendor['vendor_name'] = dim_vendor['VendorID'].map(self.vendor_mapping)

            self.logger.info(f'Dimension dim_vendor created: {dim_vendor.shape[0]} rows, {dim_vendor.shape[1]} columns')
            return dim_vendor
        
        except Exception as e:
            error_msg = f"Error creating vendor dimension: {e}"
            self.logger.error(error_msg)
            raise DimensionCreationError(error_msg)
        
    def _create_datetime_dimension(self, df):
        try:
            datetime_series = pd.concat([df['tpep_pickup_datetime'], df['tpep_dropoff_datetime']])
            dim_datetime = pd.DateFrame({'full_datetime': datetime_series})
            dim_datetime = dim_datetime.drop_duplicates().reset_index(drop=True)
            dim_datetime.reset_index(names='dim_datetime_key', inplace=True)
            
            # Add datetime attributes
            dim_datetime['hour'] = dim_datetime['full_datetime'].dt.hour
            dim_datetime['date'] = dim_datetime['full_datetime'].dt.date
            dim_datetime['day'] = dim_datetime['full_datetime'].dt.day
            dim_datetime['day_of_week'] = dim_datetime['full_datetime'].dt.day_of_week
            dim_datetime['day_name'] = dim_datetime['full_datetime'].dt.day_name()
            dim_datetime['year'] = dim_datetime['full_datetime'].dt.year
            dim_datetime['month_name'] = dim_datetime['full_datetime'].dt.month_name()
            dim_datetime['weekday'] = dim_datetime['full_datetime'].dt.weekday
            dim_datetime['is_weekend'] = dim_datetime['weekday'].isin([5, 6])
            dim_datetime['quarter'] = dim_datetime['full_datetime'].dt.quarter
            dim_datetime['month'] = dim_datetime['full_datetime'].dt.month

            self.logger.info(f'Dimension dim_datetime created: {dim_datetime.shape[0]} rows, {dim_datetime.shape[1]} columns')

        except Exception as e:
            error_msg = f"Error creating datetime dimension: {e}"
            self.logger.error(error_msg)
            raise DimensionCreationError(error_msg)
        
    def _create_pickup_location_dimension(self, df):
        try:
            dim_pickup_location = df[['pickup_latitude', 'pickup_longitude']].drop_duplicates().reset_index(drop=True)
            dim_pickup_location.reset_index(names='dim_pickup_location_key', inplace=True)
            dim_pickup_location['location_type'] = dim_pickup_location.apply(self._classify_location_type, axis=1)
            self.logger.info((f'Dimension dim_pickup_location created: {dim_pickup_location.shape[0]} rows, {dim_pickup_location.shape[1]} columns'))
            return dim_pickup_location
        except Exception as e:
            error_msg = f"Error creating pickup location dimension: {e}"
            self.logger.error(error_msg)
            raise DimensionCreationError
        
    def _create_dropoff_location_dimension(self, df):
        try:
            dim_dropoff_location = df[['dropoff_latitude', 'dropoff_longitude']].drop_duplicates().reset_index(drop=True)
            dim_dropoff_location.reset_index(names='dim_dropoff_location_key', inplace=True)
            dim_dropoff_location['location_type'] = dim_dropoff_location.apply(
                self._classify_location_type, axis=1
            )
            self.logger.info((f'Dimension dim_dropoff_location created: {dim_dropoff_location.shape[0]} rows, {dim_dropoff_location.shape[1]} columns'))
            return dim_dropoff_location
        except:
            error_msg = f"Error creating dropoff location dimension: {e}"
            self.logger.error(error_msg)
            raise DimensionCreationError(error_msg)
    
    def _create_ratecode_dimension(self, df):
        try:
            dim_ratecode = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
            dim_ratecode.reset_index(names='dim_ratecode_key', inplace=True)
            dim_ratecode['ratecode_description'] = dim_ratecode['RatecodeID'].map(self.ratecode_mapping)
            
            self.logger.info(f'Dimension dim_ratecode created: {dim_ratecode.shape[0]} rows, {dim_ratecode.shape[1]} columns')
            return dim_ratecode
            
        except Exception as e:
            error_msg = f"Error creating ratecode dimension: {e}"
            self.logger.error(error_msg)
            raise DimensionCreationError(error_msg)

    def _create_payment_type_dimension(self, df):
        try:
            dim_payment_type = df[['payment_type']].drop_duplicates().reset_index(drop=True)
            dim_payment_type.reset_index(names='dim_payment_type_key', inplace=True)
            dim_payment_type['payment_type_description'] = dim_payment_type['payment_type'].map(self.payment_mapping)
            
            self.logger.info(f'Dimension dim_payment_type created: {dim_payment_type.shape[0]} rows, {dim_payment_type.shape[1]} columns')
            return dim_payment_type
            
        except Exception as e:
            error_msg = f"Error creating payment type dimension: {e}"
            self.logger.error(error_msg)
            raise DimensionCreationError(error_msg)
    
    def _classify_location_type(self, row):
        try:
            lat, lon = row['pickup_latitude'], row['pickup_longitude']
            if pd.isna(lat) or pd.isna(lon) or lat == 0 or lon == 0
                return 'Unknown'
            if -74.1 <= lon <= -73.7 and 40.5 <= lat <= 40.9:
                if 40.75 <= lat <= 40.8 and -74.0 <= lon <= -73.9:
                    return "Downtown"
                elif 40.6 <= lat <= 40.7 and -73.8 <= lon <= -73.7:
                    return "Airport"
                else:
                    return "Residential"
            else:
                return "Outside_NYC"
    
        except Exception:
            return 'Unknown'
        
    def get_dimension_summary(self, dimensions):
        summary = {}
        for dim_name, dim_df in dimensions.items():
            summary[dim_name] = {
                'rows': len(dim_df),
                'columns': len(dim_df.columns),
                'memory_mb': dim_df.memory_usage(deep=True).sum()/1024/1024,
                'columns_list': dim_df.columns.tolist()
            }

        return summary


        
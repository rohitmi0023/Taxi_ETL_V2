"""
Data reading utilities for Taxi ETL V2 project.
"""
import pandas as pd
from pathlib import Path
from typing import Optional

from ..utils.exceptions import FileOperationError, DataValidationError
from ..utils.logger import get_logger

class DataReader:
    def __init__(self):
        self.logger = get_logger(__name__)

    def read_csv(self, file_path:Path, validate_columns:bool, required_columns: Optional[list]=None):
        try:
            self.logger.debug(f'Executing function {self.read_csv.__name__}...')
            if not file_path.exists():
                raise FileOperationError(f'File {file_path} not found!')
            df = pd.read_csv(file_path)
            self.logger.info(f'CSV file read successfully: {df.shape[0]} rows and {df.shape[1]} columns')
            if validate_columns and required_columns:
                self._validate_columns(df, required_columns)
            return df
        except Exception as e:
            error_msg = f'Error occured during reading file: {e}'
            self.logger.error(error_msg)
            raise FileOperationError(error_msg)
        
    def _validate_columns(self, df, required_columns):
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            error_msg = f'Missing required columns in df: {missing_columns}'
            self.logger.error(error_msg)
            raise DataValidationError
        self.logger.debug(f'Column Validation passed, all required columns present.')

    def get_file_info(self, file_path: Path):
        try:
            if not file_path.exists():
                raise FileOperationError(f'File {file_path} not found!')
            stat = file_path.stat()
            return {
                'exists': True,
                'size_bytes': stat.st_size,
                'size_mb': round(stat.st_size/(1024*1024), 2),
                'modified': pd.Timestamp(stat.st_mtime, unit='s'),
                'path': str(file_path.absolute())
            }
        except Exception as e:
            return {'exists': False, 'error': str(e)}
        
    def read_csv_chunks(self, file_path, chunk_size):
        try:
            return pd.read_csv(file_path, chunksize=chunk_size)
        except Exception as e:
            error_msg = f"Error creating chunked reader for {file_path}: {e}"
            self.logger.error(error_msg)
            raise FileOperationError(error_msg)
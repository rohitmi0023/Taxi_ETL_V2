"""
ETL Orchestrator for Taxi ETL Dashboarding project.
Coordinates the entire ETL pipeline with proper error handling and logging.
"""

from ..config.settings import config

from ..utils.exceptions import ConfigurationError
from ..utils.exceptions import TaxiETLException

from ..utils.logger import LoggerFactory

from ..data.reader import DataReader
from ..data.processor import DataProcessor

from ..models.dimensions import DimensionCreator
from ..models.facts import FactCreator

import time
from pathlib import Path

class ETLOrchestrator:
    def __init__(self, config_path = None):
        if config_path:
            self.config = config_path
        else:
            self.config = config
        
        if not self.config.validate_required_config():
            raise ConfigurationError("Required Configuration is missing!")
        
        log_config = self.config.get_logging_config()
        self.logger = LoggerFactory.create_logger(
            name=__name__,
            log_file=log_config.get('file'),
            level=log_config.get('level', 'INFO')
        )
        self.data_reader = DataReader()
        self.data_processor = DataProcessor()
        self.dimension_creator = DimensionCreator()
        self.fact_creator = FactCreator()

        self.pipeline_state = {
            'start_time': None,
            'end_time': None,
            'status': 'not_started',
            'error': None,
            'summary': {}
        }

    def run_pipeline(self):
        self.logger.info('='*50)
        self.logger.info('Starting the Orchestrator process')
        self.logger.info('='*50)

        self.pipeline_state['start_time'] = time.time()
        self.pipeline_state['status'] = 'running'

        try:
            self.logger.info('Step 1: Extracting data...')
            df = self._extract_data()

        except Exception as e:
            raise TaxiETLException
    
    def _extract_data(self):
        try:
            data_config = self.config.get_data_config()
            input_path = Path(data_config['input_path'])
        

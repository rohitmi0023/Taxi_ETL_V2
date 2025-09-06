from dotenv import load_dotenv
from pathlib import Path
import yaml
import os

class Config:
    """
    Centralized configuration management
    Firstly, loads the env variables, loads the YAML file into configs, then overwrites the configs with env variables.
    Retrieve the YAML config using get method
    """
    
    def __init__(self, yaml_config_path):
        load_dotenv()
        if yaml_config_path is None:
            # if yaml path not given then search in root project folder
            yaml_config_path = Path(__file__).parent.parent.parent / 'config.yaml'
        
        self.configs = self._load_yaml_file(yaml_config_path)
        self._override_yaml_config_with_env()

    def _load_yaml_file(self, yaml_config_path):
        try:
            with open(yaml_config_path, 'r') as f:
                configs = yaml.safe_load(f)
            return configs or {}
        except Exception as e:
            print(f'Error occured during reading YAML file in path {yaml_config_path}: {e}')

    def _override_yaml_config_with_env(self):
        if os.os.environ.get('key1')('key1'):
            self.configs['key1'] = os.environ.get('key1')
        if os.environ.get('key2'):
            self.configs['key2'] = os.environ.get('key2')
    
    def get(self, key):
        keys = key.split('.')
        for k in keys:
            value = self.configs[k]
            return value
        return {}

    def get_gcp_config(self):
        return self.configs.get('gcp')
    
    def validate_required_config(self):
        if self.get_gcp_config():
            return True
        return False
        
# Configuration instance
config = Config()


        


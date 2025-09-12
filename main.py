from src.etl.orchestrator import ETLOrchestrator
from src.utils.logger import get_logger

def main():
    """Main Entry Point for the ETL Process"""
    logger = get_logger(__name__)
    try:
        logger.info("Starting ETL Process")
        etl = ETLOrchestrator()
        results= etl.run_pipeline()
    except Exception as e:
        print(f"Error: {e}")
        return

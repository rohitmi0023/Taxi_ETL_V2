from pathlib import Path
import logging
import logging.handlers
import sys
import structlog

class LoggerFactory:
    @staticmethod
    def create_logger(name, log_file=None, level='DEBUG', max_bytes = 1024*1024*10, backup_count=5) -> logging.logger:
        logger = logging.getLogger(name)
        logger.setLevel(getattr(logging, level.upper()))
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        logger.handlers.clear()
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, level.upper()))
        console_handler.setFormatter(formatter)
        logging.addHandler(console_handler)
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            file_handler = logging.handlers.RotatingFileHandler(
                log_file, maxBytes=max_bytes, backupCount=backup_count
            )
            file_handler.setLevel(getattr(logging, level.upper()))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        return logger
    
    @staticmethod
    def create_structured_logger(name, log_file=None, level='DEBUG'):
        structlog.configure(
            processors=[
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.UnicodeDecoder(),
                structlog.processors.JSONRenderer()
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )
        
        # Create base logger
        base_logger = LoggerFactory.create_logger(name, log_file, level)
        
        return structlog.get_logger(base_logger.name)
    

def get_logger(name):
    return logging.getLogger(name)

def get_structured_logger(name):
    return structlog.get_logger(name)


class TaxiETLException(Exception):
    def __init__(self, message, error_code=None):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class FileOperationError(TaxiETLException):
    pass

class FileExistsError(TaxiETLException):
    pass

class DataValidationError(TaxiETLException):
    def __init__(self, message, validation_errors=None):
        self.validation_errors = validation_errors or []
        super().__init__(message, 'DATA_VALIDATION_ERROR')

class MemoryOptimizationError(TaxiETLException):
    pass

class DimensionCreationError(TaxiETLException):
    pass


class FactCreationError(TaxiETLException):
    pass

class ConfigurationError(TaxiETLException):
    pass
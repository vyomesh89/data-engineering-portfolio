import json
import logging
from jsonschema import validate, ValidationError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataValidator:
    """Comprehensive data validation framework for ETL pipelines"""
    
    def __init__(self, schema=None):
        """Initialize validator with JSON schema"""
        self.schema = schema
        self.processed_ids = set()
    
    def validate_schema(self, data):
        """Validate data against JSON schema"""
        if not self.schema:
            logger.warning("No schema provided for validation")
            return True, None
        
        try:
            validate(instance=data, schema=self.schema)
            logger.debug(f"Schema validation passed for record: {data.get('id', 'unknown')}")
            return True, None
        except ValidationError as e:
            error_msg = f"Schema validation failed: {e.message}"
            logger.error(error_msg)
            return False, error_msg
    
    def validate_business_rules(self, data):
        """Validate business-specific rules"""
        errors = []
        
        if not data.get("id"):
            errors.append("ID is required and cannot be empty")
        
        if not data.get("email"):
            errors.append("Email is required")
        
        if data.get("email") and "@" not in str(data.get("email", "")):
            errors.append("Invalid email format")
        
        if data.get("amount") and float(data.get("amount", 0)) < 0:
            errors.append("Amount cannot be negative")
        
        if errors:
            error_msg = f"Business rules validation failed: {', '.join(errors)}"
            logger.error(error_msg)
            return False, error_msg
        
        logger.debug(f"Business rules validation passed for record: {data.get('id')}")
        return True, None
    
    def detect_duplicates(self, data):
        """Detect duplicate records based on ID"""
        record_id = data.get("id")
        
        if record_id in self.processed_ids:
            error_msg = f"Duplicate record detected: {record_id}"
            logger.error(error_msg)
            return False, error_msg
        
        self.processed_ids.add(record_id)
        logger.debug(f"Duplicate check passed for record: {record_id}")
        return True, None
    
    def validate_data_quality(self, data):
        """Comprehensive data quality validation"""
        logger.info(f"Starting validation for record: {data.get('id', 'unknown')}")
        
        is_valid, error = self.validate_schema(data)
        if not is_valid:
            return False, error
        
        is_valid, error = self.validate_business_rules(data)
        if not is_valid:
            return False, error
        
        is_valid, error = self.detect_duplicates(data)
        if not is_valid:
            return False, error
        
        logger.info(f"All validations passed for record: {data.get('id')}")
        return True, None

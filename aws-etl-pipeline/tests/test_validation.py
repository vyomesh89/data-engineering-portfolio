import pytest
from aws_etl_pipeline.src.validation import validate_data_quality, validate_business_rules

class TestValidation:
    
    def test_valid_data(self):
        """Test validation with valid data"""
        data = {
            "id": "123",
            "name": "John Doe",
            "email": "john@example.com",
            "amount": 100.50
        }
        is_valid, error = validate_data_quality(data)
        assert is_valid == True
    
    def test_negative_amount(self):
        """Test validation with negative amount"""
        data = {
            "id": "123",
            "name": "John Doe",
            "email": "john@example.com",
            "amount": -50
        }
        is_valid, error = validate_business_rules(data)
        assert is_valid == False

if __name__ == "__main__":
    pytest.main([__file__, "-v"])

class SyntheticDataClientError(Exception):
    """Base exception for all client errors"""
    pass

class APIError(SyntheticDataClientError):
    """Raised when the API returns an error response"""
    def __init__(self, message, status_code=None, response=None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response

class ValidationError(SyntheticDataClientError):
    """Raised when input validation fails"""
    pass

class AuthenticationError(SyntheticDataClientError):
    """Raised when authentication fails"""
    pass 
"""Input validation models using Pydantic"""
from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import date


class CreateSOPRequest(BaseModel):
    """Validation for creating a new SOP"""
    title: str = Field(..., min_length=1, max_length=500)
    category: str = Field(..., min_length=1, max_length=100)
    content: str = Field(..., min_length=10)
    
    @validator('title', 'category')
    def strip_whitespace(cls, v):
        if not v or not v.strip():
            raise ValueError('Field cannot be empty or whitespace')
        return v.strip()


class UpdateSOPRequest(BaseModel):
    """Validation for updating an SOP"""
    title: str = Field(..., min_length=1, max_length=500)
    category: str = Field(..., min_length=1, max_length=100)
    content: str = Field(..., min_length=10)
    
    @validator('title', 'category')
    def strip_whitespace(cls, v):
        if not v or not v.strip():
            raise ValueError('Field cannot be empty or whitespace')
        return v.strip()


class CreateStaffRequest(BaseModel):
    """Validation for creating staff member"""
    name: str = Field(..., min_length=1, max_length=200)
    staff_type: Optional[str] = Field(None, max_length=50)
    role: Optional[str] = Field(None, max_length=100)
    department: Optional[str] = Field(None, max_length=100)
    supervisor: Optional[str] = Field(None, max_length=200)
    hire_date: Optional[str] = None
    
    @validator('name')
    def name_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('Name cannot be empty')
        return v.strip()
    
    @validator('hire_date')
    def validate_date(cls, v):
        if v:
            try:
                date.fromisoformat(v)
            except ValueError:
                raise ValueError('Invalid date format. Use YYYY-MM-DD')
        return v


class CreateUserRequest(BaseModel):
    """Validation for creating a user account"""
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=8)
    role: str = Field(..., regex="^(admin|hr_manager|staff)$")
    staff_id: Optional[int] = None
    
    @validator('username')
    def username_alphanumeric(cls, v):
        v = v.strip()
        if not v.replace('_', '').replace('-', '').isalnum():
            raise ValueError('Username must be alphanumeric (hyphens and underscores allowed)')
        return v.lower()
    
    @validator('password')
    def password_strength(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters')
        if not any(c.isdigit() for c in v):
            raise ValueError('Password must contain at least one number')
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain at least one uppercase letter')
        return v


class AcknowledgmentRequest(BaseModel):
    """Validation for SOP acknowledgment"""
    signature: str = Field(..., min_length=1, max_length=200)
    read_seconds: int = Field(..., ge=0)
    
    @validator('signature')
    def signature_not_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('Signature cannot be empty')
        return v.strip()


class PasswordChangeRequest(BaseModel):
    """Validation for password changes"""
    current_password: str = Field(..., min_length=1)
    new_password: str = Field(..., min_length=8)
    confirm_password: str = Field(..., min_length=8)
    
    @validator('confirm_password')
    def passwords_match(cls, v, values):
        if 'new_password' in values and v != values['new_password']:
            raise ValueError('Passwords do not match')
        return v
    
    @validator('new_password')
    def password_strength(cls, v):
        if len(v) < 8:
            raise ValueError('Password must be at least 8 characters')
        if not any(c.isdigit() for c in v):
            raise ValueError('Password must contain at least one number')
        if not any(c.isupper() for c in v):
            raise ValueError('Password must contain at least one uppercase letter')
        return v


def validate_form_data(model_class: type[BaseModel], **form_data) -> tuple[bool, Optional[BaseModel], Optional[str]]:
    """
    Validate form data against a Pydantic model
    
    Returns:
        (success, validated_data, error_message)
    """
    try:
        validated = model_class(**form_data)
        return True, validated, None
    except Exception as e:
        error_msg = str(e)
        # Clean up Pydantic error messages
        if "validation error" in error_msg.lower():
            # Extract the actual error message
            lines = error_msg.split('\n')
            for line in lines:
                if '->' in line or 'Field required' in line:
                    continue
                if line.strip() and not line.startswith('  '):
                    error_msg = line.strip()
                    break
        return False, None, error_msg

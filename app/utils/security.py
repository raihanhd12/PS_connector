"""
Security utilities for encryption and decryption
"""
import base64
import json
from typing import Dict, Any, Union
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from app.config import settings


def get_encryption_key() -> bytes:
    """
    Get encryption key for sensitive data
    
    Returns:
        bytes: Encryption key for Fernet
    """
    # Use a fixed salt for consistency
    salt = b'ps_spreadsheet_connector'
    
    # Derive key from secret key
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=100000,
    )
    
    # Create URL-safe base64 encoded key
    key = base64.urlsafe_b64encode(kdf.derive(settings.SECRET_KEY.encode()))
    
    return key


def encrypt_data(data: Union[str, Dict[str, Any]]) -> str:
    """
    Encrypt sensitive data
    
    Args:
        data: Data to encrypt (string or dict)
        
    Returns:
        str: Encrypted data as string
    """
    # Convert dictionary to JSON string if needed
    if isinstance(data, dict):
        data = json.dumps(data)
    
    # Get encryption key
    key = get_encryption_key()
    fernet = Fernet(key)
    
    # Encrypt data
    encrypted_data = fernet.encrypt(data.encode())
    
    return encrypted_data.decode()


def decrypt_data(encrypted_data: str) -> Union[str, Dict[str, Any]]:
    """
    Decrypt sensitive data
    
    Args:
        encrypted_data: Encrypted data as string
        
    Returns:
        Union[str, Dict[str, Any]]: Decrypted data (string or dict)
        
    Raises:
        ValueError: If decryption fails
    """
    try:
        # Get encryption key
        key = get_encryption_key()
        fernet = Fernet(key)
        
        # Decrypt data
        decrypted_data = fernet.decrypt(encrypted_data.encode()).decode()
        
        # Try to parse as JSON
        try:
            return json.loads(decrypted_data)
        except json.JSONDecodeError:
            # Return as string if not valid JSON
            return decrypted_data
            
    except Exception as e:
        raise ValueError(f"Failed to decrypt data: {str(e)}")
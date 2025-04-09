import base64

from cryptography.fernet import Fernet
from dotenv import dotenv_values

# Membaca isi file .env
config = dotenv_values(".env")

SECRET_KEY = config.get("SECRET_KEY", "")
ENCRYPT_CONFIG = config.get("ENCRYPT_CONFIG", "false").lower() == "true"

def get_encryption_key():
    """
    Get or generate an encryption key based on the SECRET_KEY
    """
    # Use the first 32 bytes of SECRET_KEY (padded if necessary)
    key_data = SECRET_KEY.encode().ljust(32, b'\0')[:32]
    # Convert to Fernet compatible key (URL-safe base64-encoded 32-byte key)
    fernet_key = base64.urlsafe_b64encode(key_data)
    return fernet_key


def encrypt_data(data):
    """
    Encrypt sensitive data

    Args:
        data (dict): Data dictionary to encrypt

    Returns:
        dict: Dictionary with encrypted sensitive fields
    """
    if not ENCRYPT_CONFIG:
        return data

    # Create a copy of the data
    encrypted_data = data.copy()

    # Encrypt sensitive fields based on connector type
    if 'password' in encrypted_data:
        encrypted_data['password'] = _encrypt_value(encrypted_data['password'])

    # Handle different connector types
    if 'access_token' in encrypted_data:
        encrypted_data['access_token'] = _encrypt_value(encrypted_data['access_token'])

    if 'secret_key' in encrypted_data:
        encrypted_data['secret_key'] = _encrypt_value(encrypted_data['secret_key'])

    if 'api_key' in encrypted_data:
        encrypted_data['api_key'] = _encrypt_value(encrypted_data['api_key'])

    return encrypted_data


def decrypt_data(data):
    """
    Decrypt sensitive data

    Args:
        data (dict): Data dictionary with encrypted fields

    Returns:
        dict: Dictionary with decrypted sensitive fields
    """
    if not ENCRYPT_CONFIG:
        return data

    # Create a copy of the data
    decrypted_data = data.copy()

    # Decrypt sensitive fields
    if 'password' in decrypted_data:
        decrypted_data['password'] = _decrypt_value(decrypted_data['password'])

    # Handle different connector types
    if 'access_token' in decrypted_data:
        decrypted_data['access_token'] = _decrypt_value(decrypted_data['access_token'])

    if 'secret_key' in decrypted_data:
        decrypted_data['secret_key'] = _decrypt_value(decrypted_data['secret_key'])

    if 'api_key' in decrypted_data:
        decrypted_data['api_key'] = _decrypt_value(decrypted_data['api_key'])

    return decrypted_data


def _encrypt_value(value):
    """
    Encrypt a single value
    """
    if not value:
        return value

    fernet = Fernet(get_encryption_key())
    encrypted = fernet.encrypt(value.encode()).decode()
    return f"ENCRYPTED:{encrypted}"


def _decrypt_value(value):
    """
    Decrypt a single value
    """
    if not value or not value.startswith("ENCRYPTED:"):
        return value

    fernet = Fernet(get_encryption_key())
    encrypted_data = value[10:]  # Remove "ENCRYPTED:" prefix
    decrypted = fernet.decrypt(encrypted_data.encode()).decode()
    return decrypted
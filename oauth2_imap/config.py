"""Configuration definition."""
import ast
import os

ENV_DEV = "development"
ENV_PROD = "production"

class Config:
    """App configuration."""
    TOKEN_CACHE_FILE = "token_cache.bin"
    # use common is the app targeting also external users
    AZURE_AUTHORITY = "https://login.microsoftonline.com/cern.ch"
    AZURE_CLIENT_ID = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    AZURE_SCOPE = [
        "https://outlook.office.com/IMAP.AccessAsUser.All",
        "https://outlook.office.com/POP.AccessAsUser.All",
        "https://outlook.office.com/SMTP.Send",
    ]
    IMAP_SERVER = "outlook.office365.com"
    POP_SERVER = "outlook.office365.com"
    SMTP_SERVER = "outlook.office365.com"

class DevelopmentConfig(Config):
    """Development configuration overrides."""
    ENV = os.getenv("ENV", ENV_DEV)
    DEBUG = ast.literal_eval(os.getenv("DEBUG", "True"))

class ProductionConfig(Config):
    """Production configuration overrides."""
    ENV = os.getenv("ENV", ENV_PROD)
    DEBUG = ast.literal_eval(os.getenv("DEBUG", "False"))

def load_config():
    """Load the configuration."""
    config_options = {ENV_DEV: DevelopmentConfig, ENV_PROD: ProductionConfig}
    environment = os.getenv("ENV", ENV_DEV)

    return config_options[environment]

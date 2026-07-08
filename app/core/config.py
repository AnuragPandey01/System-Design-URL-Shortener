from pydantic_settings import BaseSettings
from pydantic_settings.main import SettingsConfigDict

class Config(BaseSettings):
    
    model_config = SettingsConfigDict(
        env_prefix='shortener_',
        env_file='.env',
        env_file_encoding='utf-8' # Optional but good practice
    )
    
    database_username: str
    database_password: str
    database_hostname: str
    database_port: int
    database_name: str


config = Config()
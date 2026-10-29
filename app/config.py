from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    redis_host: str = "127.0.0.1"
    redis_port: int = 6379
    redis_db: int = 0
    # No password on local Firewalla Redis in most setups
    redis_password: str | None = None

    host: str = "0.0.0.0"
    port: int = 5140

    # Consider a device online if seen within this many seconds
    online_threshold_seconds: int = 300  # 5 minutes

    class Config:
        env_prefix = "FWAPI_"


settings = Settings()

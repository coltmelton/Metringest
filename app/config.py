from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "Metringest"
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "metric-events"
    worker_batch_size: int = 100
    worker_batch_wait_ms: int = 250
    redis_url: str = "redis://localhost:6379/0"
    database_url: str = "postgresql://metringest:metringest@localhost:5432/metringest"


settings = Settings()

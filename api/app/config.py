from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "kafka:9092"
    raw_topic: str = "raw-telemetry"
    database_url: str = "postgresql://telemetry:telemetry@postgres:5432/telemetry"
    service_name: str = "telemetry-api"

    model_config = SettingsConfigDict(env_prefix="", env_file=".env", extra="ignore")


settings = Settings()

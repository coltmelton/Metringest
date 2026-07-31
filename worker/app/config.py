from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    kafka_bootstrap_servers: str = "kafka:9092"
    raw_topic: str = "raw-telemetry"
    validated_topic: str = "validated-telemetry"
    dead_letter_topic: str = "dead-letter-telemetry"
    consumer_group: str = "telemetry-worker"
    database_url: str = "postgresql://telemetry:telemetry@postgres:5432/telemetry"
    redis_url: str = "redis://redis:6379/0"
    batch_size: int = 100
    batch_wait_ms: int = 250
    outbox_batch_size: int = 100
    outbox_poll_ms: int = 250
    metrics_port: int = 9101
    service_name: str = "telemetry-worker"

    model_config = SettingsConfigDict(env_prefix="", env_file=".env", extra="ignore")


settings = Settings()

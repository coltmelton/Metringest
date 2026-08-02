from hashlib import sha256

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEVELOPMENT_API_KEY_HASH = sha256(b"development-key").hexdigest()
BENCHMARK_API_KEY_HASH = sha256(b"benchmark-key").hexdigest()


class Settings(BaseSettings):
    app_environment: str = "development"
    kafka_bootstrap_servers: str = "kafka:9092"
    raw_topic: str = "raw-telemetry"
    database_url: str = "postgresql://telemetry:telemetry@postgres:5432/telemetry"
    redis_url: str = "redis://redis:6379/0"
    schema_registry_url: str = "http://schema-registry:8081"
    schema_subject: str = "raw-telemetry-value"
    schema_directory: str = "/schemas"
    api_key_hashes: str = f"{DEVELOPMENT_API_KEY_HASH},{BENCHMARK_API_KEY_HASH}"
    rate_limit_events: int = 1000
    rate_limit_window_seconds: int = 60
    max_request_bytes: int = 1_048_576
    max_batch_events: int = 500
    kafka_publish_timeout_seconds: float = 5.0
    service_name: str = "telemetry-api"

    @property
    def allowed_api_key_hashes(self) -> tuple[str, ...]:
        return tuple(value.strip().lower() for value in self.api_key_hashes.split(",") if value)

    @model_validator(mode="after")
    def reject_insecure_production_key(self):
        if not self.allowed_api_key_hashes:
            raise ValueError("API_KEY_HASHES must contain at least one SHA-256 hash")
        if (
            self.app_environment.lower() != "development"
            and {DEVELOPMENT_API_KEY_HASH, BENCHMARK_API_KEY_HASH}
            & set(self.allowed_api_key_hashes)
        ):
            raise ValueError("the development API key is forbidden outside development")
        return self

    model_config = SettingsConfigDict(env_prefix="", env_file=".env", extra="ignore")


settings = Settings()

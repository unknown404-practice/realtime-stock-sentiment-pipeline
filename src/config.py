"""
Strongly typed configuration management.
Resolves configuration from environment variables and .streamlit/secrets.toml
using standard tomllib/toml parsers. Gracefully supports both Pydantic BaseSettings
and pure Python dataclass fallback when pydantic is not present.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Standard TOML parsing: Python 3.11+ has tomllib in stdlib; fallback to toml
try:
    import tomllib
except ImportError:
    try:
        import toml as tomllib  # type: ignore
    except ImportError:
        tomllib = None  # type: ignore


def load_streamlit_secrets() -> dict:
    """Parse .streamlit/secrets.toml using standard TOML parser."""
    base_dir = Path(__file__).resolve().parent.parent
    secrets_path = base_dir / ".streamlit" / "secrets.toml"
    if not secrets_path.exists():
        return {}

    try:
        if tomllib is not None:
            # Standard tomllib in Python 3.11+ requires binary mode
            if hasattr(tomllib, "load"):
                try:
                    with open(secrets_path, "rb") as f:
                        return tomllib.load(f)
                except (TypeError, ValueError):
                    with open(secrets_path, "r", encoding="utf-8") as f:
                        return tomllib.load(f)
            elif hasattr(tomllib, "loads"):
                with open(secrets_path, "r", encoding="utf-8") as f:
                    return tomllib.loads(f.read())
    except Exception:
        pass
    return {}


_SECRETS = load_streamlit_secrets()

try:
    from pydantic import Field
    from pydantic_settings import BaseSettings

    class Settings(BaseSettings):
        # Kafka Streaming Configuration
        kafka_bootstrap_servers: str = Field(
            default=_SECRETS.get("KAFKA_BOOTSTRAP_SERVERS", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")),
            alias="KAFKA_BOOTSTRAP_SERVERS"
        )
        kafka_topic: str = Field(
            default=_SECRETS.get("KAFKA_TOPIC", os.getenv("KAFKA_TOPIC", "stock-news")),
            alias="KAFKA_TOPIC"
        )
        kafka_group_id: str = Field(
            default=_SECRETS.get("KAFKA_GROUP_ID", os.getenv("KAFKA_GROUP_ID", "finbert-sentiment-consumer-group")),
            alias="KAFKA_GROUP_ID"
        )
        kafka_num_partitions: int = Field(
            default=int(os.getenv("KAFKA_NUM_PARTITIONS", "4")),
            alias="KAFKA_NUM_PARTITIONS"
        )
        publish_interval: float = Field(
            default=float(os.getenv("PUBLISH_INTERVAL_SECONDS", "0.8")),
            alias="PUBLISH_INTERVAL_SECONDS"
        )

        # MongoDB Storage Configuration
        local_mongo_uri: str = Field(
            default=_SECRETS.get("LOCAL_MONGO_URI", os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017/")),
            alias="LOCAL_MONGO_URI"
        )
        mongo_uri: Optional[str] = Field(
            default=_SECRETS.get("MONGO_URI", os.getenv("MONGO_URI", None)),
            alias="MONGO_URI"
        )
        db_name: str = Field(
            default=_SECRETS.get("DB_NAME", os.getenv("DB_NAME", "StockDB")),
            alias="DB_NAME"
        )
        collection_name: str = Field(
            default=_SECRETS.get("COLLECTION_NAME", os.getenv("COLLECTION_NAME", "news_sentiment")),
            alias="COLLECTION_NAME"
        )
        logs_collection_name: str = Field(
            default=_SECRETS.get("LOGS_COLLECTION_NAME", os.getenv("LOGS_COLLECTION_NAME", "pipeline_logs")),
            alias="LOGS_COLLECTION_NAME"
        )

        # Market News Ingestion Configuration
        finnhub_api_key: Optional[str] = Field(
            default=_SECRETS.get("FINNHUB_API_KEY", os.getenv("FINNHUB_API_KEY", None)),
            alias="FINNHUB_API_KEY"
        )

        # Machine Learning / FinBERT Configuration
        model_name: str = Field(
            default=_SECRETS.get("MODEL_NAME", os.getenv("MODEL_NAME", "ProsusAI/finbert")),
            alias="MODEL_NAME"
        )
        batch_size: int = Field(
            default=int(os.getenv("BATCH_SIZE", "16")),
            alias="BATCH_SIZE"
        )

        class Config:
            env_file = ".env"
            extra = "ignore"

except ImportError:
    @dataclass
    class Settings:  # type: ignore
        kafka_bootstrap_servers: str = _SECRETS.get("KAFKA_BOOTSTRAP_SERVERS", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
        kafka_topic: str = _SECRETS.get("KAFKA_TOPIC", os.getenv("KAFKA_TOPIC", "stock-news"))
        kafka_group_id: str = _SECRETS.get("KAFKA_GROUP_ID", os.getenv("KAFKA_GROUP_ID", "finbert-sentiment-consumer-group"))
        kafka_num_partitions: int = int(os.getenv("KAFKA_NUM_PARTITIONS", "4"))
        publish_interval: float = float(os.getenv("PUBLISH_INTERVAL_SECONDS", "0.8"))

        local_mongo_uri: str = _SECRETS.get("LOCAL_MONGO_URI", os.getenv("LOCAL_MONGO_URI", "mongodb://localhost:27017/"))
        mongo_uri: Optional[str] = _SECRETS.get("MONGO_URI", os.getenv("MONGO_URI", None))
        db_name: str = _SECRETS.get("DB_NAME", os.getenv("DB_NAME", "StockDB"))
        collection_name: str = _SECRETS.get("COLLECTION_NAME", os.getenv("COLLECTION_NAME", "news_sentiment"))
        logs_collection_name: str = _SECRETS.get("LOGS_COLLECTION_NAME", os.getenv("LOGS_COLLECTION_NAME", "pipeline_logs"))

        finnhub_api_key: Optional[str] = _SECRETS.get("FINNHUB_API_KEY", os.getenv("FINNHUB_API_KEY", None))

        model_name: str = _SECRETS.get("MODEL_NAME", os.getenv("MODEL_NAME", "ProsusAI/finbert"))
        batch_size: int = int(os.getenv("BATCH_SIZE", "16"))


_settings_instance: Optional[Settings] = None


def get_settings() -> Settings:
    global _settings_instance
    if _settings_instance is None:
        _settings_instance = Settings()
    return _settings_instance

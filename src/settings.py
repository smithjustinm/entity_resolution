from pathlib import Path

from pydantic import BaseSettings


class AppSettings(BaseSettings):
    """Configuration storage for entity mapper."""

    #: Name and version of the application.
    APPLICATION: str = "entity mapper"

    #: The logging level of the application
    LOG_LEVEL: str = "INFO"

    TEST_FRACTION: float = 0.1

    abs_path = Path(__file__).resolve().parent.parent / "data"

    OUTPUT_FILE = abs_path / "output" / "entity_matching_output.csv"

    LEFT_FILE = abs_path / "output" / "df_a.csv"

    RIGHT_FILE = abs_path / "output" / "df_b.csv"

    SETTINGS_FILE = abs_path / "entity_matching_learned_settings"

    TRAINING_FILE = abs_path / "entity_matching_training.json"

    # Preprocessed data files
    A_COMPANY = abs_path / "a__company.csv"

    A_GEO = abs_path / "a__geo.csv"

    B_ADDRESS = abs_path / "b__address.csv"

    B_COMPANY = abs_path / "b__company.csv"

    # Output files from merge step
    DF_A = abs_path / "output" / "df_a.csv"

    DF_B = abs_path / "output" / "df_b.csv"

    FIELDS = [
        {"field": "id", "type": "String"},
        {"field": "name", "type": "String"},
        {"field": "postal", "type": "String", "has missing": True},
        {"field": "country", "type": "ShortString", "has missing": True},
    ]

    LEFT_DATA_PROCESSED = abs_path / "output" / "left_data_processed.csv"
    RIGHT_DATA_PROCESSED = abs_path / "output" / "right_data_processed.csv"

    def setup_logging(self):
        """
        Configure the logging.
        """
        import logging.config

        import structlog

        from src.logging_setup import STD_LIB_PROCESSORS, STRUCTLOG_PROCESSORS

        log_level = self.LOG_LEVEL.upper()

        structlog.configure(
            processors=STRUCTLOG_PROCESSORS,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
        )

        logging.config.dictConfig(
            {
                "version": 1,
                "disable_existing_loggers": False,
                "formatters": {
                    "plain": {
                        "()": structlog.stdlib.ProcessorFormatter,
                        "processor": structlog.processors.JSONRenderer(),
                        "foreign_pre_chain": STD_LIB_PROCESSORS,
                    },
                },
                "handlers": {
                    "default": {"class": "logging.StreamHandler", "formatter": "plain"}
                },
                "root": {
                    "handlers": ["default"],
                    "level": log_level,
                    "propagate": True,
                },
            }
        )

        return structlog.get_logger()


settings = AppSettings()

import logging
import os
from string import Template

import yaml
from dotenv import load_dotenv
from sqlalchemy import text

from Extractor.api_extractor import APIExtractor
from Extractor.csv_extractor import CSVExtractor
from Extractor.database_connector import DatabaseConnector
from Extractor.json_extractor import JSONExtractor
from Extractor.s3_extractor import PublicS3Extractor

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MainExtractor:
    def __init__(self, config_filename="config.yaml"):
        # ✅ Dynamically find config.yaml relative to this script's directory
        base_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(base_dir, config_filename)

        self.config = self.load_config(config_path)
        self.setup_extractors()

    def load_config(self, config_path):
        with open(config_path, "r") as file:
            config_content = file.read()

        template = Template(config_content)
        config_content = template.safe_substitute(os.environ)

        return yaml.safe_load(config_content)

    def setup_extractors(self):
        self.db_connector = DatabaseConnector(self.config)
        self.json_extractor = JSONExtractor(self.db_connector)
        self.csv_extractor = CSVExtractor(self.db_connector)
        self.s3_extractor = PublicS3Extractor(
            self.config, self.json_extractor, self.csv_extractor, self
        )
        self.api_extractor = APIExtractor(self.config, self.json_extractor)

    def get_table_columns(self, table_name, schema="landing"):
        """Get the actual column names from the database table"""
        table_name = table_name.lower()
        engine = self.db_connector.get_engine()
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text(
                        f"""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = '{schema}'
                        AND table_name = '{table_name}'
                        ORDER BY ordinal_position
                        """
                    )
                )
                columns = [row[0] for row in result]
                logger.info(f"Table {schema}.{table_name} has columns: {columns}")
                return columns
        except Exception as e:
            logger.error(f"Error getting table columns for {table_name}: {str(e)}")
            raise

    def truncate_table(self, table_name, schema="landing"):
        """Truncate the specified table in the given schema"""
        table_name = table_name.lower()
        engine = self.db_connector.get_engine()
        try:
            with engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {schema}.{table_name}"))
                conn.commit()
            logger.info(f"Truncated table {schema}.{table_name}")
        except Exception as e:
            logger.error(f"Error truncating table {table_name}: {str(e)}")
            raise

    def extract_s3_data(self):
        logger.info("Starting S3 data extraction")
        try:
            # Truncate all S3-related tables before extraction
            for s3_key, table_name in self.config["s3"]["files"].items():
                self.truncate_table(table_name, schema="landing")

            self.s3_extractor.extract_all()
            logger.info("S3 data extraction completed successfully")
        except Exception as e:
            logger.error(f"S3 data extraction failed: {str(e)}")
            raise

    def extract_api_data(self):
        logger.info("Starting API data extraction")
        try:
            # Truncate all API-related tables before extraction
            for url, table_name in self.config["api"]["endpoints"].items():
                self.truncate_table(table_name, schema="landing")

            self.api_extractor.extract_all()
            logger.info("API data extraction completed successfully")
        except Exception as e:
            logger.error(f"API data extraction failed: {str(e)}")
            raise

    def extract_all(self):
        logger.info("Starting full data extraction")
        try:
            self.extract_s3_data()
            self.extract_api_data()
            logger.info("Data extraction completed successfully")
        except Exception as e:
            logger.error(f"Data extraction failed: {str(e)}")
            raise


def main():
    try:
        extractor = MainExtractor()
        extractor.extract_all()
    except Exception as e:
        logger.error(f"Main extraction process failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()

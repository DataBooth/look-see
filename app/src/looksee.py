import subprocess
from pathlib import Path

import duckdb
import toml
from loguru import logger


class LookSee:
    def __init__(self, config_path="looksee.toml"):
        # Load configuration from TOML file
        try:
            config_path = Path.cwd() / "app" / config_path
            print(f"config_path: {config_path}")
            with open(config_path, "r") as f:
                self.config = toml.load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file '{config_path}' not found.")
        except Exception as e:
            raise RuntimeError(f"Error loading configuration file '{config_path}': {e}")

        # Initialize DuckDB connection and settings
        self.conn = duckdb.connect(database=":memory:", read_only=False)
        self.table_name = self.config["settings"].get("default_table_name", "dataset")
        logger.add(
            self.config["settings"].get("log_file", "looksee.log"), rotation="500 MB"
        )

        logger.info("LookSee initialized with configuration from looksee.toml.")

    def _get_duckdb_read_function(self, file_type):
        """
        Get the DuckDB read function for a given file type from the configuration.
        """
        read_functions = self.config["read_functions"]
        return read_functions.get(file_type.lower())

    def ingest_data(self, file_path):
        """
        Ingest data into DuckDB from supported file formats and create a table.
        """
        try:
            file_path = str(file_path)
            file_type = Path(file_path).suffix[1:]  # Get file extension without the dot
            read_function = self._get_duckdb_read_function(file_type)

            if not read_function:
                raise ValueError(f"Unsupported file format: {file_type}")

            # Dynamically construct the DuckDB query to read the file
            query = f"CREATE TABLE {self.table_name} AS SELECT * FROM {read_function}('{file_path}')"
            self.conn.execute(query)

            logger.info(f"Data ingested successfully from {file_path}.")
            return True
        except Exception as e:
            logger.error(f"Error ingesting data: {e}")
            return False

    def extract_metadata(self):
        """
        Extract metadata for the dataset.
        """
        try:
            # Query column names, types, and null counts
            query = f"""
                SELECT 
                    column_name,
                    data_type,
                    COUNT(*) AS total_rows,
                    COUNT(*) - COUNT(column_name) AS null_count,
                    COUNT(DISTINCT column_name) AS unique_count
                FROM information_schema.columns 
                WHERE table_name = '{self.table_name}'
                GROUP BY column_name, data_type;
            """
            metadata_df = self.conn.execute(query).fetchdf()

            # Convert metadata to dictionary for display
            self.metadata = metadata_df.to_dict(orient="records")
            logger.info("Metadata extracted successfully.")
        except Exception as e:
            logger.error(f"Error extracting metadata: {e}")

    def column_summary(self, column_name):
        """
        Generate summary statistics for a specific column.
        """
        try:
            query = f"""
                SELECT 
                    MIN({column_name}) AS min_value,
                    MAX({column_name}) AS max_value,
                    AVG({column_name}) AS mean_value,
                    STDDEV({column_name}) AS std_dev,
                    COUNT(DISTINCT {column_name}) AS unique_count,
                    COUNT(*) - COUNT({column_name}) AS null_count
                FROM {self.table_name};
            """
            summary = self.conn.execute(query).fetchdf().to_dict(orient="records")[0]
            return summary
        except Exception as e:
            logger.error(f"Error generating column summary for {column_name}: {e}")
            return {}

    def display_metadata(self):
        """
        Return metadata for display in Streamlit.
        """
        return self.metadata

    def render_and_publish_quarto(self, qmd_file, server_url):
        """
        Render and publish a Quarto .qmd file to Posit Connect.
        :param qmd_file: Path to the Quarto .qmd file.
        :param server_url: URL of the Posit Connect server.
        """
        try:
            # Step 1: Render the .qmd file locally
            render_command = ["quarto", "render", qmd_file]
            logger.info(f"Rendering Quarto document: {' '.join(render_command)}")
            subprocess.run(render_command, check=True)

            # Step 2: Publish the rendered document to Posit Connect
            publish_command = [
                "quarto",
                "publish",
                "connect",
                qmd_file,
                "--server",
                server_url,
            ]
            logger.info(f"Publishing Quarto document: {' '.join(publish_command)}")
            subprocess.run(publish_command, check=True)

            logger.info(
                f"Quarto document '{qmd_file}' published successfully to {server_url}."
            )
        except subprocess.CalledProcessError as e:
            logger.error(f"Error during rendering or publishing: {e}")

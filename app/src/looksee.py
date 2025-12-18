import subprocess
from pathlib import Path

import duckdb
import toml
from loguru import logger
from tabulate import tabulate
from functools import cache


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
            self.config["settings"].get("log_file", "looksee.log"), rotation="2 MB"
        )

        logger.info("LookSee initialized with configuration from looksee.toml.")

    def _get_duckdb_read_function(self, file_type):
        """
        Get the DuckDB read function for a given file type from the configuration.
        """
        read_functions = self.config["read_functions"]
        return read_functions.get(file_type.lower())
    
    @cache
    def ingest_data(self, file_path, uploaded_file_name=None):
        try:
            file_path = str(file_path)
            if uploaded_file_name:
                file_type = Path(uploaded_file_name).suffix[1:]  # Use uploaded file name
            else:
                file_type = Path(file_path).suffix[1:]
            read_function = self._get_duckdb_read_function(file_type)

            if not read_function:
                raise ValueError(f"Unsupported file format: {file_type}")

            self.conn.execute(f"DROP TABLE IF EXISTS {self.table_name}")

            query = f"""
            CREATE TABLE {self.table_name} AS 
            SELECT * FROM {read_function}('{file_path}', 
                                            sample_size=20480, 
                                            all_varchar=false)
            """
            self.conn.execute(query)
            print(query.strip())
            logger.info(f"Data ingested successfully from {file_path}.")

            validation_results = self.validate_column_types()
            if validation_results:
                logger.warning("Column type validation issues found:")
                for result in validation_results:
                    logger.warning(result)

            return True  # Return True if ingestion was successful, regardless of validation results

        except Exception as e:
            logger.error(f"Error ingesting data: {e}")
            return False


    def validate_column_types(self):
        """
        Validate column types for the current table.
        """
        query = f"""
        SELECT column_name, data_type
        FROM duckdb_columns()
        WHERE table_name = '{self.table_name}'
        """
        columns = self.conn.execute(query).fetchall()
        logger.info(f"Columns detected: {columns}")

        validation_results = []
        for col_name, data_type in columns:
            validation_query = f"""
            SELECT COUNT(*) FROM {self.table_name}
            WHERE CAST({col_name} AS {data_type}) IS NULL
            AND {col_name} IS NOT NULL
            """
            invalid_count = self.conn.execute(validation_query).fetchone()[0]
            if invalid_count > 0:
                validation_results.append(
                    f"Column {col_name} has {invalid_count} rows that don't match type {data_type}"
                )

        return validation_results

    def extract_metadata(self):
        """
        Extract metadata for the dataset.
        """
        try:
            # Query column names and data types
            column_info_query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = '{self.table_name}';
            """
            columns_info = self.conn.execute(column_info_query).fetchall()

            metadata = []
            for column_name, data_type in columns_info:
                # Query to get total rows, null count, and unique count for each column
                query = f"""
                    SELECT 
                        (SELECT COUNT(*) FROM {self.table_name}) AS total_rows,
                        COUNT(*) - COUNT("{column_name}") AS null_count,
                        COUNT(DISTINCT "{column_name}") AS unique_count
                    FROM {self.table_name};
                """
                result = self.conn.execute(query).fetchone()
                total_rows, null_count, unique_count = result

                metadata.append({
                    "column_name": column_name,
                    "data_type": data_type,
                    "total_rows": total_rows,
                    "null_count": null_count,
                    "unique_count": unique_count,
                })

            self.metadata = metadata
            logger.info("Metadata extracted successfully.")
        except Exception as e:
            logger.error(f"Error extracting metadata: {e}")
            self.metadata = []  # Set an empty list if extraction fails


    def column_summary(self, column_name):
        """
        Generate summary statistics for a specific column.
        """
        try:
            # Determine the data type of the column
            data_type_query = f"""
                SELECT data_type
                FROM information_schema.columns
                WHERE table_name = '{self.table_name}' AND column_name = '{column_name}';
            """
            data_type = self.conn.execute(data_type_query).fetchone()[0]

            if data_type in ("INTEGER", "BIGINT", "DOUBLE", "DECIMAL"):
                # Summary for numeric columns
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
            elif data_type == "DATE":
                # Summary for date columns (min and max)
                query = f"""
                    SELECT 
                        MIN({column_name}) AS min_value,
                        MAX({column_name}) AS max_value,
                        COUNT(DISTINCT {column_name}) AS unique_count,
                        COUNT(*) - COUNT({column_name}) AS null_count
                    FROM {self.table_name};
                """
            else:
                # Summary for non-numeric columns (unique count and null count)
                query = f"""
                    SELECT 
                        COUNT(DISTINCT {column_name}) AS unique_count,
                        COUNT(*) - COUNT({column_name}) AS null_count
                    FROM {self.table_name};
                """

            summary = self.conn.execute(query).fetchdf().to_dict(orient="records")[0]

            # Add distinct values if unique count is 5 or less
            if summary["unique_count"] <= 5:
                distinct_query = f"""
                    SELECT DISTINCT {column_name}
                    FROM {self.table_name}
                    ORDER BY {column_name}
                    LIMIT 5;
                """
                distinct_values = self.conn.execute(distinct_query).fetchall()
                summary["distinct_values"] = [value[0] for value in distinct_values]

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


def print_metadata(metadata):
    headers = ["Column Name", "Data Type", "Total Rows", "Null Count", "Unique Count"]
    table_data = [
        [
            item["column_name"],
            item["data_type"],
            item["total_rows"],
            item["null_count"],
            item["unique_count"],
        ]
        for item in metadata
    ]
    print("\nMetadata:")
    print(tabulate(table_data, headers=headers, tablefmt="grid"))


def print_column_summary(column_name, summary):
    print(f"\nSummary for column '{column_name}':")
    table_data = [
        [key, value] for key, value in summary.items() if key != "distinct_values"
    ]
    print(tabulate(table_data, headers=["Statistic", "Value"], tablefmt="simple"))

    if "distinct_values" in summary:
        print("\nDistinct values:")
        for value in summary["distinct_values"]:
            print(f"  - {value}")


def main():
    # Load demo datasets from TOML file
    with open(Path.cwd() / "app" / "demo_datasets.toml", "r") as f:
        demo_datasets = toml.load(f)["datasets"]

    # Initialize LookSee
    looksee = LookSee()

    # Process each dataset
    i_dataset = 0
    for dataset_name, dataset_url in demo_datasets.items():
        i_dataset += 1
        print()
        print("-" * 60)
        print(f"Processing dataset: {dataset_name} - {dataset_url}")
        print("-" * 60)

        # Ingest data
        if looksee.ingest_data(dataset_url):
            # Extract and display metadata
            looksee.extract_metadata()
            metadata = looksee.display_metadata()
            if metadata:
                print_metadata(metadata)
            else:
                print("Failed to extract metadata.")

            # Get column names
            columns_query = f"SELECT column_name FROM information_schema.columns WHERE table_name='{looksee.table_name}'"
            columns = [col[0] for col in looksee.conn.execute(columns_query).fetchall()]

            # Display summary for each column
            for column in columns:
                summary = looksee.column_summary(column)
                print_column_summary(column, summary)
        else:
            print(f"Failed to ingest dataset: {dataset_name}")

        logger.info(f"\nProcessed {i_dataset} of {len(demo_datasets.items())} datasets")

if __name__ == "__main__":
    main()

# `look-see`

Data exploration tool backed by DuckDB with publishing to Posit-Connect

---

# LookSee

LookSee is a Python-based data investigation tool designed to help users explore datasets interactively. It leverages DuckDB for backend storage and querying, Streamlit for building an interactive UI, and Quarto for generating polished reports. LookSee is ideal for data analysts and developers who want a lightweight yet powerful tool for data exploration.

---

## Features

- Data Ingestion:
  - Supports multiple file formats (CSV, Parquet, JSON) using DuckDB.
  - Automatically extracts metadata from datasets.
- Column Explorer:
  - Provides summary statistics (e.g., min, max, mean, standard deviation).
  - Displays null counts and unique value counts for each column.
- Interactive Streamlit App:
  - Upload datasets or use demo datasets.
  - Explore metadata and column summaries interactively.
- Quarto Report Integration:
  - Generate polished HTML reports from `.qmd` templates.
  - Publish reports to Posit Connect directly from LookSee.
- Configurable via TOML:
  - Centralised configuration for file type mappings, logging, and settings.

---

## Getting Started

### Prerequisites

1. Python 3.11 or later.
2. Install the following tools:
   - [Quarto CLI](https://quarto.org/docs/get-started/): For rendering `.qmd` files.
   - [Posit Connect CLI (`rsconnect-python`)](https://pypi.org/project/rsconnect-python/): For deploying Streamlit apps or Quarto reports to Posit Connect.

### Installation

1. Clone this repository:
   ```bash
   git clone https://github.com/your-repo/looksee.git
   cd looksee
   ```

2. Install dependencies using `pip` or `uv`:
   ```bash
   pip install -r requirements.txt
   ```

3. Ensure `looksee.toml` is present in the root directory.

---

## Usage

### 1. Interactive Streamlit App

Run the Streamlit app to explore datasets interactively:
```bash
streamlit run streamlit_app.py
```

#### Features in the App:

- Upload your dataset (CSV, Parquet, JSON) or use demo datasets.
- View metadata such as column names, data types, null counts, and unique counts.
- Explore summary statistics for individual columns.

---

### 2. Quarto Report

Generate a polished report from a `.qmd` template:
1. Render the report locally:
   ```bash
   quarto render report.qmd
   ```
2. Publish the report to Posit Connect:
   ```bash
   quarto publish connect report.qmd --server 
   ```

---

### 3. LookSee Class

You can also use LookSee programmatically in your Python projects:

#### Example Usage:

```python
from looksee import LookSee

# Initialise LookSee
looksee = LookSee()

# Ingest data
looksee.ingest_data("data.csv")

# Extract metadata
looksee.extract_metadata()
print(looksee.display_metadata())

# Get column summary
summary = looksee.column_summary("age")
print(summary)
```

---

## Configuration

The `looksee.toml` file contains all configuration options:

```toml
[read_functions]
csv = "read_csv_auto"
parquet = "read_parquet"
json = "read_json_auto"

[settings]
default_table_name = "dataset"
log_file = "looksee.log"
```

- Add support for new file formats by extending the `[read_functions]` section.
- Update logging settings in the `[settings]` section.

---

## Testing

Run tests using `pytest` to validate the functionality of LookSee:

```bash
pytest tests/
```

Ensure you have a sample dataset (e.g., `sample.csv`) in the `tests/` directory for testing purposes.

---

## Deployment

### 1. Deploy Streamlit App to Posit Connect

1. Add your Posit Connect server:
   ```bash
   rsconnect add-server --name posit-server --url https://your-connect-url/
   ```
2. Authenticate with your API key:
   ```bash
   rsconnect login --server posit-server
   ```
3. Deploy the app:
   ```bash
   rsconnect deploy streamlit --entrypoint streamlit_app.py --name looksee-app .
   ```

### 2. Publish Quarto Report to Posit Connect

1. Render and publish the report directly:
   ```bash
   quarto publish connect report.qmd --server https://your-connect-url/
   ```

---

## Project Structure

```
looksee/
├── looksee.py               # Core LookSee class implementation
├── streamlit_app.py         # Streamlit app for interactive exploration
├── report.qmd               # Quarto template for generating reports
├── looksee.toml             # Configuration file (file mappings, settings)
├── requirements.txt         # Python dependencies
├── tests/
│   ├── test_looksee.py      # Pytest tests for LookSee class
│   └── sample.csv           # Sample dataset for testing
└── README.md                # Project documentation (this file)
```

---

## Future Enhancements

1. Add support for additional file formats (e.g., Excel, SQLite).
2. Enable advanced filtering and querying directly in the Streamlit app.
3. Automate deployment workflows using GitHub Actions or similar CI/CD tools.
4. Add more visualisation options (e.g., histograms or sparklines).

---

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork this repository.
2. Create a feature branch (`git checkout -b feature-name`).
3. Commit your changes (`git commit -m "Add feature"`).
4. Push to your branch (`git push origin feature-name`).
5. Open a pull request.

---

## License

This project is licensed under the MIT License.
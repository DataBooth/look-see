import streamlit as st
from src.looksee import LookSee
import toml
from pathlib import Path

def main():
    st.title("Data Investigation Tool")

    # Load LookSee instance
    looksee = LookSee()

    # Sidebar for data selection
    st.sidebar.header("Select Dataset")

    # Option to upload a file
    data_file = st.sidebar.file_uploader(
        "Upload a CSV, Parquet, or JSON file", type=["csv", "parquet", "json", "tsv"]
    )

    # Option to select a demo dataset
    demo_datasets_path = Path.cwd() / "app" / "demo_datasets.toml"
    try:
        with open(demo_datasets_path, "r") as f:
            demo_datasets = toml.load(f)["datasets"]
        demo_dataset_names = list(demo_datasets.keys())
        selected_demo_dataset = st.sidebar.selectbox("Select a Demo Dataset", ["None"] + demo_dataset_names)
    except FileNotFoundError:
        st.sidebar.warning("demo_datasets.toml not found. Demo datasets will not be available.")
        demo_datasets = {}
        selected_demo_dataset = "None"

    # Process data
    dataset_path = None
    uploaded_file_name = None  # Store the uploaded file name

    if data_file is not None:
        try:
            # Write the uploaded file to a temporary file
            temp_path = "temp_file"
            with open(temp_path, "wb") as f:
                f.write(data_file.getbuffer())
            dataset_path = temp_path
            uploaded_file_name = data_file.name  # Get the uploaded file name
            st.success(f"File '{data_file.name}' uploaded successfully!")
        except Exception as e:
            st.error(f"Error writing uploaded file: {e}")
    elif selected_demo_dataset != "None" and selected_demo_dataset in demo_datasets:
        dataset_path = demo_datasets[selected_demo_dataset]
        st.info(f"Using demo dataset: {selected_demo_dataset}")
    else:
        st.info("Please upload a file or select a demo dataset from the sidebar to begin.")

    if dataset_path:
        try:
            # Pass the file name to ingest_data so it can get the extension
            if looksee.ingest_data(dataset_path, uploaded_file_name):
                looksee.extract_metadata()

                # Display Metadata
                st.header("Metadata")
                metadata = looksee.display_metadata()
                if metadata:
                    st.dataframe(metadata)  # Display metadata as a dataframe
                else:
                    st.warning("No metadata available.")

                # Column Explorer
                st.header("Column Explorer")
                columns_query = f"SELECT column_name FROM information_schema.columns WHERE table_name='{looksee.table_name}'"
                columns = [col[0] for col in looksee.conn.execute(columns_query).fetchall()]
                selected_column = st.selectbox("Select a column to explore:", columns)

                if selected_column:
                    summary_stats = looksee.column_summary(selected_column)
                    st.subheader(f"Summary for '{selected_column}'")
                    st.json(summary_stats)

            else:
                st.error("Failed to ingest data. Check the file or dataset URL.")
        except Exception as e:
            st.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

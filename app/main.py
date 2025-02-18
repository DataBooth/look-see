import streamlit as st
from looksee import LookSee

def main():
    st.title("LookSee: Data Investigation Tool")

    # Load LookSee instance
    looksee = LookSee()

    # File upload or demo datasets
    st.sidebar.header("Upload Dataset")
    data_file = st.sidebar.file_uploader("Upload a CSV, Parquet, or JSON file", type=["csv", "parquet", "json"])

    if data_file:
        temp_path = "temp_file"
        
        with open(temp_path, "wb") as f:
            f.write(data_file.getbuffer())

        if looksee.ingest_data(temp_path):
            looksee.extract_metadata()

            st.header("Metadata")
            st.json(looksee.display_metadata())

            st.header("Column Explorer")
            
            columns_query = f"SELECT column_name FROM information_schema.columns WHERE table_name='{looksee.table_name}'"
            columns = [col[0] for col in looksee.conn.execute(columns_query).fetchall()]
            
            selected_column = st.selectbox("Select a column to explore:", columns)
            
            if selected_column:
                summary_stats = looksee.column_summary(selected_column)
                st.subheader(f"Summary for '{selected_column}'")
                st.json(summary_stats)

if __name__ == "__main__":
    main()

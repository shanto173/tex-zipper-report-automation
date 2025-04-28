import pandas as pd
import mysql.connector
from mysql.connector import Error
import re
from datetime import datetime
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

# --- Connect to Google Sheet ---

sheet_id_1 = "1Rz5ctnSMSh_UGmhYkE_jX6zYGCabz28BEHaNnfbRn0I"
sheet_name_1 = "Sheet1"


csv_url_1 = f"https://docs.google.com/spreadsheets/d/{sheet_id_1}/gviz/tq?tqx=out:csv&sheet={sheet_name_1}"


# Function to load data
def load_data(csv_url):
    try:
        # Load CSV data
        df = pd.read_csv(csv_url, low_memory=False)
        
        # Clean the column names
        df.columns = [clean_column_name(col) for col in df.columns]
        
        # Replace NaN values with an empty string or NULL
        df = df.fillna('')  # You can replace '' with 'NULL' if you prefer NULL for missing values
        
        # Rename first column to 'Issued_On'
        if df.columns[0] != 'OA':
            df.rename(columns={df.columns[0]: 'OA'}, inplace=True)
        
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None
    
    
# Function to clean column names (remove spaces, special characters, etc.)
def clean_column_name(col_name):
    col_name = col_name.strip()  # Remove leading/trailing spaces
    col_name = re.sub(r'[^A-Za-z0-9_]+', '', col_name)  # Replace non-alphanumeric characters with ''
    col_name = col_name.replace(' ', '_')  # Replace spaces with underscores
    col_name = col_name.replace('-', '_')  # Replace hyphens with underscores
    return col_name

# Load the data from both sheets
df_1 = load_data(csv_url_1)

# Show the first 5 rows of both dataframes
print("Data from Sheet 1 (first 5 rows):")
print(df_1.head())


db_config_order_rel = {
    'host': 'localhost', 
    'database': 'order_rel', 
    'user': 'root', 
    'password': '',
    'port': 3306
}

# Function to create MySQL connection
def create_connection_order_rel():
    try:
        conn = mysql.connector.connect(**db_config_order_rel)
        if conn.is_connected():
            print("Connected to MySQL database")
            return conn
    except Error as e:
        print(f"Connection Error: {e}")
        return None

# --- Insert data into DB ---
def insert_data_to_db(df, table_name):
    try:
        conn = create_connection_order_rel()
        if conn:
            cursor = conn.cursor()

            # Drop the table if it exists
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Table '{table_name}' dropped successfully (if it existed).")

            # Step 1: Convert columns that look like datetime to proper datetime dtype
            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        df[col] = pd.to_datetime(df[col], errors='raise')
                        print(f"Column '{col}' converted to datetime.")
                    except:
                        continue  # Skip conversion if it fails

            # Step 2: Mapping of pandas dtypes to MySQL data types
            dtype_mapping = {
                'object': 'VARCHAR(255)',
                'int64': 'BIGINT',
                'float64': 'DECIMAL(18,6)',
                'bool': 'TINYINT(1)',
                'datetime64[ns]': 'DATETIME'
            }

            # Create column definitions using inferred types
            column_definitions = []
            for col in df.columns:
                col_dtype = str(df[col].dtype)
                mysql_type = dtype_mapping.get(col_dtype, 'VARCHAR(255)')
                column_definitions.append(f"`{col}` {mysql_type}")

            create_table_query = f"""
            CREATE TABLE `{table_name}` (
                {', '.join(column_definitions)}
            )
            """
            cursor.execute(create_table_query)
            print(f"Table '{table_name}' created successfully with inferred data types.")

            # Prepare data for insertion
            rows_to_insert = [tuple(row) for row in df.values]
            insert_query = f"""
            INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in df.columns])})
            VALUES ({', '.join(['%s'] * len(df.columns))})
            """

            # Batch insert
            batch_size = 1000
            for i in range(0, len(rows_to_insert), batch_size):
                batch = rows_to_insert[i:i + batch_size]
                cursor.executemany(insert_query, batch)
                conn.commit()
                print(f"Inserted {len(batch)} rows into '{table_name}'.")

            cursor.close()
            conn.close()

    except Error as e:
        print(f"Error during database operation: {e}")

        
if df_1 is not None:
    insert_data_to_db(df_1, 'order_relased')

# --- Fetch summary data ---
def fetch_data_from_db_order_rel(query):
    try:
        conn = create_connection_order_rel()
        if conn:
            df = pd.read_sql(query, conn)
            print("Data fetched successfully.")
            return df
    except Error as e:
        print(f"Error fetching data: {e}")
        return None
    finally:
        if conn:
            conn.close()


# --- Push DataFrame to Google Sheets ---
# Order_MGT_SHEET_3
Sheet_3_order_MGT = fetch_data_from_db_order_rel("""
                                           
SELECT OA,Product,Category,SLider,Sum(`QuantityPCS`),`ReleaseDate` FROM order_rel.order_relased
where `ReleaseDate` BETWEEN DATE_FORMAT(CURDATE(), '%Y-%m-01') AND CURDATE()
group by OA,Product,Category,SLider,`ReleaseDate`
order by `ReleaseDate` desc; 


""")

Copy_Sheet_3_order_MGT = fetch_data_from_db_order_rel("""
                                           
SELECT 
    Category,
    tzp_numbers,
    `ReleaseDate`,
    SUM(`QuantityPCS`) AS total
FROM (
    SELECT 
        OA,
        Product,
        Category,
        Slider,
        `QuantityPCS`,
        `ReleaseDate`,
        Salesperson,
        team,
        TRIM(REGEXP_SUBSTR(Slider, 'TZP-[0-9]+(.*)')) AS tzp_numbers 
    FROM 
        order_rel.order_relased
) AS new_tab
WHERE 
    `ReleaseDate` BETWEEN DATE_FORMAT(CURDATE(), '%Y-%m-01') AND CURDATE()
GROUP BY 
    Category,
    tzp_numbers,
    `ReleaseDate`
ORDER BY 
    ReleaseDate desc;


""")


def authenticate_google_sheets(json_credentials_file):
    # Define the scope
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file"]

    # Authenticate using the service account credentials
    creds = ServiceAccountCredentials.from_json_keyfile_name(json_credentials_file, scope)
    client = gspread.authorize(creds)
    
    return client

import numpy as np

def paste_dataframe_to_sheet(
    sheet_id,
    worksheet_name,
    data_frame,
    credentials_file,
    start_cell="A1",
    clear_before_paste=True,
    clear_range=None,
    timestamp_cell=None,
    include_headers=True
):
    """
    Uploads a DataFrame to a specific Google Sheet and Worksheet.

    Parameters:
    - sheet_id (str): Google Sheet ID.
    - worksheet_name (str): Sheet/tab name inside the file.
    - data_frame (pd.DataFrame): Data to upload.
    - credentials_file (str): Path to the service account credentials JSON.
    - start_cell (str): Starting cell to paste data (e.g., "A1").
    - clear_before_paste (bool): Whether to clear data before pasting.
    - clear_range (str|None): Optional range to clear (e.g., "A1:Z100").
    - timestamp_cell (str|None): Optional cell (e.g., "K1") to write current timestamp after pasting.
    - include_headers (bool): Whether to include column headers in the pasted data.
    """
    try:
        # Authenticate with Google Sheets
        client = authenticate_google_sheets(credentials_file)
        sheet = client.open_by_key(sheet_id)
        worksheet = sheet.worksheet(worksheet_name)

        # Optional: Clear entire sheet or specific range
        if clear_before_paste:
            if clear_range:
                worksheet.batch_clear([clear_range])
            else:
                worksheet.clear()

        # Handle datetime columns
        for col in data_frame.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
            data_frame[col] = data_frame[col].dt.strftime('%Y-%m-%d %H:%M:%S')

        # Replace NaNs and NaTs
        data_frame.replace([np.nan, pd.NaT], '', inplace=True)

        # Format all values
        def format_cell(val):
            if pd.isna(val):
                return ''
            elif isinstance(val, (int, float)):
                return round(val, 4) if isinstance(val, float) else val
            else:
                return str(val)
        



        formatted_values = data_frame.applymap(format_cell).values.tolist()

        # Include headers if specified
        data = (
            [data_frame.columns.tolist()] + formatted_values
            if include_headers else
            formatted_values
        )

        # Push data to sheet
        worksheet.update(start_cell, data)

        # Optional: Insert timestamp
        if timestamp_cell:
            paste_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            worksheet.update(timestamp_cell, [[paste_time]])
            print(f"⏱️ Paste timestamp written to {timestamp_cell}: {paste_time}")

        print(f"Data pasted successfully to '{worksheet_name}' starting at {start_cell}")

    except Exception as e:
        print(f"Error pasting data to Google Sheets: {e}")

        
paste_dataframe_to_sheet(
    sheet_id="1acV7UrmC8ogC54byMrKRTaD9i1b1Cf9QZ-H1qHU5ZZc",
    worksheet_name="Sheet3",
    data_frame=Sheet_3_order_MGT,
    credentials_file="testing-456510-f87e37b9a9e4.json",
    start_cell="A2",
    clear_before_paste=False,
    clear_range="A2:F2000",
    timestamp_cell="L2",
    include_headers = False
)

# Zip RM std dashboard
paste_dataframe_to_sheet(
    sheet_id="1acV7UrmC8ogC54byMrKRTaD9i1b1Cf9QZ-H1qHU5ZZc",
    worksheet_name="Copy of Sheet3",
    data_frame=Copy_Sheet_3_order_MGT,
    credentials_file="testing-456510-f87e37b9a9e4.json",
    start_cell="A2",
    clear_before_paste=False,
    clear_range="A2:D1000",
    timestamp_cell="F2",
    include_headers = False
)

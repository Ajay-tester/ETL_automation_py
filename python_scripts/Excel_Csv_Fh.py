import os
import pandas as pd

# File names
csv_file = "employeees.csv"
excel_file = "T24_Sample_Test_Case_Layeer-2.xlsx"  # Ensure file extension is included

columns = []

# Check if either file exists
if os.path.exists(csv_file):
    # If the CSV file exists, process it
    try:
        print("We have a CSV file")
        with open(csv_file, "r") as file:
            columns = file.readline().strip().split(",")
    except Exception as e:
        print(f"Error while processing the CSV file: {e}")
        raise
elif os.path.exists(excel_file):
    # If the Excel file exists, process it
    try:
        print("We have an Excel file")
        df = pd.read_excel(excel_file)
        columns = df.columns.tolist()
    except Exception as e:
        print(f"Error while processing the Excel file: {e}")
        raise
else:
    # Raise an error if neither file exists
    raise FileNotFoundError("Neither CSV nor Excel file was found.")

# Convert the list of column names to a comma-separated string
if columns:
    columns_list = ", ".join(columns)

    # Generate the SQL query for detecting duplicates
    query = f"""
    SELECT {columns_list}, COUNT(*) 
    FROM table_name 
    GROUP BY {columns_list}
    HAVING COUNT(*) > 1;
    """

    # Print the query
    print(query)
else:
    print("No columns found in the file.")

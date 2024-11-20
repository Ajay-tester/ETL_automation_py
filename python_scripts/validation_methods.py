import pandas as pd
from pandasql import sqldf

# Data Completeness Validation
def count_check(source_file, target_file, log_file="log_files\count_log.csv"):
    try:
        # Load source and target files into DataFrames
        source_df = pd.read_csv(source_file)
        target_df = pd.read_csv(target_file)

        # Get the row counts
        source_count = len(source_df)
        target_count = len(target_df)

        # Compare the row counts
        if source_count == target_count:
            print("Row counts match! No mismatch found.")
        else:
            print("Mismatch found! Writing to log file...")
            
            # Prepare the mismatch details
            log_data = {
                "Source Count": [source_count],
                "Target Count": [target_count],
                "Difference": [abs(source_count - target_count)],
            }

            # Create a DataFrame for the log data
            print(log_data)
            log_df = pd.DataFrame(log_data)

            # Write the log data to a CSV file
            log_df.to_csv(log_file, index=False)
            print(f"Details written to {log_file}")

    except Exception as e:
        print(f"Error during count_check: {e}")

# Function to extract column names from a CSV file
def get_column_names(csv_file):
    try:
        with open(csv_file, "r") as file:
            columns = file.readline().strip().split(",")
        return columns
    except Exception as e:
        print(f"Error reading columns from {csv_file}: {e}")
        return []

# Function to validate data accuracy between source and target
def accuracy_check(source_file, target_file, log_file="log_files\\accuracy_log.csv"):
    """
    Validates data accuracy between source and target datasets dynamically.
    """
    try:
        # Load source and target files into DataFrames
        source_df = pd.read_csv(source_file)
        target_df = pd.read_csv(target_file)

        # Dynamically fetch column names
        source_columns = get_column_names(source_file)
        target_columns = get_column_names(target_file)

        # Generate the SELECT clause for all columns
        select_clauses = []
        for column in source_columns:
            if column in target_columns:
                select_clauses.append(
                    f"source.{column} AS Source_{column}, target.{column} AS Target_{column}"
                )

        select_clause = ",\n".join(select_clauses)

        # Define the SQL query dynamically
        query = f"""
        SELECT source.EMPLOYEE_ID,
               {select_clause}
        FROM source_df AS source
        LEFT JOIN target_df AS target 
        ON source.EMPLOYEE_ID = target.EMPLOYEE_ID
        WHERE {" OR ".join([f"(source.{col} != target.{col} OR target.{col} IS NULL)" for col in source_columns if col in target_columns])}
        AND source.EMPLOYEE_ID IS NOT NULL;
        """
    
        # Execute the query
        mismatched_rows = sqldf(query, locals())

        # Check for mismatches
        if mismatched_rows.empty:
            print("Data accuracy check passed! No mismatches found.")
        else:
            print("Data accuracy issues found! Writing to log file...")
            mismatched_rows.to_csv(log_file, index=False)
            print(f"Details written to {log_file}")

    except Exception as e:
        print(f"Error during accuracy_check: {e}")


# Function to check for duplicates in the target file
def duplicate_check(target_file, log_file="log_files\duplicate_log.csv"):
    try:
        # Load the target file into a DataFrame
        target_df = pd.read_csv(target_file)

        # Extract the column names for the target file
        target_columns = get_column_names(target_file)

        # Generate the SELECT clause for detecting duplicates by all columns
        group_by_clause = ", ".join(target_columns)

        # Define the SQL query to find duplicates based on all columns
        query = f"""
        SELECT {', '.join(target_columns)}, COUNT(*) AS count
        FROM target_df
        GROUP BY {group_by_clause}
        HAVING COUNT(*) > 1;
        """

        # Execute the SQL query to find duplicates
        duplicates = sqldf(query, locals())

        # Check if any duplicates were found
        if not duplicates.empty:
            print(f"Duplicates found! Writing to {log_file}...")
            duplicates.to_csv(log_file, index=False)  # Write the duplicates to the log file
        else:
            print("No duplicates found.")

    except Exception as e:
        print(f"Error during duplicate_check: {e}")

# Example Usage
if __name__ == "__main__":
    # File paths for source and target data
    source_file_path = "source_employees.csv"
    target_file_path = "target_employees.csv"

    # Call the function
    count_check(source_file_path, target_file_path)
    accuracy_check(source_file_path, target_file_path)
    duplicate_check(target_file_path)

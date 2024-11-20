import pandas as pd
from pandasql import sqldf  # To run SQL queries on DataFrames

def accuracy_check(source_file, target_file, log_file="accuracy_log.csv"):
    """
    Validates data accuracy between source and target datasets using dynamically generated SQL queries.
    """
    try:
        # Load source and target files into DataFrames
        source_df = pd.read_csv(source_file)
        target_df = pd.read_csv(target_file)

        # Extract column names dynamically
        columns = source_df.columns.tolist()
        column_comparisons = []

        # Build column comparison logic dynamically
        for column in columns:
            column_comparisons.append(
                f"source.{column} != target.{column} OR target.{column} IS NULL"
            )

        # Combine the comparison logic for WHERE clause
        comparison_clause = " OR ".join(column_comparisons)

        # Dynamically create the SQL query
        query = f"""
        SELECT source.EMPLOYEE_ID, 
               {', '.join([f"source.{col} AS Source_{col}, target.{col} AS Target_{col}" for col in columns])}
        FROM source_df AS source
        LEFT JOIN target_df AS target 
        ON source.EMPLOYEE_ID = target.EMPLOYEE_ID
        WHERE ({comparison_clause})
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


# Example Usage
if __name__ == "__main__":
    # File paths for source and target data
    source_file_path = "source_employees.csv"
    target_file_path = "target_employees.csv"

    # Run the accuracy check
    accuracy_check(source_file_path, target_file_path)

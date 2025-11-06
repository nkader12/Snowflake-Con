"""
Example usage of the Snowflake Connector package.

This script demonstrates the simple module-level API.
"""

import snowflake_connector as con

# Example 1: Basic usage with credential prompting
def example_basic_usage():
    """Basic example - credentials will be prompted."""
    print("\n=== Example 1: Basic Usage ===")
    
    # Connect - will prompt for account, username, and password
    # MFA will be handled automatically if required
    con.connect()
    
    # Query data (no MFA required for subsequent queries!)
    df = con.query("SELECT CURRENT_VERSION() as version")
    print(f"\nSnowflake version: {df['VERSION'].iloc[0]}")
    
    # Query more data using fully qualified names
    # df = con.query("SELECT * FROM database.schema.your_table LIMIT 100")
    # print(f"\nRetrieved {len(df)} rows")
    
    # Disconnect when done
    con.disconnect()


# Example 2: Connect with some parameters pre-filled
def example_with_params():
    """Example with some connection parameters provided."""
    print("\n=== Example 2: With Parameters ===")
    
    # Provide some parameters, others will be prompted
    con.connect(
        account="your_account",  # Pre-fill account
        warehouse="your_warehouse",  # Pre-fill warehouse
        # username and password will still be prompted
    )
    
    # Execute multiple queries
    result1 = con.fetch_one("SELECT CURRENT_DATABASE() as db")
    result2 = con.fetch_one("SELECT CURRENT_SCHEMA() as schema")
    result3 = con.fetch_one("SELECT CURRENT_WAREHOUSE() as wh")
    
    print(f"Database: {result1[0]}")
    print(f"Schema: {result2[0]}")
    print(f"Warehouse: {result3[0]}")
    
    con.disconnect()


# Example 3: Processing large datasets in batches
def example_batch_processing():
    """Example of processing large datasets efficiently."""
    print("\n=== Example 3: Batch Processing ===")
    
    con.connect()
    
    # Process data in batches (memory-efficient for large datasets)
    total_rows = 0
    for batch_df in con.query_batches(
        "SELECT * FROM database.schema.large_table",  # Use fully qualified names
        batch_size=100000
    ):
        total_rows += len(batch_df)
        print(f"Processed batch: {len(batch_df)} rows (Total: {total_rows})")
        
        # Your processing logic here
        # For example: save to file, transform, etc.
    
    print(f"\nTotal rows processed: {total_rows}")
    con.disconnect()


# Example 4: Fetching as dictionaries
def example_dict_format():
    """Example of fetching data as dictionaries."""
    print("\n=== Example 4: Dictionary Format ===")
    
    con.connect()
    
    # Fetch as list of dictionaries
    results = con.query_dict("SELECT id, name, value FROM database.schema.my_table LIMIT 10")
    
    for row in results:
        print(f"ID: {row['id']}, Name: {row['name']}, Value: {row['value']}")
    
    con.disconnect()


# Example 5: Execute DDL/DML operations
def example_execute():
    """Example of executing DDL/DML operations."""
    print("\n=== Example 5: Execute Operations ===")
    
    con.connect()
    
    # Execute DDL using fully qualified names
    con.execute("CREATE TABLE IF NOT EXISTS database.schema.test_table (id INT, name VARCHAR)")
    
    # Execute DML
    con.execute("INSERT INTO database.schema.test_table VALUES (1, 'test')")
    
    # Query the results
    df = con.query("SELECT * FROM database.schema.test_table")
    print(df)
    
    con.disconnect()


if __name__ == "__main__":
    print("Snowflake Connector Package - Usage Examples")
    print("=" * 50)
    print("\nSimple API:")
    print("  import snowflake_connector as con")
    print("  con.connect()  # Prompts for credentials")
    print("  df = con.query('SELECT * FROM database.schema.table')")
    print("  con.disconnect()")
    print("\n" + "=" * 50)
    
    # Uncomment the example you want to run:
    # example_basic_usage()
    # example_with_params()
    # example_batch_processing()
    # example_dict_format()
    # example_execute()
    
    print("\nExamples are ready to use. Uncomment and run the example you want.")

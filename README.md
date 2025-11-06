# Snowflake Connector Custom Package

A custom Python package for connecting to Snowflake with MFA authentication and efficient data retrieval capabilities.

## Features

- **MFA Authentication**: Connect to Snowflake with MFA, but only authenticate once per session
- **Efficient Data Retrieval**: Optimized methods for pulling large datasets using chunking and streaming
- **Connection Caching**: Reuse connections without re-authenticating
- **Multiple Output Formats**: Get data as pandas DataFrames, dictionaries, or raw tuples
- **Batch Processing**: Process very large datasets in memory-efficient batches
- **Simple API**: Just `import snowflake_connector as con` and start querying!

## Installation

### From Source

```bash
pip install -e .
```

### Dependencies

Install required packages:

```bash
pip install -r requirements.txt
```

## Usage

### Finding Your Snowflake Account Identifier

Before connecting, you'll need your Snowflake account identifier. It can be in several formats:

- **Account locator only**: `xy12345` (most common)
- **With organization**: `myorg-xy12345`
- **With region**: `xy12345.us-east-1`
- **Full identifier**: `myorg-xy12345.us-east-1`

**How to find it:**
1. Look at your Snowflake URL: `https://<account>.snowflakecomputing.com`
2. In Snowflake web interface: Click your profile → hover over account name
3. The account identifier is the part before `.snowflakecomputing.com`

### Simple API - Recommended

The simplest way to use the package:

```python
import snowflake_connector as con

# Connect - will prompt for credentials interactively
# When prompted for account, use one of the formats above
con.connect()

# Query data (MFA only required once on connection!)
df = con.query("SELECT * FROM database.schema.my_table")
print(df.head())

# Execute multiple queries without re-authenticating
df1 = con.query("SELECT * FROM database.schema.table1")
df2 = con.query("SELECT * FROM database.schema.table2")

# Disconnect when done
con.disconnect()
```

### Connect with Pre-filled Parameters

You can provide some parameters and be prompted for the rest:

```python
import snowflake_connector as con

# Provide some parameters, username/password will be prompted
con.connect(
    account="your_account",
    warehouse="your_warehouse",
    database="your_database"
)

# Query data
df = con.query("SELECT * FROM database.schema.my_table")
con.disconnect()
```

### Fetching Data as Pandas DataFrame

```python
import snowflake_connector as con

con.connect()

# Fetch all data at once (for smaller datasets)
df = con.query("SELECT * FROM database.schema.my_table")

# Fetch in chunks (for larger datasets)
df = con.query("SELECT * FROM database.schema.large_table", chunk_size=50000)

con.disconnect()
```

### Streaming Large Datasets

For very large datasets, use batch iterators to avoid loading everything into memory:

```python
import snowflake_connector as con

con.connect()

# Process data in batches
for batch_df in con.query_batches(
    "SELECT * FROM database.schema.very_large_table",
    batch_size=100000
):
    # Process each batch
    print(f"Processing batch with {len(batch_df)} rows")
    # Your processing logic here

con.disconnect()
```

### Fetching as Dictionaries

```python
import snowflake_connector as con

con.connect()

# Get results as list of dictionaries
results = con.query_dict("SELECT id, name, value FROM database.schema.my_table")

# Or in batches
for batch in con.query_dict_batches(
    "SELECT * FROM database.schema.large_table",
    batch_size=50000
):
    for row in batch:
        print(row['id'], row['name'])

con.disconnect()
```

### Executing Queries Without Results

```python
import snowflake_connector as con

con.connect()

# Execute DDL or DML operations
con.execute("CREATE TABLE IF NOT EXISTS database.schema.new_table (id INT, name VARCHAR)")
con.execute("INSERT INTO database.schema.new_table VALUES (1, 'test')")

con.disconnect()
```

### Fetching Single Row or All Rows

```python
import snowflake_connector as con

con.connect()

# Get single row
row = con.fetch_one("SELECT COUNT(*) as total FROM database.schema.my_table")
print(f"Total rows: {row[0]}")

# Get all rows as tuples
rows = con.fetch_all("SELECT id, name FROM database.schema.my_table LIMIT 10")
for row in rows:
    print(row)

con.disconnect()
```

## MFA Authentication

The package handles MFA authentication automatically:

1. **First Connection**: When you call `connect()`, you'll be prompted for your credentials and MFA token if required
2. **Subsequent Queries**: The connection is cached and reused, so no additional MFA prompts
3. **Connection Reuse**: As long as the connection is active, you can execute multiple queries without re-authenticating

### Example Workflow

```python
import snowflake_connector as con

# Step 1: Connect (prompts for credentials and MFA - only once!)
con.connect()
# You'll be prompted:
#   Enter Snowflake account: your_account
#   Enter Snowflake username: your_username
#   Enter Snowflake password: ****
#   (MFA token prompt if required)

# Step 2: Execute multiple queries (no MFA required!)
df1 = con.query("SELECT * FROM database.schema.table1")
df2 = con.query("SELECT * FROM database.schema.table2")
df3 = con.query("SELECT * FROM database.schema.table3")

# Step 3: Disconnect when done
con.disconnect()
```

## Fully Qualified Names

Since you don't need to specify database or schema in the connection, always use fully qualified names in your queries:

```python
# ✅ Good - fully qualified
df = con.query("SELECT * FROM database.schema.table_name")

# ❌ Avoid - unqualified names may not work
df = con.query("SELECT * FROM table_name")
```

## Performance Tips

1. **Use Batch Iterators**: For datasets larger than available memory, use `query_batches()` or `query_dict_batches()`

2. **Optimize Chunk Size**: Adjust `chunk_size` or `batch_size` based on your available memory:
   - Smaller chunks = less memory usage, more iterations
   - Larger chunks = more memory usage, fewer iterations

3. **Connection Reuse**: Keep the connection alive for multiple queries to avoid re-authentication

4. **Query Optimization**: Use appropriate WHERE clauses and LIMITs in your SQL queries when possible

## Error Handling

The package includes logging for debugging. To enable logging:

```python
import logging

logging.basicConfig(level=logging.INFO)
```

## Requirements

- Python 3.7+
- snowflake-connector-python 3.0.0+
- pandas 1.3.0+

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

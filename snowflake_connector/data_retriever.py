"""
Efficient Data Retrieval from Snowflake

Provides methods for retrieving large amounts of data efficiently
using chunking, streaming, and optimized fetch strategies.
"""

import pandas as pd
import logging
from typing import Optional, Iterator, List, Dict, Any
from .connection import SnowflakeConnection

logger = logging.getLogger(__name__)


class DataRetriever:
    """
    Efficient data retrieval from Snowflake.
    
    Provides optimized methods for pulling large datasets
    using chunking, streaming, and batch processing.
    """
    
    def __init__(self, connection: SnowflakeConnection):
        """
        Initialize DataRetriever with a Snowflake connection.
        
        Args:
            connection: SnowflakeConnection instance
        """
        self.connection = connection
    
    def fetch_pandas(
        self,
        query: str,
        chunk_size: Optional[int] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Execute query and return results as pandas DataFrame.
        
        For large datasets, use chunk_size to process in batches.
        
        Args:
            query: SQL query to execute
            chunk_size: If specified, fetch data in chunks of this size
            **kwargs: Additional parameters for cursor.execute()
            
        Returns:
            pandas DataFrame with query results
        """
        cursor = self.connection.execute_query(query, **kwargs)
        
        try:
            if chunk_size:
                # Fetch in chunks for memory efficiency
                logger.info(f"Fetching data in chunks of {chunk_size} rows")
                chunks = []
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    chunk_df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
                    chunks.append(chunk_df)
                
                result_df = pd.concat(chunks, ignore_index=True)
                logger.info(f"Retrieved {len(result_df)} rows in {len(chunks)} chunks")
            else:
                # Use Snowflake's optimized pandas fetch
                result_df = cursor.fetch_pandas_all()
                logger.info(f"Retrieved {len(result_df)} rows")
            
            return result_df
            
        finally:
            cursor.close()
    
    def fetch_pandas_batches(
        self,
        query: str,
        batch_size: int = 100000,
        **kwargs
    ) -> Iterator[pd.DataFrame]:
        """
        Execute query and return results as an iterator of pandas DataFrames.
        
        This method is memory-efficient for very large datasets as it
        yields data in batches rather than loading everything into memory.
        
        Args:
            query: SQL query to execute
            batch_size: Number of rows per batch (default: 100,000)
            **kwargs: Additional parameters for cursor.execute()
            
        Yields:
            pandas DataFrame batches
        """
        cursor = self.connection.execute_query(query, **kwargs)
        
        try:
            column_names = [desc[0] for desc in cursor.description]
            batch_count = 0
            
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                batch_df = pd.DataFrame(rows, columns=column_names)
                batch_count += 1
                logger.debug(f"Yielding batch {batch_count} with {len(batch_df)} rows")
                yield batch_df
            
            logger.info(f"Completed fetching {batch_count} batches")
            
        finally:
            cursor.close()
    
    def fetch_dict(
        self,
        query: str,
        chunk_size: Optional[int] = None,
        **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Execute query and return results as a list of dictionaries.
        
        Args:
            query: SQL query to execute
            chunk_size: If specified, fetch data in chunks of this size
            **kwargs: Additional parameters for cursor.execute()
            
        Returns:
            List of dictionaries, each representing a row
        """
        cursor = self.connection.execute_query(query, **kwargs)
        
        try:
            column_names = [desc[0] for desc in cursor.description]
            
            if chunk_size:
                results = []
                while True:
                    rows = cursor.fetchmany(chunk_size)
                    if not rows:
                        break
                    batch = [dict(zip(column_names, row)) for row in rows]
                    results.extend(batch)
                logger.info(f"Retrieved {len(results)} rows in chunks")
            else:
                rows = cursor.fetchall()
                results = [dict(zip(column_names, row)) for row in rows]
                logger.info(f"Retrieved {len(results)} rows")
            
            return results
            
        finally:
            cursor.close()
    
    def fetch_dict_batches(
        self,
        query: str,
        batch_size: int = 100000,
        **kwargs
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Execute query and return results as an iterator of dictionary lists.
        
        Memory-efficient for very large datasets.
        
        Args:
            query: SQL query to execute
            batch_size: Number of rows per batch (default: 100,000)
            **kwargs: Additional parameters for cursor.execute()
            
        Yields:
            Lists of dictionaries, each representing a batch of rows
        """
        cursor = self.connection.execute_query(query, **kwargs)
        
        try:
            column_names = [desc[0] for desc in cursor.description]
            batch_count = 0
            
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                batch = [dict(zip(column_names, row)) for row in rows]
                batch_count += 1
                logger.debug(f"Yielding batch {batch_count} with {len(batch)} rows")
                yield batch
            
            logger.info(f"Completed fetching {batch_count} batches")
            
        finally:
            cursor.close()
    
    def execute(
        self,
        query: str,
        **kwargs
    ) -> Any:
        """
        Execute a query without returning results (e.g., DDL, DML operations).
        
        Args:
            query: SQL query to execute
            **kwargs: Additional parameters for cursor.execute()
            
        Returns:
            Cursor object
        """
        return self.connection.execute_query(query, **kwargs)
    
    def fetch_one(self, query: str, **kwargs) -> Optional[tuple]:
        """
        Execute query and return a single row.
        
        Args:
            query: SQL query to execute
            **kwargs: Additional parameters for cursor.execute()
            
        Returns:
            Single row as tuple, or None if no results
        """
        cursor = self.connection.execute_query(query, **kwargs)
        try:
            return cursor.fetchone()
        finally:
            cursor.close()
    
    def fetch_all(self, query: str, **kwargs) -> List[tuple]:
        """
        Execute query and return all rows as tuples.
        
        Args:
            query: SQL query to execute
            **kwargs: Additional parameters for cursor.execute()
            
        Returns:
            List of tuples, each representing a row
        """
        cursor = self.connection.execute_query(query, **kwargs)
        try:
            return cursor.fetchall()
        finally:
            cursor.close()


from pyspark.sql import SparkSession
from typing import List, Dict, Optional
import logging
from datetime import datetime
from pyspark.sql import DataFrame


class MySQLExtractor:
    """Extract data from MySQL using Spark JDBC with optimized configurations"""

    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize MySQL extractor

        Args:
            spark: SparkSession instance
            config: MySQL connection configuration from config.yml
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.batch_size = config.get('batch_size', 100000)

    def _get_jdbc_url(self) -> str:
        """Get JDBC URL for MySQL connection"""
        return f"jdbc:mysql://{self.config['host']}:{self.config['port']}/{self.config['database']}"

    def _get_connection_properties(self) -> Dict:
        """Get MySQL connection properties with optimized settings"""
        return {
            "user": self.config['user'],
            "password": self.config['password'],
            "driver": "com.mysql.cj.jdbc.Driver",
            "fetchsize": str(self.batch_size),
            "useSSL": "false",
            "connectTimeout": "60000"  # 60 seconds
        }

    def extract_table(self,
                      table_name: str,
                      partition_column: Optional[str] = None,
                      where_clause: Optional[str] = None) -> DataFrame:
        """
        Extract a table from MySQL with optimized partitioning

        Args:
            table_name: Name of table to extract
            partition_column: Column to use for partitioning (for large tables)
            where_clause: Optional WHERE clause for filtering

        Returns:
            Spark DataFrame containing extracted data
        """
        try:
            start_time = datetime.now()
            self.logger.info(f"Starting extraction of table {table_name}")

            jdbc_url = self._get_jdbc_url()
            connection_properties = self._get_connection_properties()

            # For tables that need partitioning
            if partition_column:
                # Get partition bounds
                bounds_query = f"""
                    (SELECT MIN({partition_column}) as min_id,
                            MAX({partition_column}) as max_id,
                            COUNT(*) as total_count
                     FROM {table_name}
                     {f'WHERE {where_clause}' if where_clause else ''}) as bounds
                """

                bounds = self.spark.read.jdbc(
                    url=jdbc_url,
                    table=bounds_query,
                    properties=connection_properties
                ).collect()[0]

                # Adjust number of partitions based on table size
                total_count = bounds.total_count
                min_id = bounds.min_id
                max_id = bounds.max_id

                if not isinstance(max_id, int):
                    raise TypeError(f"Expected integer for max_id, got {type(max_id).__name__}")

                # Calculate optimal number of partitions
                if total_count < 100_000:  # Small tables
                    num_partitions = 4
                elif total_count < 1_000_000:  # Medium tables
                    num_partitions = 8
                else:  # Large tables
                    num_partitions = 16

                # Read with calculated partitioning
                df = self.spark.read.jdbc(
                    url=jdbc_url,
                    table=table_name,
                    column=partition_column,
                    lowerBound=min_id,
                    upperBound=max_id + 1,
                    numPartitions=num_partitions,
                    properties=connection_properties,
                    predicates=[where_clause] if where_clause else None
                )

            else:
                # For smaller tables, read without partitioning
                if where_clause:
                    dbtable = f"(SELECT * FROM {table_name} WHERE {where_clause}) as t"
                else:
                    dbtable = table_name

                df = self.spark.read.jdbc(
                    url=jdbc_url,
                    table=dbtable,
                    properties=connection_properties
                )

            # Log extraction metrics
            duration = (datetime.now() - start_time).total_seconds()
            record_count = df.count()
            partition_count = df.rdd.getNumPartitions()

            self.logger.info(
                f"Completed extraction of {table_name}:\n"
                f"Records: {record_count:,}\n"
                f"Partitions: {partition_count}\n"
                f"Duration: {duration:.2f} seconds"
            )

            return df

        except Exception as e:
            self.logger.error(f"Error extracting table {table_name}: {str(e)}")
            raise

    def extract_incremental(self,
                            table_name: str,
                            timestamp_column: str,
                            last_timestamp: str,
                            partition_column: Optional[str] = None) -> DataFrame:
        """
        Extract data incrementally based on timestamp

        Args:
            table_name: Name of table
            timestamp_column: Column containing timestamp
            last_timestamp: Last extracted timestamp
            partition_column: Optional column for partitioning
        """
        where_clause = f"{timestamp_column} > '{last_timestamp}'"
        return self.extract_table(table_name, partition_column, where_clause)

    def validate_extraction(self, table_name: str, df: DataFrame) -> bool:
        """
        Validate extracted data

        Args:
            table_name: Name of table
            df: Extracted DataFrame

        Returns:
            True if validation passes
        """
        try:
            # Get row count from source
            count_query = f"(SELECT COUNT(*) as count FROM {table_name}) as cnt"
            source_count = self.spark.read.jdbc(
                url=self._get_jdbc_url(),
                table=count_query,
                properties=self._get_connection_properties()
            ).collect()[0].count

            # Compare with extracted count
            extracted_count = df.count()

            if source_count != extracted_count:
                self.logger.warning(
                    f"Count mismatch for {table_name}:\n"
                    f"Source: {source_count:,}\n"
                    f"Extracted: {extracted_count:,}"
                )
                return False

            self.logger.info(f"Validation passed for {table_name}")
            return True

        except Exception as e:
            self.logger.error(f"Error validating {table_name}: {str(e)}")
            return False
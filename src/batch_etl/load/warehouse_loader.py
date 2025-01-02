from pyspark.sql import SparkSession, DataFrame
import logging
from typing import Dict, Optional
import time
import mysql.connector


class WarehouseLoader:
    """
    Loads data into existing MySQL data warehouse tables.
    Handles dimension and fact tables with foreign key constraints.
    """

    def __init__(self, spark: SparkSession, config: Dict):
        """Initialize warehouse loader with configurations."""
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)

        # JDBC configuration
        self.jdbc_url = f"jdbc:mysql://{config['host']}:{config['port']}/{config['database']}"
        self.jdbc_props = {
            "driver": "com.mysql.cj.jdbc.Driver",
            "user": config['user'],
            "password": config['password'],
            "batchsize": "10000"
        }

    def load_dimension(self, dim_name: str) -> None:
        """
        Load dimension table using merge strategy to handle foreign keys.

        Args:
            dim_name: Name of the dimension table
        """
        try:
            self.logger.info(f"Loading dimension table: {dim_name}")
            start_time = time.time()

            # Read dimension data
            df = self.spark.table(dim_name.lower())
            record_count = df.count()
            print(df.schema)

            # Create temporary table
            temp_table = f"temp_{dim_name}"
            df.write \
                .mode("overwrite") \
                .jdbc(
                url=self.jdbc_url,
                table=temp_table,
                properties=self.jdbc_props
            )

            # Merge data using MySQL connection
            self._merge_dimension_data(dim_name, temp_table)

            duration = time.time() - start_time
            self.logger.info(
                f"Loaded {record_count:,} records into {dim_name} "
                f"in {duration:.2f} seconds"
            )

        except Exception as e:
            self.logger.error(f"Error loading dimension {dim_name}: {str(e)}")
            raise

    def load_fact(
            self,
            fact_name: str,
            partition_column: str,
            num_partitions: int = 4
    ) -> None:
        """
        Load fact table in append mode with partitioning.

        Args:
            fact_name: Name of the fact table
            partition_column: Column to use for partitioning
            num_partitions: Number of partitions for parallel loading
        """
        try:
            self.logger.info(f"Loading fact table: {fact_name}")
            start_time = time.time()

            # Read and partition fact data
            df = (self.spark.table(fact_name.lower())
                  .repartition(num_partitions, partition_column))
            record_count = df.count()

            # Load in append mode
            df.write \
                .mode("append") \
                .option("numPartitions", str(num_partitions)) \
                .jdbc(
                url=self.jdbc_url,
                table=fact_name,
                properties=self.jdbc_props
            )

            duration = time.time() - start_time
            self.logger.info(
                f"Loaded {record_count:,} records into {fact_name} "
                f"in {duration:.2f} seconds"
            )

        except Exception as e:
            self.logger.error(f"Error loading fact {fact_name}: {str(e)}")
            raise

    def _merge_dimension_data(self, dim_name: str, temp_table: str) -> None:
        """
        Merge data from temporary table into dimension table.
        Handles updates while preserving referential integrity.
        """
        try:
            with mysql.connector.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    database=self.config['database'],
                    user=self.config['user'],
                    password=self.config['password']
            ) as connection:
                with connection.cursor() as cursor:
                    # Get primary key column
                    cursor.execute(f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                        WHERE TABLE_SCHEMA = '{self.config['database']}'
                        AND TABLE_NAME = '{dim_name}'
                        AND CONSTRAINT_NAME = 'PRIMARY'
                    """)
                    pk_column = cursor.fetchone()[0]

                    # Merge data
                    merge_sql = f"""
                        INSERT INTO {dim_name}
                        SELECT * FROM {temp_table} t
                        ON DUPLICATE KEY UPDATE
                        {', '.join([
                        f"{col} = t.{col}"
                        for col in self._get_updatable_columns(dim_name)
                    ])}
                    """
                    cursor.execute(merge_sql)
                    connection.commit()

                    # Clean up
                    cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")
                    connection.commit()

        except Exception as e:
            self.logger.error(f"Error merging data for {dim_name}: {str(e)}")
            raise

    def _get_updatable_columns(self, table_name: str) -> list:
        """Get columns that can be updated (excluding keys)."""
        try:
            with mysql.connector.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    database=self.config['database'],
                    user=self.config['user'],
                    password=self.config['password']
            ) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT COLUMN_NAME 
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = '{self.config['database']}'
                        AND TABLE_NAME = '{table_name}'
                        AND COLUMN_KEY NOT IN ('PRI', 'MUL')
                    """)
                    return [row[0] for row in cursor.fetchall()]

        except Exception as e:
            self.logger.error(f"Error getting updatable columns for {table_name}: {str(e)}")
            raise

    def load_all_dimensions(self, dimensions: Dict[str, DataFrame]) -> None:
        """
        Load multiple dimension tables in sequence.

        Args:
            dimensions: Dictionary of dimension name to DataFrame mappings
        """
        for dim_name in dimensions:
            self.load_dimension(dim_name)

    def load_all_facts(self, facts: Dict[str, Dict]) -> None:
        """
        Load multiple fact tables in sequence.

        Args:
            facts: Dictionary of fact configurations
        """
        for fact_name, config in facts.items():
            self.load_fact(
                fact_name=fact_name,
                partition_column=config['partition_column'],
                num_partitions=config.get('num_partitions', 4)
            )
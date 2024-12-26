from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, Optional, List
import logging
from datetime import datetime, timedelta
import os
from pathlib import Path


class DataLakeLoader:
    """Handler for loading data into different zones of the data lake with optimized configurations"""

    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize DataLakeLoader

        Args:
            spark: SparkSession instance
            config: Configuration dictionary containing data lake settings from config.yml
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Extract paths from config
        self.base_path = config['base_path']
        self.zones = config['zones']

        # Create zone directories if they don't exist
        self._initialize_zones()

    def _initialize_zones(self) -> None:
        """Initialize data lake zone directories"""
        try:
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jsc.hadoopConfiguration()
            )

            for zone in self.zones.values():
                zone_path = os.path.join(self.base_path, zone)
                path = self.spark._jvm.org.apache.hadoop.fs.Path(zone_path)
                if not fs.exists(path):
                    fs.mkdirs(path)
                    self.logger.info(f"Created zone directory: {zone_path}")

        except Exception as e:
            self.logger.error(f"Error initializing zones: {str(e)}")
            raise

    def _get_zone_path(self, zone: str, table_name: str) -> str:
        """
        Get full path for a table in a specific zone

        Args:
            zone: Zone name (raw, staging, processed)
            table_name: Name of the table

        Returns:
            Full HDFS path for the table
        """
        return os.path.join(self.base_path, self.zones[zone], table_name)

    def _add_metadata_columns(self, df: DataFrame, source_table: str) -> DataFrame:
        """
        Add metadata columns to DataFrame before saving

        Args:
            df: Input DataFrame
            source_table: Name of source table

        Returns:
            DataFrame with added metadata columns
        """
        return df.withColumns({
            "dl_ingestion_timestamp": F.current_timestamp(),
            "dl_source_table": F.lit(source_table),
            "dl_batch_id": F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")),
            "dl_process_date": F.current_date()
        })

    def load_to_raw(self,
                    df: DataFrame,
                    table_name: str,
                    partition_columns: Optional[List[str]] = None) -> None:
        """
        Load data to raw zone with metadata and optimized partitioning

        Args:
            df: DataFrame to save
            table_name: Name of the table
            partition_columns: Optional list of columns to partition by
        """
        try:
            start_time = datetime.now()
            self.logger.info(f"Starting raw zone load for {table_name}")

            # Add metadata columns
            df_with_metadata = self._add_metadata_columns(df, table_name)

            # Get path for raw zone
            raw_path = self._get_zone_path('raw', table_name)

            # Configure writer with optimized settings
            writer = df_with_metadata.write.format('parquet') \
                .mode('overwrite') \
                .option('compression', 'snappy')

            # Add partitioning if specified
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)

            # Save to raw zone
            writer.save(raw_path)

            # Log metrics
            duration = (datetime.now() - start_time).total_seconds()
            record_count = df.count()
            partition_info = f"partitioned by {partition_columns}" if partition_columns else "not partitioned"

            self.logger.info(
                f"Completed raw zone load for {table_name}:\n"
                f"Records: {record_count:,}\n"
                f"Partitioning: {partition_info}\n"
                f"Duration: {duration:.2f} seconds\n"
                f"Path: {raw_path}"
            )

        except Exception as e:
            self.logger.error(f"Error loading {table_name} to raw zone: {str(e)}")
            raise

    def load_to_staging(self,
                        df: DataFrame,
                        table_name: str,
                        partition_columns: Optional[List[str]] = None) -> None:
        """
        Load data to staging zone with optimization for transformations

        Args:
            df: DataFrame to save
            table_name: Name of the table
            partition_columns: Optional list of columns to partition by
        """
        try:
            start_time = datetime.now()
            staging_path = self._get_zone_path('staging', table_name)

            writer = df.write.format('parquet') \
                .mode('overwrite') \
                .option('compression', 'snappy')

            if partition_columns:
                writer = writer.partitionBy(*partition_columns)

            writer.save(staging_path)

            # Log metrics
            duration = (datetime.now() - start_time).total_seconds()
            record_count = df.count()

            self.logger.info(
                f"Completed staging zone load for {table_name}:\n"
                f"Records: {record_count:,}\n"
                f"Duration: {duration:.2f} seconds"
            )

        except Exception as e:
            self.logger.error(f"Error loading {table_name} to staging zone: {str(e)}")
            raise

    def load_to_processed(self,
                          df: DataFrame,
                          table_name: str,
                          update_type: str = 'overwrite',
                          partition_columns: Optional[List[str]] = None) -> None:
        """
        Load final transformed data to processed zone with optimized settings

        Args:
            df: DataFrame to save
            table_name: Name of the table
            update_type: How to update existing data (overwrite/append)
            partition_columns: Optional list of columns to partition by
        """
        try:
            start_time = datetime.now()
            processed_path = self._get_zone_path('processed', table_name)

            # Configure writer with optimized settings
            writer = df.write.format('parquet') \
                .mode(update_type) \
                .option('compression', 'snappy')

            if partition_columns:
                writer = writer.partitionBy(*partition_columns)

            writer.save(processed_path)

            # Log metrics
            duration = (datetime.now() - start_time).total_seconds()
            record_count = df.count()

            self.logger.info(
                f"Completed processed zone load for {table_name}:\n"
                f"Records: {record_count:,}\n"
                f"Update Type: {update_type}\n"
                f"Duration: {duration:.2f} seconds"
            )

        except Exception as e:
            self.logger.error(f"Error loading {table_name} to processed zone: {str(e)}")
            raise

    def cleanup_old_data(self, table_name: str, zone: str, days_to_keep: int) -> None:
        """
        Clean up old data based on retention policy

        Args:
            table_name: Name of the table
            zone: Zone name
            days_to_keep: Number of days of data to retain
        """
        try:
            path = self._get_zone_path(zone, table_name)
            cutoff_date = (datetime.now().date() - timedelta(days=days_to_keep)).strftime('%Y-%m-%d')

            # Read existing data
            df = self.spark.read.parquet(path)

            # Filter for retention
            df_retained = df.filter(F.col('dl_process_date') >= cutoff_date)

            # Write back retained data
            df_retained.write.format('parquet') \
                .mode('overwrite') \
                .option('compression', 'snappy') \
                .save(path)

            self.logger.info(
                f"Cleaned up old data for {table_name} in {zone} zone\n"
                f"Retention days: {days_to_keep}\n"
                f"Cutoff date: {cutoff_date}"
            )

        except Exception as e:
            self.logger.error(f"Error cleaning up old data for {table_name}: {str(e)}")
            raise

    def read_from_zone(self,
                      zone: str,
                      table_name: str,
                      columns: Optional[List[str]] = None) -> DataFrame:
        """
        Read data from a specific zone with optimized settings

        Args:
            zone: Zone name to read from
            table_name: Name of the table
            columns: Optional list of columns to select

        Returns:
            DataFrame containing the requested data
        """
        try:
            start_time = datetime.now()
            path = self._get_zone_path(zone, table_name)

            # Configure reader with optimized settings
            reader = self.spark.read.format('parquet') \
                .option('mergeSchema', 'true')

            # Read data
            df = reader.load(path)

            # Select specific columns if requested
            if columns:
                df = df.select(*columns)

            # Log metrics
            duration = (datetime.now() - start_time).total_seconds()
            record_count = df.count()

            self.logger.info(
                f"Read from {zone} zone for {table_name}:\n"
                f"Records: {record_count:,}\n"
                f"Columns: {columns if columns else 'all'}\n"
                f"Duration: {duration:.2f} seconds"
            )

            return df

        except Exception as e:
            self.logger.error(f"Error reading {table_name} from {zone} zone: {str(e)}")
            raise

    def validate_zone_data(self,
                          zone: str,
                          table_name: str,
                          validation_query: Optional[str] = None) -> bool:
        """
        Validate data in a specific zone with custom checks

        Args:
            zone: Zone to validate
            table_name: Name of the table
            validation_query: Optional SQL query for validation

        Returns:
            True if validation passes
        """
        try:
            start_time = datetime.now()
            df = self.read_from_zone(zone, table_name)

            # Basic validation checks
            validation_results = {
                'row_count': df.count() > 0,
                'metadata_columns': all(
                    col in df.columns for col in [
                        'dl_ingestion_timestamp',
                        'dl_source_table',
                        'dl_batch_id',
                        'dl_process_date'
                    ]
                )
            }

            # Run custom validation if provided
            if validation_query:
                df.createOrReplaceTempView(f"v_{table_name}")
                result = self.spark.sql(validation_query)
                validation_results['custom_query'] = result.first()[0] > 0

            # Log validation results
            duration = (datetime.now() - start_time).total_seconds()
            passed = all(validation_results.values())

            self.logger.info(
                f"Data validation for {table_name} in {zone} zone:\n"
                f"Status: {'PASSED' if passed else 'FAILED'}\n"
                f"Checks: {validation_results}\n"
                f"Duration: {duration:.2f} seconds"
            )

            return passed

        except Exception as e:
            self.logger.error(
                f"Error validating {table_name} in {zone} zone: {str(e)}"
            )
            return False

    def get_zone_statistics(self, zone: str, table_name: str) -> Dict:
        """
        Get detailed statistics for a table in a specific zone

        Args:
            zone: Zone name
            table_name: Name of the table

        Returns:
            Dictionary containing statistics
        """
        try:
            start_time = datetime.now()
            df = self.read_from_zone(zone, table_name)

            # Calculate statistics
            stats = {
                'record_count': df.count(),
                'partition_count': df.rdd.getNumPartitions(),
                'column_count': len(df.columns),
                'size_bytes': sum(
                    [f.size for f in self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                        self.spark._jsc.hadoopConfiguration()
                    ).listStatus(
                        self.spark._jvm.org.apache.hadoop.fs.Path(
                            self._get_zone_path(zone, table_name)
                        )
                    )]
                ),
                'last_modified': datetime.fromtimestamp(
                    self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                        self.spark._jsc.hadoopConfiguration()
                    ).getFileStatus(
                        self.spark._jvm.org.apache.hadoop.fs.Path(
                            self._get_zone_path(zone, table_name)
                        )
                    ).getModificationTime() / 1000
                ).strftime('%Y-%m-%d %H:%M:%S')
            }

            # Add execution metrics
            stats['execution_time'] = (datetime.now() - start_time).total_seconds()

            self.logger.info(
                f"Zone statistics for {table_name} in {zone}:\n"
                f"Records: {stats['record_count']:,}\n"
                f"Partitions: {stats['partition_count']}\n"
                f"Size: {stats['size_bytes'] / (1024*1024):.2f} MB\n"
                f"Last Modified: {stats['last_modified']}\n"
                f"Duration: {stats['execution_time']:.2f} seconds"
            )

            return stats

        except Exception as e:
            self.logger.error(
                f"Error getting statistics for {table_name} in {zone} zone: {str(e)}"
            )
            raise

    def _cleanup_temporary_files(self, table_name: str) -> None:
        """
        Clean up temporary files and staging data

        Args:
            table_name: Name of the table
        """
        try:
            # Clean up staging area
            staging_path = self._get_zone_path('staging', table_name)
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jsc.hadoopConfiguration()
            )
            staging_path_obj = self.spark._jvm.org.apache.hadoop.fs.Path(staging_path)

            if fs.exists(staging_path_obj):
                fs.delete(staging_path_obj, True)
                self.logger.info(f"Cleaned up staging data for {table_name}")

            # Clean up temporary views
            self.spark.catalog.dropTempView(f"v_{table_name}")

        except Exception as e:
            self.logger.warning(f"Error during cleanup for {table_name}: {str(e)}")
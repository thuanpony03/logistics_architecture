import os
from datetime import datetime
import yaml
from pyspark.sql import SparkSession
import time
from typing import Dict
from pyspark.sql import functions as F
import logging
from pathlib import Path

from pyspark.sql.functions import date_format

from src.extract.mysql_extractor import MySQLExtractor
from src.load.datalake_loader import DataLakeLoader
from src.transform.transform_executor import TransformExecutor
from src.load.warehouse_loader import WarehouseLoader

class LogisticsPipeline:
    """Main pipeline class for logistics data processing"""

    def __init__(self):
        self.start_time = datetime.utcnow()
        self.user = os.getenv('USER', 'thuanpony03')
        self.config = self.load_config()
        self.spark = self.spark_init()
        self._setup_logging()
        self.logger = logging.getLogger(__name__)

        # Initialize components
        self.extractor = MySQLExtractor(self.spark, self.config['mysql_source'])
        self.datalake = DataLakeLoader(self.spark, self.config['datalake'])
        self.transform_executor = TransformExecutor(self.spark, self.config)
        self.warehouse_loader = WarehouseLoader(self.spark, self.config['warehouse'])

        # Initialize metrics
        self.metrics = {
            'pipeline_info': {
                'start_time': self.start_time,
                'user': self.user,
                'environment': self.config['user']['environment']
            },
            'extraction_metrics': {},
            'load_metrics': {},
            'transformation_metrics': {},
            'warehouse_metrics': {}
        }

    def load_config(self) -> Dict:
        """Load and validate configuration"""
        try:
            config_path = Path(__file__).parent.parent / "config" / "config.yml"
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)

            # Add runtime information
            config['user'] = {
                'name': os.getenv('USER', 'thuanpony03'),
                'environment': 'development',
                'created_at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            }

            return config
        except Exception as e:
            raise RuntimeError(f"Failed to load configuration: {str(e)}")

    def spark_init(self) -> SparkSession:
        """Initialize Spark session with optimized configurations"""
        builder = SparkSession.builder.appName(self.config['spark']['app_name'])

        # Apply all Spark configurations from config
        for key, value in self.config['spark']['config'].items():
            builder = builder.config(key, value)

        return builder.getOrCreate()

    def _setup_logging(self) -> None:
        """Setup logging with configuration"""
        log_config = self.config['logging']
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        log_file = log_dir / f"pipeline_{timestamp}.log"

        logging.basicConfig(
            level=getattr(logging, log_config['level']),
            format=log_config['format'],
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )

    def extract_and_load_table(self, table_config: Dict) -> None:
        """Extract and load a single table with appropriate partitioning"""
        table_name = table_config['name']
        try:
            self.logger.info(f"Processing table: {table_name}")
            start_time = time.time()

            # Extract data with configured partitioning
            df = self.extractor.extract_table(
                table_name=table_name,
                partition_column=table_config.get('partition_column'),
                # batch_size=self.config['mysql_source']['batch_size']
            )

            # Record extraction metrics
            record_count = df.count()
            self.metrics['extraction_metrics'][table_name] = {
                'record_count': record_count,
                'partition_count': df.rdd.getNumPartitions(),
                'duration_seconds': time.time() - start_time
            }

            # Prepare write partitioning if configured
            write_partitions = table_config.get('write_partitions')
            if write_partitions:
                for col in write_partitions:
                    if 'date' in col or 'created_at' in col:
                        df = df.withColumn(
                            f"{col}_partition",
                            date_format(col, "yyyy-MM-dd")
                        )
                partition_cols = [
                    f"{col}_partition" if 'date' in col or 'created_at' in col else col
                    for col in write_partitions
                ]

            # Load to raw zone
            load_start_time = time.time()
            self.datalake.load_to_raw(
                df=df,
                table_name=table_name,
                partition_columns=partition_cols if write_partitions else None
            )

            # Record load metrics
            self.metrics['load_metrics'][table_name] = {
                'write_partitions': write_partitions,
                'record_count': record_count,
                'duration_seconds': time.time() - load_start_time
            }

            self.logger.info(
                f"Completed {table_name}: {record_count:,} records processed "
                f"in {time.time() - start_time:.2f} seconds"
            )

        except Exception as e:
            self.logger.error(f"Failed processing table {table_name}: {str(e)}")
            raise

    def extract(self) -> None:
        """Execute data extraction and loading for all configured tables"""
        try:
            self.logger.info("Starting data extraction phase")
            start_time = time.time()

            # Process tables in configured order
            for table_config in self.config['mysql_source']['tables']:
                self.extract_and_load_table(table_config)

            duration = time.time() - start_time
            self.logger.info(f"Data extraction phase completed in {duration:.2f} seconds")

        except Exception as e:
            self.logger.error(f"Error in extraction phase: {str(e)}")
            raise

    def transform(self) -> None:
        """Execute data transformation"""
        try:
            self.logger.info("Starting data transformation phase")
            start_time = time.time()

            # Execute transformation
            self.transform_executor.execute_transform()

            # Record transformation metrics
            self.metrics['transformation_metrics'] = self.transform_executor.get_transform_metrics()

            duration = time.time() - start_time
            self.logger.info(f"Data transformation phase completed in {duration:.2f} seconds")

        except Exception as e:
            self.logger.error(f"Error in transformation phase: {str(e)}")
            raise

    def load_to_warehouse(self) -> None:
        """
        Demonstrates how to load dimension or fact tables into a MySQL data warehouse
        using the WarehouseLoader class after transformation is complete.
        """
        try:
            self.logger.info("Starting warehouse loading phase")
            start_time = time.time()

            # Load dimensions
            dimensions = {
                'dim_date': self.spark.table('dim_date'),
                'dim_user': self.spark.table('dim_user'),
                'dim_driver': self.spark.table('dim_driver'),
                'dim_location': self.spark.table('dim_location'),
            }
            self.warehouse_loader.load_all_dimensions(dimensions)

            # Load facts
            facts = {
                'fact_orders': {
                    'partition_column': 'order_date_key',
                    'num_partitions': 4
                },
                'fact_shipment_tracking': {
                    'partition_column': 'date_key',
                    'num_partitions': 4
                }
            }
            self.warehouse_loader.load_all_facts(facts)

            duration = time.time() - start_time
            self.logger.info(f"Warehouse loading completed in {duration:.2f} seconds")

        except Exception as e:
            self.logger.error(f"Error in warehouse loading phase: {str(e)}")
            raise


    def run(self) -> Dict:
        """Execute the complete ETL pipeline"""
        try:
            self.logger.info(
                f"Starting logistics data pipeline\n"
                f"User: {self.user}\n"
                f"Environment: {self.config['user']['environment']}\n"
                f"Start Time (UTC): {self.start_time}"
            )

            # Execute pipeline phases
            self.extract()
            self.transform()
            self.load_to_warehouse()
            # Record completion
            self.metrics['pipeline_info']['end_time'] = datetime.utcnow()
            self.metrics['pipeline_info']['duration_seconds'] = \
                (self.metrics['pipeline_info']['end_time'] - self.start_time).total_seconds()

            self.logger.info(
                f"Pipeline completed successfully in "
                f"{self.metrics['pipeline_info']['duration_seconds']:.2f} seconds"
            )

            # Save metrics if configured
            if self.config['metrics']['enabled']:
                self._save_metrics()

            return self.metrics

        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise

    def _save_metrics(self) -> None:
        """Save pipeline metrics to file"""
        try:
            metrics_dir = Path(self.config['metrics']['save_path']).parent
            metrics_dir.mkdir(exist_ok=True)

            metrics_file = Path(self.config['metrics']['save_path'].format(
                date=datetime.utcnow().strftime('%Y%m%d')
            ))

            with open(metrics_file, 'w') as f:
                yaml.dump(self.metrics, f)

        except Exception as e:
            self.logger.error(f"Failed to save metrics: {str(e)}")


if __name__ == "__main__":
    pipeline = LogisticsPipeline()
    metrics = pipeline.run()

    # Print execution summary
    print("\nPipeline Execution Summary:")
    print("=" * 50)
    print(f"Start Time (UTC): {metrics['pipeline_info']['start_time']}")
    print(f"End Time (UTC): {metrics['pipeline_info']['end_time']}")
    print(f"Duration: {metrics['pipeline_info']['duration_seconds']:.2f} seconds")
    print(f"User: {metrics['pipeline_info']['user']}")
    print(f"Environment: {metrics['pipeline_info']['environment']}")

    print("\nExtraction Metrics:")
    print("-" * 50)
    for table, data in metrics['extraction_metrics'].items():
        print(f"\nTable: {table}")
        print(f"Records: {data['record_count']:,}")
        print(f"Read Partitions: {data['partition_count']}")
        print(f"Duration: {data['duration_seconds']:.2f} seconds")

    print("\nLoad Metrics:")
    print("-" * 50)
    for table, data in metrics['load_metrics'].items():
        print(f"\nTable: {table}")
        print(f"Records Written: {data['record_count']:,}")
        print(f"Write Partitions: {data['write_partitions'] or 'None'}")
        print(f"Duration: {data['duration_seconds']:.2f} seconds")

    print("\nTransformation Metrics:")
    print("-" * 50)
    for table, data in metrics['transformation_metrics'].items():
        print(f"\nTable: {table}")
        print(f"{data}")

    print("\nWarehouse Loading Metrics:")
    print("-" * 50)
    for table, data in metrics['warehouse_metrics'].items():
        print(f"\nTable: {table}")
        print(f"Status: {data['status']}")
        print(f"Load time (seconds): {data['load_time_seconds']:.2f}")
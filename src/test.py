import os
from datetime import datetime
import yaml
from pyspark.sql import SparkSession
import time
from typing import Dict
from src.extract.mysql_extractor import MySQLExtractor
from transform.transform_executor import TransformExecutor
from load.warehouse_loader import WarehouseLoader
from load.datalake_loader import DataLakeLoader
import logging

class LogisticsPipeline:
    def __init__(self):
        self.spark = self.spark_init()
        self.config = self.load_config()
        # Setup logging
        self._setup_logging()
        self.logger = logging.getLogger(__name__)

        self.extractor = MySQLExtractor(self.spark, self.config['mysql_source'])
        self.transform = TransformExecutor(self.spark, self.config)
        self.warehouse = WarehouseLoader(self.spark, self.config)
        self.datalake = DataLakeLoader(self.spark, self.config)
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'extraction_counts': {},
            'transform_counts': {},
            'load_counts': {}
        }

    def spark_init(self):
        spark = SparkSession.builder \
            .appName("MySQL Extractor Test") \
            .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.28") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.memory.fraction", "0.8") \
            .getOrCreate()

        # # Create SparkSession builder
        # builder = SparkSession.builder \
        #     .appName(self.config['spark']['app_name']) \
        #     .master(self.config['spark'].get('master', 'local[*]'))
        #
        # # Apply all configurations from YAML
        # for key, value in self.config['spark']['config'].items():
        #     builder = builder.config(key, value)

        # Create SparkSession
        # spark = builder.getOrCreate()

        return spark


    def _setup_logging(self) -> None:
        """Setup logging configuration"""
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(log_dir, f"pipeline_{timestamp}.log")

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )


    def load_config(self):
        config_path = "/home/ponydasierra/projects/logistics-data-pipeline/config/config.yml"
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        return config

    def extract(self) -> None:
        """Execute data extraction phase"""
        try:
            self.logger.info("Starting data extraction phase")
            start_time = time.time()

            # Extract all configured tables
            tables = [
                ("Users", "user_id"),
                ("Drivers", "driver_id"),
                ("Orders", "order_id"),
                ("Shipments", "shipment_id"),
                ("Payments", "payment_id"),
                ("Notifications", None)
            ]

            for table_name, partition_column in tables:
                df = self.extractor.extract_table(table_name, partition_column)
                self.metrics['extraction_counts'][table_name] = df.count()
                # Register as temp view for transformation
                df.createOrReplaceTempView(table_name.lower())
                self.datalake.load_to_raw(df, table_name, partition_column)

            duration = time.time() - start_time
            self.logger.info(f"Data extraction and load to raw completed in {duration:.2f} seconds")

        except Exception as e:
            self.logger.error(f"Error in extraction phase: {str(e)}")
            raise

    def run(self) -> Dict:
        """
        Execute the complete ETL pipeline

        Returns:
            Dictionary containing pipeline metrics
        """
        try:
            self.logger.info("Starting logistics data pipeline")
            self.metrics['start_time'] = datetime.now()
            # Execute pipeline phases
            self.extract()
            # self.transform()
            # self.load()

            # Complete pipeline
            self.metrics['end_time'] = datetime.now()
            duration = (self.metrics['end_time'] - self.metrics['start_time']).total_seconds()

            self.logger.info(f"Pipeline completed successfully in {duration:.2f} seconds")
            return self.get_detail_metrics()

        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise
        # finally:
        #     self._cleanup()

    def get_detail_metrics(self) -> Dict:
        """Get pipeline execution metrics"""
        return {
            'pipeline_info': {
                'start_time': self.metrics['start_time'],
                'end_time': self.metrics['end_time'],
                'duration_seconds': (self.metrics['end_time'] - self.metrics['start_time']).total_seconds()
            },
            'extraction_metrics': self.metrics['extraction_counts'],
            'transform_metrics': self.metrics['transform_counts'],
            'load_metrics': self.metrics['load_counts']
        }

    # def _cleanup(self) -> None:
    #     """Cleanup resources after pipeline execution"""
    #     try:
    #         # Clean up temporary views
    #         for table in self.spark.catalog.listTables():
    #             if table.isTemporary:
    #                 self.spark.catalog.dropTempView(table.name)
    #
    #         # Stop Spark session
    #         spark.stop()
    #
    #         self.logger.info("Cleanup completed successfully")
    #
    #     except Exception as e:
    #         self.logger.error(f"Error during cleanup: {str(e)}")
if __name__ == "__main__":
    extract = LogisticsPipeline()
    metrics = extract.run()

    #Print metrics
    print("\nPipeline Metrics:")
    print("-" * 50)
    print(f"Start Time: {metrics['pipeline_info']['start_time']}")
    print(f"End Time: {metrics['pipeline_info']['end_time']}")
    print(f"Duration: {metrics['pipeline_info']['duration_seconds']:.2f} seconds")

    print("\nExtraction Counts:")
    for table, count in metrics['extraction_metrics'].items():
        print(f"{table}: {count:,} records")


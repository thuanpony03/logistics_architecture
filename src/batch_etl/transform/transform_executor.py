from pyspark.sql import SparkSession, DataFrame
from typing import Dict
import logging
from datetime import datetime
from pathlib import Path
import pyspark.sql.functions as F

from src.batch_etl.transform.dimension_builder import DimensionBuilder
from src.batch_etl.transform.fact_builder import FactBuilder
from src.batch_etl.load.datalake_loader import DataLakeLoader


class TransformExecutor:
    """Main class to orchestrate the transformation process"""

    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize transform executor with current context

        Args:
            spark: SparkSession instance
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)

        # Set execution context
        self.execution_timestamp = "2024-12-24 09:43:35"
        self.execution_user = "thuanpony03"

        # Initialize builders
        self.dim_builder = DimensionBuilder(spark, config)
        self.fact_builder = FactBuilder(spark, config)
        self.datalake_loader = DataLakeLoader(spark, config['datalake'])

        # Track transformation state
        self.dimensions = {}
        self.facts = {}
        self.metrics = {
            'start_time': self.execution_timestamp,
            'dimension_counts': {},
            'fact_counts': {},
            'errors': []
        }

    def transform_dimensions(self) -> None:
        """Transform and build all dimension tables"""
        try:
            self.logger.info(
                f"Starting dimension transformation at {self.execution_timestamp}"
            )

            # Build date dimension (independent)
            self.dimensions['date'] = self.dim_builder.build_date_dimension(
                start_date='2024-01-01',
                end_date='2024-12-31'
            )

            # Load source data for other dimensions
            users_df = self.spark.read.parquet(
                f"{self.config['datalake']['base_path']}/raw/Users"
            )
            drivers_df = self.spark.read.parquet(
                f"{self.config['datalake']['base_path']}/raw/Drivers"
            )
            orders_df = self.spark.read.parquet(
                f"{self.config['datalake']['base_path']}/raw/Orders"
            )
            shipments_df = self.spark.read.parquet(
                f"{self.config['datalake']['base_path']}/raw/Shipments"
            )

            # Build user dimension
            self.dimensions['user'] = self.dim_builder.build_user_dimension(users_df)

            # Build driver dimension
            self.dimensions['driver'] = self.dim_builder.build_driver_dimension(
                drivers_df,
                self.dimensions['user']
            )

            # Build location dimension from combined addresses
            # Create a combined address dataset
            addresses = orders_df.select(
                'pickup_address', 'delivery_address'
            ).crossJoin(
                shipments_df.select('current_location')
            )

            self.dimensions['location'] = self.dim_builder.build_location_dimension(
                addresses
            )

            # # Build payment method dimension
            # self.dimensions['payment_method'] = self.dim_builder.build_payment_method_dimension()

            self.logger.info(
                f"Completed dimension transformation with counts:\n" +
                "\n".join([
                    f"{dim}: {df.count()} rows"
                    for dim, df in self.dimensions.items()
                ])
            )

        except Exception as e:
            self.logger.error(f"Error in dimension transformation: {str(e)}")
            raise


    def transform_facts(self) -> None:
        """Transform and build fact tables based on schema"""
        try:
            self.logger.info(
                f"Starting fact transformation at {self.execution_timestamp}"
            )

            # Load source data
            orders_df = self.spark.read.parquet(
                f"{self.config['datalake']['base_path']}/raw/Orders"
            )
            shipments_df = self.spark.read.parquet(
                f"{self.config['datalake']['base_path']}/raw/Shipments"
            )
            payments_df = self.spark.read.parquet(
                f"{self.config['datalake']['base_path']}/raw/Payments"
            )

            # Build orders fact
            self.facts['orders'] = self.fact_builder.build_orders_fact(
                orders_df=orders_df,
                payments_df=payments_df,
                dim_user=self.dimensions['user'],
                dim_driver=self.dimensions['driver'],  # Will be used in shipment fact
                dim_date=self.dimensions['date'],
                dim_location=self.dimensions['location'],
                # dim_payment_method=self.dimensions['payment_method']
            )

            # Build shipment tracking fact table
            self.facts['shipment_tracking'] = self.fact_builder.build_shipment_tracking_fact(
                shipments_df=shipments_df,
                fact_orders=self.facts['orders'],
                dim_driver=self.dimensions['driver'],
                dim_date=self.dimensions['date'],
                dim_location=self.dimensions['location']
            )

            # Update metrics
            self.metrics['fact_counts'] = {
                name: df.count() for name, df in self.facts.items()
            }

            self.logger.info(
                f"Completed fact transformation\n"
                f"Built facts: {list(self.facts.keys())}\n"
                f"Counts: {self.metrics['fact_counts']}"
            )

        except Exception as e:
            self.logger.error(f"Error in fact transformation: {str(e)}")
            self.metrics['errors'].append(str(e))
            raise

    def save_transformed_data(self) -> None:
        """Save transformed tables to the processed zone"""
        try:
            self.logger.info("Saving transformed tables to processed zone")

            # Save dimensions with overwrite mode
            for dim_name, dim_df in self.dimensions.items():
                self.datalake_loader.load_to_processed(
                    df=dim_df,
                    table_name=f"dim_{dim_name}",
                    update_type='overwrite'
                )

            # Save facts with append mode to maintain history
            for fact_name, fact_df in self.facts.items():
                self.datalake_loader.load_to_processed(
                    df=fact_df,
                    table_name=f"fact_{fact_name}",
                    update_type='append'
                )

            self.logger.info("Successfully saved all transformed tables")

        except Exception as e:
            self.logger.error(f"Error saving transformed tables: {str(e)}")
            self.metrics['errors'].append(str(e))
            raise

    def get_transform_metrics(self) -> Dict:
        """
        Get comprehensive metrics about the transformation process

        Returns:
            Dictionary containing detailed transformation metrics
        """
        try:
            # Calculate end time and duration if not set
            if not self.metrics['end_time']:
                self.metrics['end_time'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                self.metrics['duration_seconds'] = (
                        datetime.strptime(self.metrics['end_time'], '%Y-%m-%d %H:%M:%S') -
                        datetime.strptime(self.metrics['start_time'], '%Y-%m-%d %H:%M:%S')
                ).total_seconds()

            # Get current counts for all tables
            dimension_counts = {
                name: df.count() for name, df in self.dimensions.items()
            }
            fact_counts = {
                name: df.count() for name, df in self.facts.items()
            }

            # Validate dimensions
            validation_results = {}
            for dim_name, dim_df in self.dimensions.items():
                validation_results[f'dim_{dim_name}'] = self._validate_dimension(
                    dim_df, dim_name
                )

            # Validate facts
            for fact_name, fact_df in self.facts.items():
                validation_results[f'fact_{fact_name}'] = self._validate_fact(
                    fact_df, fact_name
                )

            transform_metrics = {
                'execution_context': {
                    'timestamp': self.execution_timestamp,
                    'user': self.execution_user,
                    'duration_seconds': self.metrics['duration_seconds']
                },
                'dimension_metrics': {
                    name: {
                        'row_count': count,
                        'validation': validation_results.get(f'dim_{name}', {})
                    }
                    for name, count in dimension_counts.items()
                },
                'fact_metrics': {
                    name: {
                        'row_count': count,
                        'validation': validation_results.get(f'fact_{name}', {})
                    }
                    for name, count in fact_counts.items()
                },
                'errors': self.metrics['errors']
            }

            self.logger.info(
                f"Transform metrics generated:\n"
                f"Duration: {transform_metrics['execution_context']['duration_seconds']:.2f} seconds\n"
                f"Dimensions processed: {len(dimension_counts)}\n"
                f"Facts processed: {len(fact_counts)}\n"
                f"Total errors: {len(self.metrics['errors'])}"
            )

            return transform_metrics

        except Exception as e:
            self.logger.error(f"Error generating transform metrics: {str(e)}")
            return {
                'error': str(e),
                'timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            }

    def _validate_dimension(self, dim_df: DataFrame, dim_name: str) -> Dict:
        """Validate a dimension table"""
        try:
            key_column = f"{dim_name}_key"
            validation = {
                'has_key_column': key_column in dim_df.columns,
                'null_keys': 0,
                'duplicate_keys': 0
            }

            if validation['has_key_column']:
                validation['null_keys'] = dim_df.filter(
                    F.col(key_column).isNull()
                ).count()

                key_counts = dim_df.groupBy(key_column).count()
                validation['duplicate_keys'] = key_counts.filter(
                    F.col('count') > 1
                ).count()

            return validation

        except Exception as e:
            self.logger.error(f"Error validating dimension {dim_name}: {str(e)}")
            return {'error': str(e)}

    def _validate_fact(self, fact_df: DataFrame, fact_name: str) -> Dict:
        """Validate a fact table"""
        try:
            validation = {
                'null_keys': 0,
                'orphaned_records': 0
            }

            # Check for null keys
            key_columns = [col for col in fact_df.columns if col.endswith('_key')]
            for key_col in key_columns:
                null_count = fact_df.filter(F.col(key_col).isNull()).count()
                validation['null_keys'] += null_count

            return validation

        except Exception as e:
            self.logger.error(f"Error validating fact {fact_name}: {str(e)}")
            return {'error': str(e)}

    def execute_transform(self) -> Dict:
        """
        Execute the complete transformation process

        Returns:
            Dictionary containing transformation metrics
        """
        try:
            self.logger.info(
                f"Starting transformation process\n"
                f"Timestamp: {self.execution_timestamp}\n"
                f"User: {self.execution_user}"
            )

            # Execute transformations
            self.transform_dimensions()
            self.transform_facts()
            self.save_transformed_data()

            # Update final metrics
            self.metrics['end_time'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            self.metrics['duration_seconds'] = (
                    datetime.strptime(self.metrics['end_time'], '%Y-%m-%d %H:%M:%S') -
                    datetime.strptime(self.metrics['start_time'], '%Y-%m-%d %H:%M:%S')
            ).total_seconds()

            self.logger.info(
                f"Completed transformation process\n"
                f"Duration: {self.metrics['duration_seconds']:.2f} seconds"
            )

            return self.metrics

        except Exception as e:
            self.logger.error(f"Transform process failed: {str(e)}")
            self.metrics['errors'].append(str(e))
            raise
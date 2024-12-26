from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from typing import Dict, Optional, List
from datetime import datetime
import logging
from pathlib import Path


class FactBuilder:
    """
    Responsible for building fact tables in the data warehouse.
    Works in conjunction with dimension tables to create a complete star schema.
    """

    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize FactBuilder

        Args:
            spark: SparkSession instance
            config: Configuration dictionary containing table definitions
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.current_timestamp = "2024-12-24 09:39:09"
        self.current_user = "thuanpony03"

    def _calculate_duration_minutes(self, start_time: str, end_time: str) -> F.Column:
        """
        Calculate duration between timestamps in minutes

        Args:
            start_time: Start timestamp column name
            end_time: End timestamp column name

        Returns:
            Column containing duration in minutes
        """
        return F.when(
            F.col(end_time).isNotNull(),
            F.round((F.unix_timestamp(end_time) - F.unix_timestamp(start_time)) / 60)
        ).otherwise(None)

    def build_orders_fact(
            self,
            orders_df: DataFrame,
            payments_df: DataFrame,
            dim_user: DataFrame,
            dim_driver: DataFrame,
            dim_date: DataFrame,
            dim_location: DataFrame,
            # dim_payment_method: DataFrame
    ) -> DataFrame:
        """
        Build orders fact table with all dimensions

        Args:
            orders_df: Source orders DataFrame
            payments_df: Source payments DataFrame
            dim_*: Dimension tables for lookups

        Returns:
            Orders fact table DataFrame
        """
        try:
            self.logger.info("Building orders fact table")

            # 1. Join orders with payments first
            orders_with_payment = orders_df.join(
                payments_df.select('order_id', 'amount', 'payment_method', 'payment_status'),
                'order_id',
                'left'
            )

            # 2. Add date keys
            orders_with_dates = orders_with_payment.withColumns({
                'order_date_key': F.date_format('created_at', 'yyyyMMdd').cast('int'),
                'delivery_date_key': F.date_format('delivery_time', 'yyyyMMdd').cast('int')
            })

            # 3. Join with dimensions one by one
            # User dimension
            fact_orders = orders_with_dates.join(
                dim_user.select('user_key', 'user_id'),
                'user_id',
                'left'
            )

            # Location dimension - pickup
            fact_orders = fact_orders.join(
                dim_location.select('location_key', 'full_address').alias('pickup_loc'),
                fact_orders.pickup_address == F.col('pickup_loc.full_address'),
                'left'
            ).withColumnRenamed('location_key', 'pickup_location_key')

            # Location dimension - delivery
            fact_orders = fact_orders.join(
                dim_location.select('location_key', 'full_address').alias('delivery_loc'),
                fact_orders.delivery_address == F.col('delivery_loc.full_address'),
                'left'
            ).withColumnRenamed('location_key', 'delivery_location_key')

            # # Payment method dimension
            # fact_orders = fact_orders.join(
            #     dim_payment_method,
            #     fact_orders.payment_method == dim_payment_method.payment_method,
            #     'left'
            # )

            # 4. Calculate measures
            fact_orders = fact_orders.withColumn(
                'delivery_duration_minutes',
                F.when(
                    F.col('delivery_time').isNotNull(),
                    F.round((F.unix_timestamp('delivery_time') -
                             F.unix_timestamp('created_at')) / 60)
                ).otherwise(None)
            )
            # 5. Generate sequential order_key using row_number instead of monotonically_increasing_id
            window_spec = Window.orderBy('order_id')
            # 5. Select final columns with proper column references
            fact_orders = fact_orders.select(
                # Surrogate key
                F.row_number().over(window_spec).alias('order_key'),

                # Business keys
                F.col('order_id'),

                # Dimension keys
                F.col('user_key'),
                F.col('order_date_key'),
                F.col('pickup_location_key'),
                F.col('delivery_location_key'),
                F.col('payment_method_key'),

                # Measures
                F.col('package_weight'),
                F.col('amount').alias('order_amount'),
                F.col('delivery_duration_minutes'),

                # Status tracking
                F.col('status').alias('order_status'),
                F.col('payment_status'),

                # Timestamps
                F.col('created_at').alias('order_created_at'),
                F.col('delivery_time').alias('order_completed_at')
            )

            # Add ETL metadata
            fact_orders = fact_orders.withColumns({
                'etl_timestamp': F.lit(self.current_timestamp),
                'etl_user': F.lit(self.current_user)
            })

            # Log metrics
            row_count = fact_orders.count()
            self.logger.info(
                f"Successfully built orders fact table with {row_count} rows\n"
                f"Time range: {fact_orders.agg(F.min('order_created_at'), F.max('order_created_at')).collect()[0]}"
            )

            fact_orders.createTempView('fact_orders')

            return fact_orders

        except Exception as e:
            self.logger.error(f"Error building orders fact table: {str(e)}")
            raise

    def build_shipment_tracking_fact(
            self,
            shipments_df: DataFrame,
            fact_orders: DataFrame,
            dim_driver: DataFrame,
            dim_date: DataFrame,
            dim_location: DataFrame
    ) -> DataFrame:
        """
        Build the shipment tracking fact table

        Args:
            shipments_df: Source shipments DataFrame
            fact_orders: Orders fact table for order_key lookup
            dim_*: Dimension tables for key lookups

        Returns:
            Shipment tracking fact DataFrame
        """
        try:
            self.logger.info("Building shipment tracking fact table")

            # 1. Add date key to shipments
            shipments_with_date = shipments_df.withColumn(
                'date_key',
                F.date_format('estimated_delivery_time', 'yyyyMMdd').cast('int')
            )

            # 2. Join with fact_orders to get order_key
            fact_shipments = shipments_with_date.join(
                fact_orders.select('order_key', 'order_id'),
                'order_id',
                'left'
            )

            # 3. Join with driver dimension
            fact_shipments = fact_shipments.join(
                dim_driver.select('driver_key', 'driver_id'),
                'driver_id',
                'left'
            )

            # # 4. Join with location dimension
            # fact_shipments = fact_shipments.join(
            #     dim_location.select('location_key', 'full_address'),
            #     fact_shipments.current_location == dim_location.full_address,
            #     'left'
            # )

            # 5. Calculate elapsed time using estimated_delivery_time
            fact_shipments = fact_shipments.withColumn(
                'elapsed_time_minutes',
                F.when(
                    F.col('estimated_delivery_time').isNotNull(),
                    F.round(
                        (F.unix_timestamp(F.current_timestamp()) -
                         F.unix_timestamp('estimated_delivery_time')) / 60
                    )
                ).otherwise(None)
            )

            # 6. Select final columns
            fact_tracking = fact_shipments.select(
                # Surrogate key
                F.monotonically_increasing_id().alias('tracking_key'),

                # Business keys
                F.col('shipment_id'),

                # Foreign keys
                F.col('order_key'),
                F.col('driver_key'),
                F.col('date_key'),

                # Status and tracking
                F.col('status'),
                F.current_timestamp().alias('tracking_timestamp'),
                F.col('current_location'),
                F.col('elapsed_time_minutes')

            )

            # Add ETL metadata
            fact_tracking = fact_tracking.withColumns({
                'etl_timestamp': F.lit(self.current_timestamp),
                'etl_user': F.lit(self.current_user)
            })

            # Validate results
            row_count = fact_tracking.count()
            self.logger.info(
                f"Successfully built shipment tracking fact table with {row_count} rows"
            )

            # Additional validation
            null_keys = fact_tracking.filter(
                F.col('tracking_key').isNull() |
                F.col('order_key').isNull()
            ).count()

            if null_keys > 0:
                self.logger.warning(
                    f"Found {null_keys} rows with null keys in shipment tracking fact"
                )

            fact_tracking.createTempView('fact_shipment_tracking')

            return fact_tracking

        except Exception as e:
            self.logger.error(f"Error building shipment tracking fact table: {str(e)}")
            raise

    def get_build_metadata(self) -> Dict:
        """
        Get metadata about the fact building process

        Returns:
            Dictionary containing build metadata
        """
        return {
            'builder': 'FactBuilder',
            'build_timestamp': self.current_timestamp,
            'build_user': self.current_user,
            'version': '1.0.0'
        }

    def validate_fact_tables(
            self,
            fact_orders: DataFrame,
            fact_shipment_tracking: DataFrame
    ) -> Dict:
        """
        Validate built fact tables

        Args:
            fact_orders: Orders fact table
            fact_shipment_tracking: Shipment tracking fact table

        Returns:
            Dictionary containing validation results
        """
        validation_results = {
            'fact_orders': {
                'total_rows': fact_orders.count(),
                'null_keys': fact_orders.filter(
                    F.col('order_key').isNull() |
                    F.col('date_key').isNull()
                ).count(),
                'has_measures': all(
                    col in fact_orders.columns
                    for col in ['package_weight', 'order_amount', 'delivery_duration_minutes']
                )
            },
            'fact_shipment_tracking': {
                'total_rows': fact_shipment_tracking.count(),
                'null_keys': fact_shipment_tracking.filter(
                    F.col('tracking_key').isNull() |
                    F.col('order_key').isNull()
                ).count(),
                'has_required_columns': all(
                    col in fact_shipment_tracking.columns
                    for col in ['status', 'tracking_timestamp', 'elapsed_time_minutes']
                )
            }
        }

        self.logger.info(f"Fact tables validation results: {validation_results}")
        return validation_results
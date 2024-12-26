from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from typing import Dict, Optional, List
from datetime import datetime
import logging
from pathlib import Path


class DimensionBuilder:
    def __init__(self, spark: SparkSession, config: Dict):
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.current_timestamp = "2024-12-24 10:02:42"
        self.current_user = "thuanpony03"

    def build_date_dimension(self, start_date: str, end_date: str) -> DataFrame:
        """Build date dimension with proper key generation"""
        try:
            self.logger.info(f"Building date dimension from {start_date} to {end_date}")

            # Generate date sequence
            date_df = self.spark.sql(f"""
                SELECT date_add('2024-01-01', pos) as full_date
                FROM (
                    SELECT explode(sequence(0, datediff('{end_date}', '{start_date}'))) as pos
                )
            """)

            # Add date attributes with explicit column names
            date_dim = date_df.select(
                F.date_format('full_date', 'yyyyMMdd').cast('int').alias('date_key'),
                F.col('full_date'),
                F.date_format('full_date', 'EEEE').alias('day_of_week'),
                F.dayofmonth('full_date').alias('day_of_month'),
                F.month('full_date').alias('month'),
                F.quarter('full_date').alias('quarter'),
                F.year('full_date').alias('year'),
                (F.dayofweek('full_date').isin([1, 7])).alias('is_weekend'),
                F.when(
                    F.date_format('full_date', 'MM-dd').isin(['01-01', '12-25']),
                    True
                ).otherwise(False).alias('is_holiday')
            )

            date_dim.createTempView("dim_date")

            self.logger.info(f"Successfully built date dimension with {date_dim.count()} rows")
            return date_dim

        except Exception as e:
            self.logger.error(f"Error building date dimension: {str(e)}")
            raise

    def build_user_dimension(self, users_df: DataFrame) -> DataFrame:
        """Build user dimension with proper surrogate key"""
        try:
            self.logger.info("Building user dimension")

            # Select relevant columns with explicit naming
            user_dim = users_df.select(
                'user_id',
                'full_name',
                'email',
                'phone_number',
                'address',
                'role',
                'created_at'
            )

            # Add SCD Type 2 columns with explicit dates
            user_dim = user_dim.withColumns({
                'is_active': F.lit(True),
                'effective_start_date': F.coalesce(
                    F.col('created_at').cast('date'),
                    F.current_date()
                ),
                'effective_end_date': F.to_date(F.lit('9999-12-31'))
            })

            # Generate surrogate key using user_id
            user_dim = user_dim.withColumn(
                'user_key',
                F.row_number().over(Window.orderBy('user_id'))
            )
            # Reorder columns to place user_key before user_id
            dim_user = user_dim.select(
                'user_key',
                'user_id',
                'full_name',
                'email',
                'phone_number',
                'address',
                'role',
                'created_at',
                'is_active',
                'effective_start_date',
                'effective_end_date'
            )

            dim_user.createTempView("dim_user")

            self.logger.info(f"Successfully built user dimension with {user_dim.count()} rows")
            return user_dim

        except Exception as e:
            self.logger.error(f"Error building user dimension: {str(e)}")
            raise

    def build_driver_dimension(self, drivers_df: DataFrame, user_dim: DataFrame) -> DataFrame:
        """
        Build driver dimension table with user information and SCD Type 2

        Args:
            drivers_df: Source drivers DataFrame
            user_dim: User dimension DataFrame for joining

        Returns:
            Driver dimension DataFrame with surrogate key
        """
        try:
            self.logger.info("Building driver dimension")

            # Join with user dimension to get user details
            driver_dim = drivers_df.join(
                user_dim.select('user_id', 'user_key', 'full_name'),
                'user_id',
                'inner'
            ).select(
                drivers_df.driver_id,
                user_dim.user_key,
                user_dim.full_name,
                'vehicle_license_plate',
                'vehicle_type',
                'vehicle_year'
            )

            # Add SCD Type 2 columns
            driver_dim = driver_dim.withColumns({
                'is_active': F.lit(True),
                'effective_start_date': F.current_date(),
                'effective_end_date': F.to_date(F.lit('9999-12-31'))
            })

            # Generate surrogate key using driver_id instead of 'driver'
            driver_dim = driver_dim.withColumn(
                'driver_key',
                F.row_number().over(
                    Window.orderBy('driver_id')
                )
            )
           # Reorder columns to place driver_key before driver_id
            driver_dim = driver_dim.select(
                'driver_key',
                'driver_id',
                'user_key',
                'full_name',
                'vehicle_license_plate',
                'vehicle_type',
                'vehicle_year',
                'is_active',
                'effective_start_date',
                'effective_end_date'
            )
            driver_dim.createTempView("dim_driver")

            self.logger.info(f"Successfully built driver dimension with {driver_dim.count()} rows")
            return driver_dim

        except Exception as e:
            self.logger.error(f"Error building driver dimension: {str(e)}")
            raise

    def build_location_dimension(self, addresses_df: DataFrame) -> DataFrame:
        """
        Build location dimension table by combining all address sources

        Args:
            addresses_df: DataFrame containing addresses from different sources
                (pickup_address, delivery_address, current_location)

        Returns:
            Location dimension DataFrame
        """
        try:
            self.logger.info("Building location dimension")

            # Get all unique addresses from various sources and union them
            addresses = addresses_df.select(
                F.col('pickup_address').alias('address')
            ).union(
                addresses_df.select(F.col('delivery_address').alias('address'))
            )

            # Deduplicate and clean addresses
            location_dim = addresses.select(
                F.trim(F.col('address')).alias('full_address')
            ).distinct()

            # Add location parsing with null handling
            location_dim = location_dim.withColumns({
                'city': F.when(
                    F.regexp_extract('full_address', r'(?i)(?:,\s*)([^,]+)(?:,\s*[^,]+$)', 1) != '',
                    F.regexp_extract('full_address', r'(?i)(?:,\s*)([^,]+)(?:,\s*[^,]+$)', 1)
                ).otherwise(None),

                'district': F.when(
                    F.regexp_extract('full_address', r'(?i)(?:,\s*)([^,]+)(?:,\s*[^,]+,)', 1) != '',
                    F.regexp_extract('full_address', r'(?i)(?:,\s*)([^,]+)(?:,\s*[^,]+,)', 1)
                ).otherwise(None)
            })

            # Add SCD Type 2 columns
            location_dim = location_dim.withColumns({
                'effective_start_date': F.current_date(),
                'effective_end_date': F.to_date(F.lit('9999-12-31'))
            })

            # Generate surrogate key
            location_dim = location_dim.withColumn(
                'location_key',
                F.row_number().over(Window.orderBy('full_address'))
            )

            # Reorder columns to place location_key before full_address
            location_dim = location_dim.select(
                'location_key',
                'full_address',
                'city',
                'district',
                'effective_start_date',
                'effective_end_date'
            )

            # Validate the results
            row_count = location_dim.count()
            self.logger.info(
                f"Successfully built location dimension with {row_count} unique addresses"
            )

            # Additional validation
            null_addresses = location_dim.filter(F.col('full_address').isNull()).count()
            if null_addresses > 0:
                self.logger.warning(f"Found {null_addresses} null addresses in location dimension")

            location_dim.createTempView("dim_location")

            return location_dim

        except Exception as e:
            self.logger.error(f"Error building location dimension: {str(e)}")
            raise

    def build_payment_method_dimension(self) -> DataFrame:
        """Build payment method dimension with static values"""
        try:
            self.logger.info("Building payment method dimension")

            # Create static payment methods with explicit schema
            payment_methods_data = [
                (1, 'credit_card', 'Card Payment'),
                (2, 'e_wallet', 'Digital Payment'),
                (3, 'bank_transfer', 'Bank Payment')
            ]

            payment_method_dim = self.spark.createDataFrame(
                payment_methods_data,
                schema="""
                    payment_method_key INT,
                    payment_method STRING,
                    payment_method_category STRING
                """
            )

            self.logger.info(
                f"Successfully built payment method dimension with "
                f"{payment_method_dim.count()} rows"
            )
            return payment_method_dim

        except Exception as e:
            self.logger.error(f"Error building payment method dimension: {str(e)}")
            raise

    def validate_dimension(self, dim_df: DataFrame, dim_name: str) -> bool:
        """Validate dimension table quality"""
        try:
            validation_results = {
                'row_count': dim_df.count() > 0,
                'null_keys': dim_df.filter(
                    F.col(f"{dim_name}_key").isNull()
                ).count() == 0,
                'distinct_keys': dim_df.select(
                    f"{dim_name}_key"
                ).distinct().count() == dim_df.count()
            }

            is_valid = all(validation_results.values())

            self.logger.info(
                f"Validation results for {dim_name} dimension:\n"
                f"Valid: {is_valid}\n"
                f"Details: {validation_results}"
            )

            return is_valid

        except Exception as e:
            self.logger.error(f"Error validating {dim_name} dimension: {str(e)}")
            return False

    def get_build_metadata(self) -> Dict:
        """Get metadata about the dimension building process"""
        return {
            'builder': 'DimensionBuilder',
            'build_timestamp': self.current_timestamp,
            'build_user': self.current_user,
            'version': '1.0.0'
        }
"""
Ingest job flow for Mill Profitability data from the Currency Exchange Fixed Length File.

Data Source Description: 
Currency Exchange Fixed Length File
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_process_common.ingest import IngestFixedLengthFile

class IngestCurrencyExchangeRates(IngestFixedLengthFile):

    def filter_df(self, data_frame: DataFrame) -> DataFrame:
        return data_frame.where(F.col("value").startswith("DTL"))

    def pre_processing(self, data_frame: DataFrame) -> DataFrame:
         return data_frame.drop_duplicates()
         
    def process_columns(self, data_frame: DataFrame) -> DataFrame:
        return (
           data_frame
            .withColumn("conversion_rate_multiplier", F.col("conversion_rate_multiplier").substr(1,8).cast(T.IntegerType()) + F.col("conversion_rate_multiplier").substr(9,7).cast(T.DoubleType()) * .0000001)
            .withColumn("conversion_rate_divisor", F.col("conversion_rate_divisor").substr(1,8).cast(T.IntegerType()) + F.col("conversion_rate_divisor").substr(9,7).cast(T.DoubleType()) * .0000001)
            .drop("value")
         )
         
if __name__ == '__main__':

    spark_config = {
        "spark.sql.hive.convertMetastoreOrc": "true",
        "spark.sql.files.ignoreMissingFiles": "true",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.hive.verifyPartitionPath": "false",
        "spark.sql.orc.filterPushdown": "true",
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "hive.exec.dynamic.partition.mode": "nonstrict",
        "hive.exec.dynamic.partition": "true"         
    }

    # Ingest 
    with IngestCurrencyExchangeRates(spark_config=spark_config) as ingest:
        ingest.execute()

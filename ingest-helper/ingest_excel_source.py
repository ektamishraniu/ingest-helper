"""
Ingest job flow for Excel Based Data sources.
"""
from spark_process_common.ingest import IngestExcelFile


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
    with IngestExcelFile(spark_config=spark_config) as ingest:
        ingest.execute()

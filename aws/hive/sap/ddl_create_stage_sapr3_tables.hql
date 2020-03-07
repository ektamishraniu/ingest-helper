DROP TABLE IF EXISTS stage.sapr3_sap_payor;
CREATE TABLE stage.sapr3_sap_payor (
    invoice_id string,
    sap_payor string,
    iptmeta_corrupt_record string,
    iptmeta_extract_dttm timestamp,  
    primary key(invoice_id, sap_payor) disable novalidate)
COMMENT 'staging sap_payor excel sheet'
PARTITIONED BY(iptmeta_record_origin string)
STORED AS ORC
LOCATION 's3://mys3bucket/hive-warehouse/stage.db/sapr3_sap_payor';

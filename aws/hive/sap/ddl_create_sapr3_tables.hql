DROP TABLE IF EXISTS sapr3.sap_payor;
CREATE TABLE sapr3.sap_payor (
    invoice_id string,
    sap_payor string,
    iptmeta_corrupt_record string,
    iptmeta_extract_dttm timestamp,
    primary key(invoice_id, sap_payor) disable novalidate)
COMMENT 'validated sap_payor excel sheet'
STORED AS ORC
LOCATION 's3://mys3bucket/hive-warehouse/sapr3.db/sap_payor';

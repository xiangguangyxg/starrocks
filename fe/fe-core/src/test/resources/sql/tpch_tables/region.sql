CREATE TABLE region (
    r_regionkey     INT NOT NULL,
    r_name          VARCHAR(25) NOT NULL,
    r_comment       VARCHAR(152)
) ENGINE=OLAP
DUPLICATE KEY(`r_regionkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "DEFAULT"
);

-- ========= hive.hql =========
-- Table for MapReduce results
CREATE EXTERNAL TABLE IF NOT EXISTS default.visits_stats (
     park_id STRING,
     visit_date STRING,
     visits_count INT,
     avg_group_size DOUBLE
)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hiveconf:INPUT_DIR3}';

-- Table for Park Dictionary
CREATE EXTERNAL TABLE IF NOT EXISTS default.park_dictionary (
    park_id STRING,
    name STRING,
    region STRING,
    facilities STRING,
    attractions STRING,
    nature_types STRING,
    established_year INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
    WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\""
    )
STORED AS TEXTFILE
LOCATION '${hiveconf:INPUT_DIR4}' -- Parameter from shell script
TBLPROPERTIES ("skip.header.line.count"="1"); -- Skips the header row

-- Ensure reusability
DROP TABLE IF EXISTS default.final_output;

-- Table for final results in json format
CREATE TABLE IF NOT EXISTS default.final_output (
    nature_type STRING,
    region STRING,
    total_visits BIGINT,
    avg_group_size DOUBLE,
    visits_deviation DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${hiveconf:OUTPUT_DIR6}';

-- use CTEs for max efficiency
WITH
RegionStats AS (
    SELECT
        d.nature_types,
        d.region,

        SUM(v.visits_count) AS total_visits,

        SUM(v.visits_count * v.avg_group_size) / SUM(v.visits_count) AS avg_group_size

    FROM
        default.visits_stats v
            JOIN
        default.park_dictionary d ON v.park_id = d.park_id
    GROUP BY
        d.nature_types,
        d.region
),

DeviationStats AS (
    SELECT
        nature_types,
        region,
        total_visits,
        avg_group_size,

        AVG(total_visits) OVER (PARTITION BY nature_types) AS avg_visits_for_type

    FROM
        RegionStats
)

-- save data (redundant overwrite, but better be safe)
INSERT OVERWRITE TABLE default.final_output
SELECT
    nature_types,
    region,
    total_visits,
    avg_group_size,

    (total_visits - avg_visits_for_type) AS visits_deviation
FROM
    DeviationStats;
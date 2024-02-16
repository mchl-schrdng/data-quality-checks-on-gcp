CREATE OR REPLACE PROCEDURE `xxx.xxx..check_not_null`(project_name STRING, dataset_name STRING, table_name STRING, column_name STRING)
BEGIN
    -- Updated the rejection table name to use a consistent prefix for easier querying.
    DECLARE reject_table_name STRING DEFAULT CONCAT('rejected_', table_name);
    DECLARE full_reject_table_name STRING DEFAULT CONCAT(project_name, '.', dataset_name, '.', reject_table_name);
    DECLARE current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    -- SQL statement to create the rejection table if it does not exist, with partitioning and clustering.
    EXECUTE IMMEDIATE FORMAT("""
        CREATE TABLE IF NOT EXISTS `%s` (
            `project_name` STRING, 
            `dataset_name` STRING, 
            `table_name` STRING, 
            `procedure_name` STRING, 
            `tested_column_name` STRING, 
            `rejection_reason` STRING, 
            `rejected_at` TIMESTAMP, 
            `data` STRING
        )
        PARTITION BY DATE(rejected_at)
        CLUSTER BY table_name, procedure_name, tested_column_name
    """, full_reject_table_name);

    -- SQL statement to insert records with NULL values in the specified column into the rejection table.
    EXECUTE IMMEDIATE FORMAT("""
        INSERT INTO `%s` (
            `project_name`, `dataset_name`, `table_name`, `procedure_name`, `tested_column_name`, `rejection_reason`, `rejected_at`, `data`
        ) SELECT 
            @project_name AS project_name, 
            @dataset_name AS dataset_name, 
            @table_name AS table_name, 
            'check_not_null' AS procedure_name, 
            @column_name AS tested_column_name, 
            'NULL value found' AS rejection_reason, 
            @current_timestamp AS rejected_at, 
            TO_JSON_STRING(t) AS data
        FROM `%s.%s.%s` t
        WHERE t.%s IS NULL
    """, full_reject_table_name, project_name, dataset_name, table_name, column_name)
    USING project_name AS project_name, dataset_name AS dataset_name, table_name AS table_name, column_name AS column_name, current_timestamp AS current_timestamp;
END;
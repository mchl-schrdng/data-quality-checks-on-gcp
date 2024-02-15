CREATE OR REPLACE PROCEDURE `project_id.dataset.manage_rejected_records`(
    project_name STRING,
    dataset_name STRING,
    table_name STRING,
    rejected_column_name STRING,
    rejection_reason STRING
)
BEGIN
    DECLARE reject_table_name STRING DEFAULT CONCAT(project_name, '.', dataset_name, '.', table_name, '_rejected');
    
    EXECUTE IMMEDIATE CONCAT(
        'CREATE TABLE IF NOT EXISTS `', reject_table_name, '` ',
        '(project_name STRING, dataset_name STRING, table_name STRING, rejected_column_name STRING, ',
        'rejection_reason STRING, rejected_at TIMESTAMP, data STRING)'
    );
    
    EXECUTE IMMEDIATE CONCAT(
        'INSERT INTO `', reject_table_name, '` ',
        '(project_name, dataset_name, table_name, rejected_column_name, rejection_reason, rejected_at, data) ',
        'SELECT "', project_name, '", "', dataset_name, '", "', table_name, '", "', rejected_column_name, '", "', 
        rejection_reason, '", CURRENT_TIMESTAMP(), TO_JSON_STRING(t) ',
        'FROM `', project_name, '.', dataset_name, '.', table_name, '` t'
    );
END;

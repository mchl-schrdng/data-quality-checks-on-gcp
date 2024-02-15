CREATE OR REPLACE PROCEDURE `project_id.dataset.error_handling`(
    error_project_name STRING,
    error_dataset_name STRING,
    error_table_name STRING,
    error_source STRING,
    error_message STRING
)
BEGIN
    DECLARE full_table_name STRING DEFAULT CONCAT(error_project_name, '.', error_dataset_name, '.', error_table_name);
    
    EXECUTE IMMEDIATE CONCAT(
        'INSERT INTO `', full_table_name, '` (error_source, error_message, error_timestamp) ',
        'VALUES("', error_source, '", "', error_message, '", CURRENT_TIMESTAMP())'
    );
END;

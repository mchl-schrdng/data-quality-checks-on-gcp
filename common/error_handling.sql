CREATE OR REPLACE PROCEDURE `project_id.dataset.error_logging`(
    project_name STRING,
    dataset_name STRING,
    procedure_name STRING,
    error_message STRING
)
BEGIN
    -- Variables to hold the current timestamp.
    DECLARE current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    -- Define the name of the error log table.
    DECLARE error_log_table_name STRING DEFAULT 'error_log';

    -- Attempt to create the error log table if it does not exist.
    EXECUTE IMMEDIATE CONCAT(
        'CREATE TABLE IF NOT EXISTS `', project_name, '`.`, dataset_name, '`.`', error_log_table_name, '` (',
        '`logged_at` TIMESTAMP, ',
        '`procedure_name` STRING, ',
        '`error_message` STRING',
        ')'
    );

    -- Insert the error message into the error log table.
    EXECUTE IMMEDIATE CONCAT(
        'INSERT INTO `', project_name, '`.`, dataset_name, '`.`', error_log_table_name, '` (',
        '`logged_at`, ',
        '`procedure_name`, ',
        '`error_message`',
        ') VALUES (',
        'TIMESTAMP "', CAST(current_timestamp AS STRING), '", ',
        '"', procedure_name, '", ',
        '"', REPLACE(error_message, '"', '\"'), '"',  -- Escape double quotes in the error message.
        ')'
    );
END;
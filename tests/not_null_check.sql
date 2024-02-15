CREATE OR REPLACE PROCEDURE `project_id.dataset.not_null_check`(
    project_name STRING,
    dataset_name STRING,
    table_name STRING,
    column_name STRING
)
BEGIN
    DECLARE rows_with_nulls INT64;
    DECLARE sql STRING;
    
    SET sql = CONCAT(
        'SELECT COUNT(*) FROM `', project_name, '.', dataset_name, '.', table_name, 
        '` WHERE ', column_name, ' IS NULL'
    );
    EXECUTE IMMEDIATE sql INTO rows_with_nulls;

    IF rows_with_nulls > 0 THEN
        CALL `project_id.dataset.manage_rejected_records`(
            project_name,
            dataset_name,
            table_name,
            column_name,
            'NULL value found'
        );
    END IF;
END;

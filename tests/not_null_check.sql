CREATE OR REPLACE PROCEDURE `project_id.dataset.not_null_check`(
    project_name STRING,
    dataset_name STRING,
    table_name STRING,
    column_name STRING,
    procedure_name STRING DEFAULT 'not_null_check',
    delete_rows BOOL DEFAULT FALSE
)
BEGIN
    -- Attempt to identify rows with NULL values in the specified column.
    DECLARE rows_with_nulls INT64;
    SET rows_with_nulls = (
        SELECT COUNT(*)
        FROM `project_id.dataset.table_name`
        WHERE FORMAT("%T", SAFE_CAST(column_name AS STRING)) IS NULL
    );

    -- If there are rows with NULLs, proceed with logging and optional row deletion.
    IF rows_with_nulls > 0 THEN
        -- Call the common procedure to log rejected records.
        CALL `project_id.dataset.manage_rejected_records`(
            project_name,
            dataset_name,
            table_name,
            procedure_name,
            column_name,
            CONCAT('Found ', CAST(rows_with_nulls AS STRING), ' rows with NULLs in column ', column_name),
            delete_rows
        );

        -- Optionally, delete rows with NULLs if specified.
        IF delete_rows THEN
            EXECUTE IMMEDIATE CONCAT(
                'DELETE FROM `', project_name, '`.`, dataset_name, '`.`', table_name, 
                '` WHERE ', column_name, ' IS NULL'
            );
        END IF;
    END IF;
END;
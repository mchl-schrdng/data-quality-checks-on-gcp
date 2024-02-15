-- File: common/manage_rejected_records.sql

CREATE OR REPLACE PROCEDURE `project_id.dataset.manage_rejected_records`(
    project_name STRING,
    dataset_name STRING,
    table_name STRING,
    procedure_name STRING,
    column_name STRING, -- Can be NULL for checks that are not column-specific.
    error_message STRING,
    delete_rows BOOL
)
BEGIN
    -- Variables to hold the current timestamp and the name of the reject table.
    DECLARE current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE reject_table_name STRING DEFAULT CONCAT(table_name, '_rejected');

    -- Create the reject table if it does not exist, with a structure to capture general rejection reasons.
    EXECUTE IMMEDIATE CONCAT(
        'CREATE TABLE IF NOT EXISTS `', project_name, '`.`, dataset_name, '`.`', reject_table_name, '` (',
        '`project_name` STRING, ',
        '`dataset_name` STRING, ',
        '`table_name` STRING, ',
        '`procedure_name` STRING, ',
        '`tested_column_name` STRING, ',
        '`rejected_at` TIMESTAMP, ',
        '`error_message` STRING, ',
        '`data` STRING',
        ')'
    );

    -- Logic to insert rejected records goes here.
    -- This is an example placeholder for how you might structure the dynamic SQL to insert into the reject table.
    -- You'll replace this with specifics depending on the nature of the check (e.g., uniqueness, not null, etc.)
    EXECUTE IMMEDIATE CONCAT(
        'INSERT INTO `', project_name, '`.`, dataset_name, '`.`', reject_table_name, '` (',
        '`project_name`, ',
        '`dataset_name`, ',
        '`table_name`, ',
        '`procedure_name`, ',
        '`tested_column_name`, ',
        '`rejected_at`, ',
        '`error_message`, ',
        '`data`',
        ') SELECT ',
        '''', project_name, ''' AS project_name, ',
        '''', dataset_name, ''' AS dataset_name, ',
        '''', table_name, ''' AS table_name, ',
        '''', procedure_name, ''' AS procedure_name, ',
        '''', column_name, ''' AS tested_column_name, ',
        'TIMESTAMP "', CAST(current_timestamp AS STRING), '" AS rejected_at, ',
        '''', error_message, ''' AS error_message, ',
        'TO_JSON_STRING(t) AS data ',
        'FROM `', project_name, '`.`, dataset_name, '`.`', table_name, '` t ',
        'WHERE <CONDITION_FOR_REJECTION>' -- This condition will vary based on the check being performed.
    );

    -- Optionally, delete rows from the source table if specified.
    IF delete_rows THEN
        EXECUTE IMMEDIATE CONCAT(
            'DELETE FROM `', project_name, '`.`, dataset_name, '`.`', table_name,
            '` WHERE <CONDITION_FOR_DELETION>' -- This condition mirrors the rejection criteria.
        );
    END IF;
END;

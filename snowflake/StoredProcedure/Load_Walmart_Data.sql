CREATE OR REPLACE PROCEDURE WALMART.BRONZE.LOAD_WALMART_DATA()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
BEGIN

DECLARE
  v_file_name STRING;
  v_table_name STRING;
  v_file_path STRING;
  v_md5 STRING;
  v_table_exists BOOLEAN;
  v_file_already_imported BOOLEAN;
  c1 CURSOR for SELECT file_name, table_name, md5 FROM control_table;
  v_column_definitions STRING;
  v_create_stmt STRING;
  v_update_stmt STRING;
  v_log_stmt STRING;
  v_dataload_stmt STRING;
  v_sql TEXT;
  v_count INTEGER;
BEGIN
  
  FOR record IN c1 DO
    v_file_name := record.file_name;
    v_table_name := record.table_name;
    v_md5 := record.md5;
    v_file_path := ''@my_stage/'' || v_file_name;
    -- return v_file_path;

    -- Check if the table exists
    v_table_exists := EXISTS (
      SELECT 1 FROM information_schema.tables
      WHERE table_name = UPPER(:v_table_name)
        AND table_schema = CURRENT_SCHEMA()
    );
    -- RETURN table_exists;

    -- If the table does not exist, create it using inferred schema and add metadata columns
    IF (NOT v_table_exists) THEN
      -- Infer schema and construct column definitions

      SELECT LISTAGG(column_name || '' '' || type, '', '') INTO :v_column_definitions
      FROM TABLE(
        INFER_SCHEMA(
        LOCATION => :v_file_path,
        FILE_FORMAT => ''my_csv_format'',
        IGNORE_CASE => TRUE
        )
      );
      -- Create the table with inferred schema and additional metadata columns
     v_create_stmt := ''CREATE TABLE '' || v_table_name || '' ('' || v_column_definitions || '', ingestion_timestamp TIMESTAMP_NTZ, source_file_name STRING);'';
     -- return create_stmt;
      EXECUTE IMMEDIATE v_create_stmt;
    END IF;
    
      SELECT COUNT(*) INTO v_count
      FROM imported_files
      WHERE file_name = :v_file_name AND md5 = :v_md5;
    
      -- Assign the boolean value based on the count
      v_file_already_imported := v_count > 0;

    -- If the file has not been imported, load data and log the file
    IF (not v_file_already_imported) THEN
      -- Load data into the table with metadata
        v_dataload_stmt := ''COPY INTO '' || v_table_name || '' FROM @my_stage/'' || v_file_name || ''
          FILE_FORMAT = (FORMAT_NAME = ''''my_csv_format'''', ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE)
          MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE'';
        EXECUTE IMMEDIATE v_dataload_stmt;

      
      -- Construct the dynamic SQL statement
        v_update_stmt := ''UPDATE '' || v_table_name || ''
                    SET ingestion_timestamp = CURRENT_TIMESTAMP(),
                        source_file_name = ? 
                    WHERE ingestion_timestamp IS NULL;'';
        EXECUTE IMMEDIATE :v_update_stmt USING (v_file_name);

        
      -- Log the imported file
        v_log_stmt := ''INSERT INTO imported_files (file_name, md5) VALUES (?, ?);'';
        EXECUTE IMMEDIATE :v_log_stmt USING (v_file_name, v_md5);

    END IF;
  END FOR;
END;

RETURN ''Procedure executed successfully.'';
END;
';
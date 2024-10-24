import uuid
import pandas as pd
import redshift_connector

from redshift_loader.utils import (
    list_s3_prefix,
    load_s3_object,
    save_s3_dataframe, 
    apply_columns_mapping)


def dataframe_to_redshift(
        df: pd.DataFrame,
        s3_client,
        s3_copy_bucket: str,
        redshift_host: str,
        redshift_database: str,
        redshift_username: str,
        redshift_password: str,
        redshift_schema: str,
        redshift_table: str,
        redshift_copy_iam_role_arn: str | None = None,
        columns_mapping: dict | None = None,
        deduplication_columns: list | None = None,
        on_duplicate_action: str | None = None
    ) -> None:
    """Upload a DataFrame to a Redshift table.
    
    Args:
        df (pd.DataFrame): DataFrame to be uploaded.
        s3_client (boto3.client): S3 client.
        s3_copy_bucket (str): S3 bucket for generated files 
                              to be used in COPY command.
        redshift_host (str): Redshift host.
        redshift_database (str): Redshift database.
        redshift_username (str): Redshift username.
        redshift_password (str): Redshift password.
        redshift_schema (str): Redshift schema.
        redshift_table (str): Redshift table.
        redshift_copy_iam_role_arn (str, optional): IAM role for 
                                                    COPY command.
        columns_mapping (dict, optional): Columns mapping.
        deduplication_columns (list, optional): Columns to be used in 
                                                deduplication.
        on_duplicate_action (str, optional): Action to be taken in 
                                             case of duplicates
                                             ('ignore', 'overwrite' or
                                             'merge(col1,col2,coln)').
    """
    if not df.empty:
        if columns_mapping:
            df = apply_columns_mapping(df, columns_mapping)

        if deduplication_columns:
            df = df.drop_duplicates(subset=deduplication_columns)

            if not on_duplicate_action:
                on_duplicate_action = 'ignore'

        s3_copy_key = str(uuid.uuid4())
        save_s3_dataframe(s3_client, s3_copy_bucket, s3_copy_key, df)

        try:
            redshift_connection = redshift_connector.connect(
                                    host=redshift_host,
                                    database=redshift_database,
                                    user=redshift_username,
                                    password=redshift_password)
            redshift_cursor = redshift_connection.cursor()

            if not deduplication_columns:
                iam_role_clause = (
                    f"IAM_ROLE '{redshift_copy_iam_role_arn}'" 
                    if redshift_copy_iam_role_arn 
                    else "IAM_ROLE default")
                                
                redshift_cursor.execute(f"""
                    COPY {redshift_schema}.{redshift_table}
                    FROM 's3://{s3_copy_bucket}/{s3_copy_key}'
                    {iam_role_clause}
                    FORMAT AS PARQUET;""")
            else:
                redshift_staging_table = \
                    f"redshift_loader_staging_{uuid.uuid4().hex}"

                redshift_cursor.execute(f"""
                    CREATE TEMP TABLE {redshift_staging_table} 
                    (LIKE {redshift_schema}.{redshift_table});""")

                iam_role_clause = (
                    f"IAM_ROLE '{redshift_copy_iam_role_arn}'" 
                    if redshift_copy_iam_role_arn 
                    else "IAM_ROLE default")
                                
                redshift_cursor.execute(f"""
                    COPY {redshift_staging_table}
                    FROM 's3://{s3_copy_bucket}/{s3_copy_key}'
                    {iam_role_clause}
                    FORMAT AS PARQUET;""")
                
                redshift_cursor.execute("BEGIN;")

                if on_duplicate_action == 'ignore':
                    delete_conditions = ' AND '.join(
                        [f'"{redshift_schema}"."{redshift_table}"."{col}" = '
                         f'{redshift_staging_table}."{col}"'
                         for col in deduplication_columns])
                                    
                    redshift_cursor.execute(f"""
                        DELETE FROM {redshift_staging_table}
                        USING "{redshift_schema}"."{redshift_table}"
                        WHERE {delete_conditions};""")
                    
                    redshift_cursor.execute(f"""
                        INSERT INTO "{redshift_schema}"."{redshift_table}"
                        SELECT * 
                        FROM {redshift_staging_table};""")
                elif (on_duplicate_action == 'overwrite' 
                      or on_duplicate_action.startswith('merge(')):
                    join_conditions = ' AND '.join(
                        [f'"{redshift_schema}"."{redshift_table}"."{col}" = '
                         f'"{redshift_staging_table}"."{col}"'
                         for col in deduplication_columns])
                    
                    columns_to_update = []
                    if on_duplicate_action == 'overwrite':
                        columns_to_update = [
                            col for col in df.columns 
                            if col not in deduplication_columns]
                    elif on_duplicate_action.startswith('merge('):
                        columns_to_update = [
                            col.strip() for col in \
                                on_duplicate_action[6:-1].split(',')
                            if col.strip() not in deduplication_columns]
                    
                    update_set_clause = ', '.join(
                        [f'"{col}" = {redshift_staging_table}.{col}'
                         for col in columns_to_update])
                    
                    insert_columns = ', '.join(
                        [f'{redshift_staging_table}."{col}"'
                         for col in df.columns])
                    
                    redshift_cursor.execute(f"""
                        MERGE INTO "{redshift_schema}"."{redshift_table}"
                        USING {redshift_staging_table}
                        ON {join_conditions}
                        WHEN MATCHED THEN 
                            UPDATE SET {update_set_clause}
                        WHEN NOT MATCHED THEN
                            INSERT VALUES ({insert_columns});""")
                            
            redshift_connection.commit()
        except Exception:
            try:
                if redshift_connection:
                    redshift_connection.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                if redshift_cursor:
                    redshift_cursor.close()
            except Exception:
                pass

            try:
                if redshift_connection:
                    redshift_connection.close()
            except Exception:
                pass

            s3_client.delete_object(Bucket=s3_copy_bucket, 
                                    Key=s3_copy_key)

        print(f"Data uploaded to {redshift_table} in "
              f"{redshift_schema} schema")
    else:
        print("No data to upload")


def s3_objects_to_redshift(
        s3_client,
        s3_bucket: str,
        s3_prefix: str,
        s3_object_type: str,
        redshift_host: str,
        redshift_database: str,
        redshift_username: str,
        redshift_password: str,
        redshift_schema: str,
        redshift_table: str,
        redshift_copy_iam_role_arn: str | None = None,
        new_columns: dict | None = None,
        columns_mapping: dict | None = None,
        deduplication_columns: list | None = None,
        on_duplicate_action: str | None = None,
        s3_copy_bucket: str | None = None
    ) -> None:
    """Upload S3 objects to a Redshift table.

    Args:
        s3_client (boto3.client): S3 client.
        s3_bucket (str): S3 bucket.
        s3_prefix (str): S3 prefix.
        s3_object_type (str): S3 object type ('csv' or 'excel').
        redshift_host (str): Redshift host.
        redshift_database (str): Redshift database.
        redshift_username (str): Redshift username.
        redshift_password (str): Redshift password.
        redshift_schema (str): Redshift schema.
        redshift_table (str): Redshift table.
        redshift_copy_iam_role_arn (str, optional): IAM role for 
                                                    COPY command.
        new_columns (dict, optional): New columns to be added.
        columns_mapping (dict, optional): Columns mapping.
        deduplication_columns (list, optional): Columns to be used in 
                                                deduplication.
        on_duplicate_action (str, optional): Action to be taken in 
                                             case of duplicates 
                                             ('ignore', 'overwrite' or
                                             'merge(col1,col2,coln)').
        s3_copy_bucket (str, optional): S3 bucket for generated files 
                                        to be used in COPY command.
    """
    objects = list_s3_prefix(s3_client, s3_bucket, s3_prefix)
    df_list = []

    for object in objects:
        df = load_s3_object(s3_client, s3_bucket, object, s3_object_type)
        df_list.append(df)

    if df_list:
        df = pd.concat(df_list, ignore_index=True)

        if new_columns:
            for column, value in new_columns.items():
                df[column] = value

        if not s3_copy_bucket:
            s3_copy_bucket = s3_bucket
        
        dataframe_to_redshift(
            df,
            s3_client,
            s3_copy_bucket,
            redshift_host,
            redshift_database,
            redshift_username,
            redshift_password,
            redshift_schema,
            redshift_table,
            redshift_copy_iam_role_arn,
            columns_mapping,
            deduplication_columns,
            on_duplicate_action)
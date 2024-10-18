import uuid
import pandas as pd
import redshift_connector

from redshift_loader.utils import (
    list_s3_prefix, 
    get_s3_xls, 
    save_s3_dataframe, 
    apply_columns_mapping)


def dataframe_to_redshift(
        df,
        s3_client,
        s3_copy_bucket,
        redshift_host,
        redshift_database,
        redshift_username,
        redshift_password,
        redshift_schema,
        redshift_table,
        redshift_copy_iam_role_arn=None,
        columns_mapping=None
    ):
    if not df.empty:
        if columns_mapping:
            df = apply_columns_mapping(df, columns_mapping)

        s3_copy_key = str(uuid.uuid4())
        save_s3_dataframe(s3_client, s3_copy_bucket, s3_copy_key, df)

        try:
            redshift_connection = redshift_connector.connect(
                                    host=redshift_host,
                                    database=redshift_database,
                                    user=redshift_username,
                                    password=redshift_password)
            redshift_cursor = redshift_connection.cursor()

            iam_role_clause = (
                f"IAM_ROLE '{redshift_copy_iam_role_arn}'" 
                if redshift_copy_iam_role_arn 
                else "IAM_ROLE default")
            redshift_cursor.execute(f"COPY {redshift_schema}.{redshift_table} "
                                    f"FROM 's3://{s3_copy_bucket}/{s3_copy_key}' "
                                    f"{iam_role_clause} "
                                    "FORMAT AS PARQUET")
                            
            redshift_connection.commit()
        except Exception:
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


def s3_excel_to_redshift(
        s3_client,
        s3_bucket,
        s3_prefix,
        redshift_host,
        redshift_database,
        redshift_username,
        redshift_password,
        redshift_schema,
        redshift_table,
        redshift_copy_iam_role_arn=None,        
        columns_mapping=None,
        s3_copy_bucket=None
    ):
    files = list_s3_prefix(s3_client, s3_bucket, s3_prefix)
    df_list = []

    for file in files:
        df = get_s3_xls(s3_client, s3_bucket, file)
        df_list.append(df)

    if df_list:
        df = pd.concat(df_list, ignore_index=True)

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
            columns_mapping)
    else:
        print("No files found to process")
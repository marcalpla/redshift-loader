import pandas as pd
import gzip

from io import BytesIO


def list_s3_prefix(s3_client, bucket_name, prefix):
    response = s3_client.list_objects_v2(
                            Bucket=bucket_name, 
                            Prefix=prefix)
    if "Contents" in response:
        files = [content["Key"] for content in response["Contents"]]
    else:
        files = []
    return files


def get_s3_xls(s3_client, bucket_name, key):
    df = pd.DataFrame()

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        file_content = response["Body"].read()

        if key.endswith(".gz"):
            file_content = gzip.decompress(file_content)

        df = pd.read_excel(BytesIO(file_content))
    except Exception as e:
        print(f"Error reading {key}: {e}")
        
    return df


def save_s3_dataframe(s3_client, bucket_name, key, df):
    bytestream = BytesIO()
    df.to_parquet(bytestream, index=False,
                  engine='pyarrow', coerce_timestamps='us')
    bytestream.seek(0)
    s3_client.put_object(Bucket=bucket_name, Key=key, 
                         Body=bytestream.getvalue())


def apply_columns_mapping(df, columns_mapping):
    new_columns = {}

    for column in df.columns:
        if column in columns_mapping:
            redshift_column = columns_mapping[column]. \
                                get('redshift_column', column)
            column_type = columns_mapping[column].get('type')
            date_format = columns_mapping[column].get('date_format')

            if column_type in ['DATE', 'TIMESTAMP']:
                df[column] = pd.to_datetime(df[column], 
                                            format=date_format, 
                                            errors='coerce')

            elif column_type in ['INT', 'SMALLINT', 'BIGINT']:
                df[column] = pd.to_numeric(df[column], errors='coerce', 
                                           downcast='integer')

            elif column_type in ['FLOAT', 'DOUBLE', 'DECIMAL']:
                df[column] = pd.to_numeric(df[column], errors='coerce', 
                                           downcast='float')

            elif column_type == 'BOOLEAN':
                df[column] = df[column].astype(str).str.lower().map({
                    'true': True, 'false': False, '1': True, '0': False
                })

            elif column_type == 'VARCHAR':
                df[column] = df[column].fillna('').astype(str)

            new_columns[column] = redshift_column
        else:
            new_columns[column] = column

    df = df.rename(columns=new_columns)

    return df
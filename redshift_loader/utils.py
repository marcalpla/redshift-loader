import pandas as pd
import pyarrow as pa
import gzip
import chardet

from io import BytesIO


def list_s3_prefix(s3_client, bucket_name: str, prefix: str) -> list:
    """List all objects in a S3 bucket with a given prefix.

    Args:
        s3_client (boto3.client): S3 client.
        bucket_name (str): S3 bucket name.
        prefix (str): Prefix to filter objects.

    Returns:
        list: List of objects in the S3 bucket with the given prefix.    
    """
    response = s3_client.list_objects_v2(
                            Bucket=bucket_name, 
                            Prefix=prefix)
    if "Contents" in response:
        objects = [content["Key"] for content in response["Contents"]]
    else:
        objects = []
    return objects


def load_s3_object(s3_client, bucket_name: str, 
                   key: str, type: str) -> pd.DataFrame:
    """Load a S3 object into a pandas DataFrame.

    Args:
        s3_client (boto3.client): S3 client.
        bucket_name (str): S3 bucket name.
        key (str): S3 object key.
        type (str): Type of the object ('csv' or 'excel').

    Returns:
        pd.DataFrame: DataFrame with the content of the S3 object.
    """
    df = pd.DataFrame()

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        object_content = response["Body"].read()

        if key.endswith(".gz"):
            object_content = gzip.decompress(object_content)

        if type == 'csv':
            df = pd.read_csv(BytesIO(object_content), 
                             encoding=chardet.detect(
                                        object_content)['encoding'])
        elif type == 'excel':
            df = pd.read_excel(BytesIO(object_content))
    except Exception as e:
        print(f"Error reading {key}: {e}")

    return df 


def save_s3_dataframe(s3_client, bucket_name: str, 
                      key: str, df: pd.DataFrame) -> None:
    """Save a DataFrame in a S3 object.

    Args:
        s3_client (boto3.client): S3 client.
        bucket_name (str): S3 bucket name.
        key (str): S3 object key.
        df (pd.DataFrame): DataFrame to be saved.
    """
    bytestream = BytesIO()
    df.to_parquet(bytestream, index=False,
                  engine='pyarrow', coerce_timestamps='us')
    bytestream.seek(0)
    s3_client.put_object(Bucket=bucket_name, Key=key, 
                         Body=bytestream.getvalue())


def apply_columns_mapping(df: pd.DataFrame, 
                          columns_mapping: dict) -> pd.DataFrame:
    """Apply column mapping to a DataFrame.

    Args:
        df (pd.DataFrame): DataFrame to be mapped.
        columns_mapping (dict): Mapping of columns.

    Returns:
        pd.DataFrame: DataFrame with columns mapped.
    """
    new_columns = {}

    for column in df.columns:
        if column in columns_mapping:
            target_column = columns_mapping[column]. \
                                get('target_column', column)
            target_type = columns_mapping[column].get('target_type')
            source_timestamp_format = columns_mapping[column].get(
                                            'source_timestamp_format')
            
            if target_type == 'DATE':
                df[column] = pd.to_datetime(df[column], 
                                format=source_timestamp_format).dt.date
            elif target_type == 'TIMESTAMP':
                df[column] = pd.to_datetime(df[column], 
                                        format=source_timestamp_format)
                
            elif target_type == 'INT':
                df[column] = df[column].astype('Int32')
            elif target_type == 'SMALLINT':
                df[column] = df[column].astype('Int16')
            elif target_type == 'BIGINT':
                df[column] = df[column].astype('Int64')

            elif target_type == 'REAL':
                df[column] = df[column].astype('Float32')
            elif target_type == 'DOUBLE PRECISION':
                df[column] = df[column].astype('Float64')

            elif target_type.startswith('DECIMAL('):
                precision = int(target_type.split('(')[1].split(',')[0])
                scale = int(target_type.split(',')[1].split(')')[0])
                df[column] = df[column].astype(pd.ArrowDtype(
                                                pa.decimal128(
                                                    precision, 
                                                    scale)))
                                    
            elif target_type == 'BOOLEAN':
                df[column] = df[column].astype(str).str.lower().map({
                    'true': True, 'false': False, '1': True, '0': False
                })

            elif target_type == 'VARCHAR':
                df[column] = df[column].astype(str).where(
                                    df[column].notna(),
                                    None)

            new_columns[column] = target_column
        else:
            new_columns[column] = column

    df = df.rename(columns=new_columns)

    return df
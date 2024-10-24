import os
import click
import boto3
import json

from redshift_loader.loader import s3_objects_to_redshift


@click.command()
@click.option(
    '--s3_bucket', 
    type=str, 
    required=True, 
    help='S3 bucket containing the files')
@click.option(
    '--s3_prefix', 
    type=str, 
    required=True, 
    help='Prefix for the files in S3')
@click.option(
    '--s3_object_type',
    type=str,
    required=True,
    help='Type of the S3 object (csv or excel)')
@click.option(
    '--redshift_host', 
    type=str, 
    required=True, 
    help='Host for Redshift connection')
@click.option(
    '--redshift_database', 
    type=str, 
    required=True, 
    help='Database for Redshift connection')
@click.option(
    '--redshift_username', 
    type=str, 
    required=True, 
    help='Username for Redshift authentication')
@click.option(
    '--redshift_password', 
    type=str, 
    required=True, 
    help='Password for Redshift authentication')
@click.option(
    '--redshift_schema', 
    type=str, 
    required=True, 
    help='Target schema in Redshift')
@click.option(
    '--redshift_table', 
    type=str, 
    required=True, 
    help='Target table in Redshift')
@click.option(
    '--redshift_copy_iam_role_arn',
    type=str,
    required=False,
    help='IAM role ARN for Redshift COPY command. If not specified, '
        'default IAM role will be used')
@click.option(
    '--columns_mapping_file',
    type=str, 
    required=False, 
    help='JSON file in the columns_mappings folder with the columns '
        'mapping')
@click.option(
    '--deduplication_columns',
    type=str,
    required=False,
    help='Columns to be used in deduplication, separated by commas. '
        'If not specified, no deduplication will be performed')
@click.option(
    '--on_duplicate_action',
    type=str,
    required=False,
    help='Action to be taken in case of duplicates (ignore, '
         'overwrite or merge(col1, col2, ...)). Only takes effect if '
         'deduplication_columns is specified. If not specified, '
         'ignore will be used')
@click.option(
    '--s3_copy_bucket',
    type=str,
    required=False,
    help='S3 bucket for generated files to be used in COPY command. '
        'If not specified, s3_bucket will be used')
def main(
        s3_bucket, 
        s3_prefix,
        s3_object_type,
        redshift_host,
        redshift_database,
        redshift_username, 
        redshift_password, 
        redshift_schema, 
        redshift_table,
        redshift_copy_iam_role_arn,
        columns_mapping_file,
        deduplication_columns,
        on_duplicate_action,
        s3_copy_bucket
    ):
    columns_mapping = {}
    if columns_mapping_file:
        base_path = os.path.dirname(os.path.dirname(
                                        os.path.abspath(__file__)))
        columns_mapping_path = os.path.join(base_path, 
                                            'columns_mappings', 
                                            columns_mapping_file)
        with open(columns_mapping_path, 'r') as file:
            columns_mapping = json.load(file)

    deduplication_columns_list = [
        col.strip() for col in deduplication_columns.split(',')
    ] if deduplication_columns else None

    s3_client = boto3.client('s3')            

    s3_objects_to_redshift(
        s3_client,
        s3_bucket,
        s3_prefix,
        s3_object_type,
        redshift_host,
        redshift_database,
        redshift_username,
        redshift_password,
        redshift_schema,
        redshift_table,
        redshift_copy_iam_role_arn=redshift_copy_iam_role_arn,
        columns_mapping=columns_mapping,
        deduplication_columns=deduplication_columns_list,
        on_duplicate_action=on_duplicate_action,
        s3_copy_bucket=s3_copy_bucket)


if __name__ == '__main__':
    main()

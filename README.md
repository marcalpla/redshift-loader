# Redshift Loader

A Python package for loading data into Redshift.

## Installation

```bash
poetry add git+https://github.com/user/redshift-loader.git
```
or
```bash
pip install git+https://github.com/user/redshift-loader.git
```

## Usage

### Functions

#### `dataframe_to_redshift`

This function uploads a Pandas DataFrame to a Redshift table using the `COPY` command. The data is first uploaded temporarily to an S3 bucket and then copied into Redshift. It is important to ensure that your AWS setup has the necessary permissions:

- The function needs permission to write data to the specified S3 bucket.
- Redshift must be able to access the S3 bucket to perform the `COPY` operation.

After the data is copied to Redshift, the temporary file in S3 is deleted.

```python
from redshift_loader.loader import dataframe_to_redshift

dataframe_to_redshift(
    df,
    s3_client,
    's3_copy_bucket',
    'redshift_host',
    'redshift_database',
    'redshift_username',
    'redshift_password',
    'redshift_schema',
    'redshift_table',
    'optional_iam_role_arn_for_copy',
    optional_columns_mapping_dict,
    optional_deduplication_columns_list,
    'optional_on_duplicate_action'
)
```

#### `s3_objects_to_redshift`

This function processes multiple objects (CSV or Excel files) stored in an S3 bucket and loads them into Redshift. The files are combined into a single DataFrame, which is then uploaded temporarily to an S3 bucket and copied into Redshift. The same permissions are required as mentioned above, and the temporary file in S3 is deleted after the `COPY` operation.

```python
from redshift_loader.loader import s3_objects_to_redshift

s3_objects_to_redshift(
    s3_client,
    's3_bucket',
    's3_prefix',
    's3_object_type', # 'csv' or 'excel'
    'redshift_host',
    'redshift_database',
    'redshift_username',
    'redshift_password',
    'redshift_schema',
    'redshift_table',
    'optional_iam_role_arn_for_copy',
    optional_new_columns_dict,
    optional_columns_mapping_dict,        
    optional_deduplication_columns_list,
    'optional_on_duplicate_action',
    'optional_s3_copy_bucket'
)
```

### CLI

Also a CLI is available to load data into Redshift, although currently it only supports the functionality given by `s3_objects_to_redshift`. An example of usage is available in the `presets` directory.

### JSON Columns Mapping

You can define a custom column mapping either as a dictionary passed directly to the functions or in a JSON file located in the `columns_mappings` directory for use with the CLI. An example is provided in the `columns_mappings` directory.
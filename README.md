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
    optinal_columns_mapping_dict
)
```

```python
from redshift_loader.loader import s3_excel_to_redshift

s3_excel_to_redshift(
    s3_client,
    's3_bucket',
    's3_prefix',
    'redshift_host',
    'redshift_database',
    'redshift_username',
    'redshift_password',
    'redshift_schema',
    'redshift_table',
    'optional_iam_role_arn_for_copy',        
    optinal_columns_mapping_dict,
    'optional_s3_copy_bucket'
)
```

### CLI

Also a CLI is available to load data into Redshift, although currently it only supports the functionality given by `s3_excel_to_redshift`. An example of usage is available in the `presets` directory.

### JSON Columns Mapping

You can define a custom column mapping either as a dictionary passed directly to the functions or in a JSON file located in the `columns_mappings` directory for use with the CLI. An example is provided in the `columns_mappings` directory.
#!/bin/bash

cd $(dirname "$(realpath "$0")")/..
poetry run redshift-loader --s3_bucket bucket_name --s3_prefix prefix/of/path/containing/data/files --redshift_host name.account_id.region.redshift-serverless.amazonaws.com --redshift_database database --redshift_username user --redshift_password password --redshift_schema schema --redshift_table table --columns_mapping_file example.json --s3_copy_bucket bucket_for_temporary_files
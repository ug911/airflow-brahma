# Athena
Query Engine to run queries directly over files stored in s3

### Running a Query
```sh
python3 athena_connect.py --query_file <Query File> --database <Database> --s3_output <temp s3 location> --final_s3_path <final s3 location> --wait 1 (default is 1)
```

### References
Athena can be accessed using [https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html]

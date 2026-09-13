import dagster as dg
from dagster_aws.s3 import S3Resource



@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(resources={
        "s3":S3Resource(
            endpoint_url="http://localhost:9000",
            aws_access_key_id="admin",
            aws_secret_access_key="password123",
            
        )
    })

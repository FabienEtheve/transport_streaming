import io
import zipfile

import dagster as dg
import requests
from dagster_aws.s3 import S3Resource


@dg.asset
def gtfs_static(context: dg.AssetExecutionContext, s3: S3Resource):
    URL = "https://www.data.gouv.fr/api/1/datasets/r/c087ca61-f4db-44b2-bdc1-ec221c2bf762"
    BUCKET = "landing"

    minio = s3.get_client()

    response = requests.get(URL, timeout=60)
    response.raise_for_status()

    uploaded = []
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        for name in z.namelist():
            if name.endswith("/"):
                continue
            with z.open(name) as f:
                minio.put_object(
                    Bucket=BUCKET,
                    Key=f"gtfs_static/{name}",
                    Body=f.read(),
                )
            uploaded.append(name)

    context.log.info(f"{len(uploaded)} fichiers uploadés dans {BUCKET}")
    return dg.MaterializeResult(metadata={"files": uploaded, "count": len(uploaded)})
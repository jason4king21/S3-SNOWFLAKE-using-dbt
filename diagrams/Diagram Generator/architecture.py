from diagrams import Cluster, Diagram
from diagrams.aws.storage import S3
from diagrams.aws.network import Privatelink
from diagrams.onprem.analytics import Dbt
from diagrams.generic.database import SQL
from diagrams.saas.analytics import Snowflake
from diagrams.programming.language import Python



with Diagram("SCD2 Architecture with Snowflake and DBT", show=False, direction="LR", filename="diagrams/architecture", outformat="png"):
    with Cluster("Data Ingestion"):
        s3 = S3("AWS S3 Bucket\n(raw data)")
        privatelink = Privatelink("AWS PrivateLink")
        python = Python("Python")
        s3 >> privatelink

    with Cluster("Data Transformation and Warehousing"):
        with Cluster("Snowflake VPC"):
            stage = Snowflake("External Stage")
            bronze = SQL("Bronze Layer\n(Copy Table)")
            silver = SQL("Silver Layer\n(Transform Table)")
            snapshot = Dbt("DBT Snapshot")
            gold = SQL("Gold Layer\n(View)")

        privatelink >> stage >> bronze >> silver
        snapshot >> silver >> gold

        dbt_cloud = Dbt("DBT Cloud")

    python >> s3
    dbt_cloud >> snapshot
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 Customer Curated
S3CustomerCurated_node1696426280652 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-mpl/curated_zone/customer/"],
        "recurse": True,
    },
    transformation_ctx="S3CustomerCurated_node1696426280652",
)

# Script generated for node S3 Step-trainer landing
S3Steptrainerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-mpl/landing_zone/step-trainer/"],
        "recurse": True,
    },
    transformation_ctx="S3Steptrainerlanding_node1",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1696426418057 = ApplyMapping.apply(
    frame=S3CustomerCurated_node1696426280652,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("z", "double", "right_z", "double"),
        ("birthDay", "string", "right_birthDay", "string"),
        ("timeStamp", "bigint", "timeStamp", "long"),
        ("shareWithPublicAsOfDate", "bigint", "shareWithPublicAsOfDate", "long"),
        ("shareWithResearchAsOfDate", "bigint", "shareWithResearchAsOfDate", "long"),
        ("registrationDate", "bigint", "registrationDate", "long"),
        ("customerName", "string", "right_customerName", "string"),
        ("user", "string", "right_user", "string"),
        ("y", "double", "right_y", "double"),
        ("x", "double", "right_x", "double"),
        ("email", "string", "right_email", "string"),
        ("lastUpdateDate", "bigint", "lastUpdateDate", "long"),
        ("phone", "string", "right_phone", "string"),
        ("shareWithFriendsAsOfDate", "bigint", "shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1696426418057",
)

# Script generated for node Join
Join_node1696426408578 = Join.apply(
    frame1=S3Steptrainerlanding_node1,
    frame2=RenamedkeysforJoin_node1696426418057,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Join_node1696426408578",
)

# Script generated for node Drop Fields
DropFields_node1696426700791 = DropFields.apply(
    frame=Join_node1696426408578,
    paths=[
        "right_serialNumber",
        "right_birthDay",
        "right_customerName",
        "right_email",
        "right_phone",
    ],
    transformation_ctx="DropFields_node1696426700791",
)

# Script generated for node S3 bucket step-trainer
S3bucketsteptrainer_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1696426700791,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-mpl/trusted_zone/step-trainer/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucketsteptrainer_node2",
)

job.commit()

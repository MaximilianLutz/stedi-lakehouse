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

# Script generated for node Customer Trusted
CustomerTrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-mpl/trusted_zone/customer/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1695899414060 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-mpl/landing_zone/accelerometer/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1695899414060",
)

# Script generated for node Join
Join_node1695899466071 = Join.apply(
    frame1=AccelerometerLanding_node1695899414060,
    frame2=CustomerTrusted_node1,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1695899466071",
)

# Script generated for node Drop Fields
DropFields_node1695899629536 = DropFields.apply(
    frame=Join_node1695899466071,
    paths=[
        "shareWithPublicAsOfDate",
        "serialNumber",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1695899629536",
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1695899629536,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-mpl/trusted_zone/accelerometer/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node2",
)

job.commit()

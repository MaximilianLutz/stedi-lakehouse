import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 Customer
S3Customer_node1696414378492 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-mpl/trusted_zone/customer/"],
        "recurse": True,
    },
    transformation_ctx="S3Customer_node1696414378492",
)

# Script generated for node S3 Accelerometer
S3Accelerometer_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-mpl/trusted_zone/accelerometer/"],
        "recurse": True,
    },
    transformation_ctx="S3Accelerometer_node1",
)

# Script generated for node Join
Join_node1696414412619 = Join.apply(
    frame1=S3Customer_node1696414378492,
    frame2=S3Accelerometer_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1696414412619",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1696425589285 = DynamicFrame.fromDF(
    Join_node1696414412619.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1696425589285",
)

# Script generated for node S3 bucket
S3bucket_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1696425589285,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-mpl/curated_zone/customer/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node2",
)

job.commit()

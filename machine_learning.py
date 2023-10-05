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

# Script generated for node S3 Accellerometer Trusted
S3AccellerometerTrusted_node1696426280652 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://stedi-lakehouse-mpl/trusted_zone/accelerometer/"],
            "recurse": True,
        },
        transformation_ctx="S3AccellerometerTrusted_node1696426280652",
    )
)

# Script generated for node S3 Step-trainer trusted
S3Steptrainertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-mpl/trusted_zone/step-trainer/"],
        "recurse": True,
    },
    transformation_ctx="S3Steptrainertrusted_node1",
)

# Script generated for node S3 Customer Curated
S3CustomerCurated_node1696427179905 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lakehouse-mpl/curated_zone/customer/"],
        "recurse": True,
    },
    transformation_ctx="S3CustomerCurated_node1696427179905",
)

# Script generated for node Renamed keys for Second Join
RenamedkeysforSecondJoin_node1696507841146 = ApplyMapping.apply(
    frame=S3AccellerometerTrusted_node1696426280652,
    mappings=[
        ("z", "double", "accellerometer_z", "double"),
        ("timeStamp", "bigint", "accellerometer_timeStamp", "bigint"),
        ("user", "string", "accellerometer_user", "string"),
        ("y", "double", "accellerometer_y", "double"),
        ("x", "double", "accellerometer_x", "double"),
    ],
    transformation_ctx="RenamedkeysforSecondJoin_node1696507841146",
)

# Script generated for node Renamed keys for First Join
RenamedkeysforFirstJoin_node1696507650820 = ApplyMapping.apply(
    frame=S3CustomerCurated_node1696427179905,
    mappings=[
        ("serialNumber", "string", "customer_serialNumber", "string"),
        ("z", "double", "customer_z", "double"),
        ("birthDay", "string", "customer_birthDay", "string"),
        ("timeStamp", "bigint", "customer_timeStamp", "bigint"),
        (
            "shareWithPublicAsOfDate",
            "bigint",
            "customer_shareWithPublicAsOfDate",
            "bigint",
        ),
        (
            "shareWithResearchAsOfDate",
            "bigint",
            "customer_shareWithResearchAsOfDate",
            "bigint",
        ),
        ("registrationDate", "bigint", "customer_registrationDate", "bigint"),
        ("customerName", "string", "customer_customerName", "string"),
        ("user", "string", "customer_user", "string"),
        ("y", "double", "customer_y", "double"),
        ("x", "double", "customer_x", "double"),
        ("email", "string", "customer_email", "string"),
        ("lastUpdateDate", "bigint", "customer_lastUpdateDate", "bigint"),
        ("phone", "string", "customer_phone", "string"),
        (
            "shareWithFriendsAsOfDate",
            "bigint",
            "customer_shareWithFriendsAsOfDate",
            "bigint",
        ),
    ],
    transformation_ctx="RenamedkeysforFirstJoin_node1696507650820",
)

# Script generated for node First Join
FirstJoin_node1696426408578 = Join.apply(
    frame1=S3Steptrainertrusted_node1,
    frame2=RenamedkeysforFirstJoin_node1696507650820,
    keys1=["serialNumber"],
    keys2=["customer_serialNumber"],
    transformation_ctx="FirstJoin_node1696426408578",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1696490436575 = DynamicFrame.fromDF(
    FirstJoin_node1696426408578.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1696490436575",
)

# Script generated for node Second Join
SecondJoin_node1696427279492 = Join.apply(
    frame1=DropDuplicates_node1696490436575,
    frame2=RenamedkeysforSecondJoin_node1696507841146,
    keys1=["sensorReadingTime", "customer_email"],
    keys2=["accellerometer_user", "accellerometer_user"],
    transformation_ctx="SecondJoin_node1696427279492",
)

# Script generated for node Drop Fields
DropFields_node1696490625180 = DropFields.apply(
    frame=SecondJoin_node1696427279492,
    paths=[
        "customer_serialNumber",
        "customer_birthDay",
        "customer_customerName",
        "customer_phone",
        "customer_email",
        "customer_shareWithPublicAsOfDate",
        "customer_shareWithFriendsAsOfDate",
        "customer_lastUpdateDate",
    ],
    transformation_ctx="DropFields_node1696490625180",
)

# Script generated for node S3 bucket step-trainer
S3bucketsteptrainer_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1696490625180,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lakehouse-mpl/curated_zone/machine_learning/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucketsteptrainer_node2",
)

job.commit()

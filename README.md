
Efficient Data Ingestion with Glue Concurrency: Using a Single Template for Multiple S3 Tables into a Transactional Hudi Data Lake
![Capture](https://user-images.githubusercontent.com/39345855/231465693-a9c13123-0c8e-41fc-88ff-e76b175c69b2.JPG)

* Managing a data lake with multiple tables can be challenging, especially when it comes to writing ETL or Glue jobs for each table. That's why I'm excited to share my upcoming video on the templated approach for managing ETL jobs in a data lake. By creating a single job that can be used for multiple tables, you can save time and reduce the amount of infrastructure code needed to manage your data lake. Join me to learn more about how you can templatize your code and run the same job for multiple tables, making it easier to scale and manage your data lake

#### Benifits 
* Reduces the amount of infrastructure code needed to manage the data lake
* Saves time by allowing you to reuse the same job code for multiple tables
* Reduces the risk of errors and inconsistencies that can occur when manually creating separate jobs for each table
* Makes it easier to scale your data lake by adding new tables without having to create new ETL jobs from scratch
* Allows for custom configuration settings for each table using parameters
* Simplifies the ETL process by automating repetitive tasks and reducing manual effort
* Provides a more streamlined and efficient way of managing a large number of tables in a data lake
* Enables the creation of a Hudi transactional data lake, providing more robust and scalable data management capabilities.

# Labs 

## Step 1: Create S3 Bucket and Generate multiple Tables with Script given to you 

```
try:
    import datetime
    import json
    import random
    import boto3
    import os
    import uuid
    import time
    from datetime import datetime
    from faker import Faker
    from dotenv import load_dotenv

    load_dotenv(".env")
except Exception as e:
    pass


class AWSS3(object):
    """Helper class to which add functionality on top of boto3 """

    def __init__(self, bucket, aws_access_key_id, aws_secret_access_key, region_name):

        self.BucketName = bucket
        self.client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )

    def put_files(self, Response=None, Key=None):
        """
        Put the File on S3
        :return: Bool
        """
        try:

            response = self.client.put_object(
                ACL="private", Body=Response, Bucket=self.BucketName, Key=Key
            )
            return "ok"
        except Exception as e:
            print("Error : {} ".format(e))
            return "error"

    def item_exists(self, Key):
        """Given key check if the items exists on AWS S3 """
        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return True
        except Exception as e:
            return False

    def get_item(self, Key):

        """Gets the Bytes Data from AWS S3 """

        try:
            response_new = self.client.get_object(Bucket=self.BucketName, Key=str(Key))
            return response_new["Body"].read()

        except Exception as e:
            print("Error :{}".format(e))
            return False

    def find_one_update(self, data=None, key=None):

        """
        This checks if Key is on S3 if it is return the data from s3
        else store on s3 and return it
        """

        flag = self.item_exists(Key=key)

        if flag:
            data = self.get_item(Key=key)
            return data

        else:
            self.put_files(Key=key, Response=data)
            return data

    def delete_object(self, Key):

        response = self.client.delete_object(Bucket=self.BucketName, Key=Key, )
        return response

    def get_all_keys(self, Prefix=""):

        """
        :param Prefix: Prefix string
        :return: Keys List
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.BucketName, Prefix=Prefix)

            tmp = []

            for page in pages:
                for obj in page["Contents"]:
                    tmp.append(obj["Key"])

            return tmp
        except Exception as e:
            return []

    def print_tree(self):
        keys = self.get_all_keys()
        for key in keys:
            print(key)
        return None

    def find_one_similar_key(self, searchTerm=""):
        keys = self.get_all_keys()
        return [key for key in keys if re.search(searchTerm, key)]

    def __repr__(self):
        return "AWS S3 Helper class "


global faker
global helper

faker = Faker()
helper = AWSS3(
    aws_access_key_id=os.getenv("DEV_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("DEV_SECRET_KEY"),
    region_name=os.getenv("DEV_REGION"),
    bucket=os.getenv("BUCKET")
)


def run():
    for i in range(1,100):

        order_id = uuid.uuid4().__str__()
        customer_id = uuid.uuid4().__str__()
        partition_key = uuid.uuid4().__str__()

        orders = {
            "orderid": order_id,
            "customer_id": customer_id,
            "ts": datetime.now().isoformat().__str__(),
            "order_value": random.randint(10, 1000).__str__(),
            "priority": random.choice(["LOW", "MEDIUM", "URGENT"])
        }
        helper.put_files(Response=json.dumps(orders), Key='raw/orders/{}.json'.format(uuid.uuid4().__str__()))

        customers = {
            "customer_id": customer_id,
            "name": faker.name(),
            "state": faker.state(),
            "city": faker.city(),
            "email": faker.email(),
            "ts": datetime.now().isoformat().__str__()
        }
        helper.put_files(Response=json.dumps(customers), Key='raw/customers/{}.json'.format(uuid.uuid4().__str__()))


if __name__ == "__main__":
    run()

```

## Step 2: Create Glue job and upload this template 
```
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(
    sys.argv, ['JOB_NAME',
               'SOURCE_S3_PATH',
               'GLUE_DATABASE', 'GLUE_TABLE_NAME', 'HUDI_PRECOMB_KEY', 'HUDI_RECORD_KEY',
               'TARGET_S3_PATH']
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": [args['SOURCE_S3_PATH']],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node S3 bucket
additional_options = {
    "hoodie.table.name": "customers",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.recordkey.field": args['HUDI_RECORD_KEY'],
    "hoodie.datasource.write.precombine.field": args['HUDI_PRECOMB_KEY'],
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.parquet.compression.codec": "gzip",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": args['GLUE_DATABASE'],
    "hoodie.datasource.hive_sync.table": args['GLUE_TABLE_NAME'],
    "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.mode": "hms",
}
S3bucket_node3_df = S3bucket_node1.toDF()
S3bucket_node3_df.write.format("hudi").options(**additional_options).mode(
    "append"
).save(args['TARGET_S3_PATH'])

job.commit()

```

# Step 3: Make sure to set concureecny on Glue to 4
![image](https://user-images.githubusercontent.com/39345855/231466430-27d9290c-84c0-4df9-972f-af77756e63eb.png)


# Step 4 : Fire Jobs 

```
try:
    import datetime
    import json
    import random
    import boto3
    import os
    import uuid
    import time
    from datetime import datetime
    from faker import Faker
    from dotenv import load_dotenv

    load_dotenv(".env")
except Exception as e:
    pass

global helper

glue = boto3.client(
    "glue",
    aws_access_key_id=os.getenv("DEV_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("DEV_SECRET_KEY"),
    region_name=os.getenv("DEV_REGION"),
)

payloads = [
    {
        'JOB_NAME': 'customers',
        'SOURCE_S3_PATH': 's3://delta-streamer-demo-hudi/raw/customers/',
        'GLUE_DATABASE': 'hudi_db',
        'GLUE_TABLE_NAME': 'customers',
        'HUDI_PRECOMB_KEY': 'ts',
        'HUDI_RECORD_KEY': 'customer_id',
        'TARGET_S3_PATH': 's3://delta-streamer-demo-hudi/hudi/customers/'
    },
    {
        'JOB_NAME': 'orders',
        'SOURCE_S3_PATH': 's3://delta-streamer-demo-hudi/raw/orders/',
        'GLUE_DATABASE': 'hudi_db',
        'GLUE_TABLE_NAME': 'orders',
        'HUDI_PRECOMB_KEY': 'ts',
        'HUDI_RECORD_KEY': 'orderid',
        'TARGET_S3_PATH': 's3://delta-streamer-demo-hudi/hudi/orders/'
    }

]

for payload in payloads:
    job_name = 'hudi-template-ingestion-s3'

    fire_payload = {}
    for key, value in payload.items(): fire_payload[f"--{key}"] = value

    response = glue.start_job_run(
        JobName=job_name,
        Arguments=fire_payload
    )
    print(response)

```


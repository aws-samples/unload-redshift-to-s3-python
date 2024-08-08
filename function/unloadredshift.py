import ast
import boto3
import psycopg2
import base64
import datetime
from botocore.exceptions import ClientError


def lambda_handler(event, context):
    sts_client = boto3.client('sts')

    # Secrets Manager Configurations
    secret_name = "redshift_creds"
    sm_region = "eu-west-1"
    sm_read_role = "arn:aws:iam::PROD_ACCOUNT_NUMBER:role/SM-Read-Role"

    # S3 Bucket Configurations
    s3_bucket_path = "s3://mybucket/"
    s3_bucket_region = "eu-west-1"
    s3_write_role = "arn:aws:iam::DEV_ACCOUNT_NUMBER:role/S3-Write-Role"

    # Redshift Configurations
    sql_query = "select * from category"
    redshift_db = "dev"
    redshift_s3_write_role = "arn:aws:iam::PROD_ACCOUNT_NUMBER:role/CrossAccount-S3-Write-Role"

    chained_s3_write_role = "%s,%s" % (redshift_s3_write_role, s3_write_role)

    assumed_role_object = sts_client.assume_role(
        RoleArn=sm_read_role,
        RoleSessionName="CrossAccountRoleAssumption",
        ExternalId="YOUR_EXTERNAL_ID",
    )
    credentials = assumed_role_object['Credentials']

    secret_dict = ast.literal_eval(
        get_secret(credentials, secret_name, sm_region))
    execute_query(secret_dict, sql_query, s3_bucket_path,
                  chained_s3_write_role, s3_bucket_region, redshift_db)

    return {
        'statusCode': 200
    }


def get_secret(credentials, secret_name, sm_region):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    sm_client = session.client(
        service_name='secretsmanager',
        aws_access_key_id=credentials['AccessKeyId'],
        aws_secret_access_key=credentials['SecretAccessKey'],
        aws_session_token=credentials['SessionToken'],
        region_name=sm_region
    )

    try:
        get_secret_value_response = sm_client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(e)
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            return get_secret_value_response['SecretString']
        else:
            return base64.b64decode(get_secret_value_response['SecretBinary'])


def execute_query(secret_dict, sql_query, s3_bucket_path, chained_s3_write_role, s3_bucket_region, redshift_db):
    conn_string = "dbname='%s' port='%s' user='%s' password='%s' host='%s'" \
        % (redshift_db,
           secret_dict["port"],
           secret_dict["username"],
           secret_dict["password"],
           secret_dict["host"])

    con = psycopg2.connect(conn_string)

    unload_command = "UNLOAD ('{}') TO '{}' IAM_ROLE '{}' DELIMITER '|' REGION '{}';" \
        .format(sql_query,
                s3_bucket_path + str(datetime.datetime.now()) + ".csv",
                chained_s3_write_role,
                s3_bucket_region)

    # Opening a cursor and run query
    cur = con.cursor()
    cur.execute(unload_command)

    print(cur.fetchone())
    cur.close()
    con.close()

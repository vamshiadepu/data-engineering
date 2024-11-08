import pandas as pd
import boto3
import json
import configparser
import argparse
from time import sleep
import os

config = configparser.ConfigParser()
config.optionxform = str
config.read_file(open('dwh.cfg'))

KEY = os.getenv('KEY')
SECRET = os.getenv('SECRET')

CLUSTER_TYPE       = config.get("CLUSTER","CLUSTER_TYPE")
NUM_NODES          = config.get("CLUSTER","NUM_NODES")
NODE_TYPE          = config.get("CLUSTER","NODE_TYPE")

CLUSTER_IDENTIFIER = config.get("CLUSTER","CLUSTER_IDENTIFIER")
DB_NAME                 = config.get("CLUSTER","DB_NAME")
DB_USER            = config.get("CLUSTER","DB_USER")
DB_PASSWORD        = config.get("CLUSTER","DB_PASSWORD")
PORT               = config.get("CLUSTER","DB_PORT")

IAM_ROLE_NAME      = config.get("CLUSTER", "IAM_ROLE_NAME")

(DB_USER, DB_PASSWORD, DB_NAME)

region_name = "us-west-2"


def create_iam_role(iam):
    """
    Creates IAM role
    """
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                    'Effect': 'Allow',
                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                    'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)
    return roleArn

def create_cluster(roleArn, redshift):
    """
    Creates cluster
    """
    try:
        response = redshift.create_cluster(
            #HW
            ClusterType=CLUSTER_TYPE,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NUM_NODES),

            #Identifiers & Credentials
            DBName=DB_NAME,
            ClusterIdentifier=CLUSTER_IDENTIFIER,
            MasterUsername=DB_USER,
            MasterUserPassword=DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[roleArn]
        )
        print(json.dumps(response, indent=4, default=str))
    except Exception as e:
        print("Exception in redshift cluster creation")
        print(e)


def clean_up(iam, redshift):
    """
    Deletes cluster and IAM role
    """
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
    DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
    DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    redshift.delete_cluster(ClusterIdentifier=CLUSTER_IDENTIFIER, SkipFinalClusterSnapshot=True)
    iam.detach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=IAM_ROLE_NAME)
    cluster = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]


def main(args):
    wait_minutes = 10
    seconds = wait_minutes * 60
    ec2 = boto3.resource('ec2',
                         region_name=region_name,
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET)
    s3 = boto3.resource('s3',
                        region_name=region_name,
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET)
    iam = boto3.client('iam',aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name=region_name
                       )
    redshift = boto3.client('redshift',
                            region_name=region_name,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)

    if args.clean_up is True:
        clean_up(iam, redshift)
        for i in range(seconds):
            cluster = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
            print(cluster['ClusterStatus'], end='\r')
            sleep(1)
    else:
        role_arn = create_iam_role(iam)
        create_cluster(role_arn, redshift)
        for i in range(seconds):
            cluster = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
            print(cluster['ClusterStatus'])
            if cluster['ClusterStatus'] == 'available':
                with open('dwh.cfg', 'w') as configfile:
                    DWH_ENDPOINT = cluster['Endpoint']['Address']
                    DWH_ROLE_ARN = cluster['IamRoles'][0]['IamRoleArn']
                    config.set('CLUSTER', 'HOST', DWH_ENDPOINT)
                    config.set('CLUSTER', 'DB_USER', DB_USER)
                    config.set('CLUSTER', 'DB_PORT', PORT)
                    config.set('CLUSTER', 'DB_PASSWORD', DB_PASSWORD)
                    config.set('CLUSTER', 'DB_NAME', DB_NAME)
                    config.set('CLUSTER', 'REGION', str(region_name))
                    config.set('IAM_ROLE', 'ARN', role_arn)
                    config.write(configfile)
                sleep(5)
                try:
                    vpc = ec2.Vpc(id=cluster['VpcId'])
                    defaultSg = list(vpc.security_groups.all())[0]
                    print(defaultSg)
                    defaultSg.authorize_ingress(
                        GroupName=defaultSg.group_name,
                        CidrIp='0.0.0.0/0',
                        IpProtocol='TCP',
                        FromPort=int(PORT),
                        ToPort=int(PORT)
                    )
                except Exception as e:
                    print(e)
                break
            elif cluster['ClusterStatus'] == 'unavailable':
                print('\nCreated cluster but it went down')
            else:
                print('waiting for redshift to come up for %s seconds' % i, end='\r')
            sleep(5)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--clean_up', dest='clean_up', default=False, action='store_true')
    args = parser.parse_args()
    main(args)
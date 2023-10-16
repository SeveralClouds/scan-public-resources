import os
import boto3
from datetime import datetime
import json
import csv
import re

current_time = datetime.now().strftime("%Y-%m-%d")
report_name = f"public_resources-{current_time}.csv"
filename = f"/tmp/{report_name}"
AGGREGATOR_NAME = os.environ["AGGREGATOR_NAME"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
client = boto3.client("config")


def deserializer(public_resources, resource_name):
    try:
        with open(filename, "a", newline="") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerows(public_resources)
        print(f"Building report on {resource_name}...")
    except Exception as e:
        print(f"An error occurred while writing the report: {e}")


def load_balancer_v2(AGGREGATOR_NAME, filename):
    result_load_balancer_v2 = client.select_aggregate_resource_config(
        Expression="SELECT  accountId,  resourceType,  resourceId, configuration.dNSName WHERE  resourceType = 'AWS::ElasticLoadBalancingV2::LoadBalancer'  and configuration.scheme = 'internet-facing'",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )

    results = result_load_balancer_v2["Results"]

    while "NextToken" in result_load_balancer_v2:
        result_load_balancer_v2 = client.select_aggregate_resource_config(
            Expression="SELECT  accountId,  resourceType,  resourceId, configuration.dNSName WHERE  resourceType = 'AWS::ElasticLoadBalancingV2::LoadBalancer'  and configuration.scheme = 'internet-facing'",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_load_balancer_v2["NextToken"],
        )
        results.extend(result_load_balancer_v2["Results"])

    parsed_results = [json.loads(item) for item in results]
    public_resources = [
        [
            item["accountId"],
            item["resourceType"],
            item["resourceId"],
            item["configuration"]["dNSName"],
        ]
        for item in parsed_results
    ]
    deserializer(public_resources, "Load Balancer v2")


def load_balancer(AGGREGATOR_NAME, filename):
    result_load_balancer = client.select_aggregate_resource_config(
        Expression="SELECT  accountId,  resourceType,  resourceId, configuration.dnsname WHERE  resourceType = 'AWS::ElasticLoadBalancing::LoadBalancer'  and configuration.scheme = 'internet-facing'",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )

    results = result_load_balancer["Results"]

    while "NextToken" in result_load_balancer:
        result_load_balancer = client.select_aggregate_resource_config(
            Expression="SELECT  accountId,  resourceType,  resourceId, configuration.dnsname WHERE  resourceType = 'AWS::ElasticLoadBalancing::LoadBalancer'  and configuration.scheme = 'internet-facing'",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_load_balancer["NextToken"],
        )
        results.extend(result_load_balancer["Results"])

    parsed_results = [json.loads(item) for item in results]
    public_resources = [
        [
            item["accountId"],
            item["resourceType"],
            item["resourceId"],
            item["configuration"]["dnsname"],
        ]
        for item in parsed_results
    ]
    deserializer(public_resources, "Load Balancer")


def elastic_ip(AGGREGATOR_NAME, filename):
    result_elastic_ip = client.select_aggregate_resource_config(
        Expression="SELECT  accountId,  resourceType,  resourceId, configuration.publicIp WHERE  resourceType = 'AWS::EC2::EIP' AND configuration.associationId LIKE 'eipassoc-%'",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )
    results = result_elastic_ip["Results"]

    while "NextToken" in result_elastic_ip:
        result_elastic_ip = client.select_aggregate_resource_config(
            Expression="SELECT  accountId,  resourceType,  resourceId, configuration.publicIp WHERE  resourceType = 'AWS::EC2::EIP' AND configuration.associationId LIKE 'eipassoc-%'",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_elastic_ip["NextToken"],
        )
        results.extend(result_elastic_ip["Results"])
    parsed_results = [json.loads(item) for item in results]
    public_resources = [
        [
            item["accountId"],
            item["resourceType"],
            item["resourceId"],
            item["configuration"]["publicIp"],
        ]
        for item in parsed_results
    ]
    deserializer(public_resources, "Elastic Ip")


def cloud_front(AGGREGATOR_NAME, filename):
    result_cloud_front = client.select_aggregate_resource_config(
        Expression="SELECT  accountId,  resourceType,  resourceId, configuration.domainName,  configuration.aliasICPRecordals.cNAME WHERE  resourceType = 'AWS::CloudFront::Distribution'",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )
    results = result_cloud_front["Results"]

    while "NextToken" in result_cloud_front:
        result_cloud_front = client.select_aggregate_resource_config(
            Expression="SELECT  accountId,  resourceType,  resourceId, configuration.domainName,  configuration.aliasICPRecordals.cNAME WHERE  resourceType = 'AWS::CloudFront::Distribution'",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_cloud_front["NextToken"],
        )
        results.extend(result_cloud_front["Results"])

    parsed_results = [json.loads(item) for item in results]
    public_resources = []

    for item in parsed_results:
        account_id = item["accountId"]
        resource_type = item["resourceType"]
        resource_id = item["resourceId"]

        configuration = item.get("configuration")
        if configuration:
            domain_name = configuration.get("domainName")

            if domain_name:
                public_resources.append(
                    [account_id, resource_type, resource_id, domain_name]
                )

            if "aliasICPRecordals" in configuration:
                cnames = item["configuration"]["aliasICPRecordals"]
                if cnames:
                    for cname in cnames:
                        public_resources.append(
                            [account_id, resource_type, resource_id, cname["cNAME"]]
                        )

    deserializer(public_resources, "Cloud Front")


def ec2(AGGREGATOR_NAME, filename):
    result_ec2 = client.select_aggregate_resource_config(
        Expression="SELECT  accountId,  resourceType,  resourceId, configuration.publicIpAddress WHERE  resourceType = 'AWS::EC2::Instance'  and configuration.publicIpAddress BETWEEN '10.0.0.0'  AND '255.255.255.255'",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )

    results = result_ec2["Results"]

    while "NextToken" in result_ec2:
        result_ec2 = client.select_aggregate_resource_config(
            Expression="SELECT  accountId,  resourceType,  resourceId, configuration.publicIpAddress WHERE  resourceType = 'AWS::EC2::Instance'  and configuration.publicIpAddress BETWEEN '10.0.0.0'  AND '255.255.255.255'",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_ec2["NextToken"],
        )
        results.extend(result_ec2["Results"])

    parsed_results = [json.loads(item) for item in results]
    public_resources = [
        [
            item["accountId"],
            item["resourceType"],
            item["resourceId"],
            item["configuration"]["publicIpAddress"],
        ]
        for item in parsed_results
    ]
    deserializer(public_resources, "EC2 with public IP")


def rds_instance(AGGREGATOR_NAME, filename):
    result_rds_instance = client.select_aggregate_resource_config(
        Expression="SELECT  accountId,  resourceType,  resourceId, configuration.endpoint.address  WHERE resourceType = 'AWS::RDS::DBInstance' and configuration.publiclyAccessible = TRUE",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )

    results = result_rds_instance["Results"]

    while "NextToken" in result_rds_instance:
        result_rds_instance = client.select_aggregate_resource_config(
            Expression="SELECT  accountId,  resourceType,  resourceId, configuration.endpoint.address  WHERE resourceType = 'AWS::RDS::DBInstance' and configuration.publiclyAccessible = TRUE",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_rds_instance["NextToken"],
        )
        results.extend(result_rds_instance["Results"])

    parsed_results = [json.loads(item) for item in results]
    public_resources = [
        [
            item["accountId"],
            item["resourceType"],
            item["resourceId"],
            item["configuration"]["endpoint"]["address"],
        ]
        for item in parsed_results
    ]
    deserializer(public_resources, "RDS DB Instances")


def rds_cluster(AGGREGATOR_NAME, filename):
    result_rds_cluster = client.select_aggregate_resource_config(
        Expression="SELECT  accountId,  resourceType,  resourceId, configuration.endpoint.value WHERE resourceType = 'AWS::RDS::DBCluster' and configuration.publiclyAccessible = TRUE",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )
    results = result_rds_cluster["Results"]

    while "NextToken" in result_rds_cluster:
        result_rds_cluster = client.select_aggregate_resource_config(
            Expression="SELECT  accountId,  resourceType,  resourceId, configuration.endpoint.value WHERE resourceType = 'AWS::RDS::DBCluster' and configuration.publiclyAccessible = TRUE",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_rds_cluster["NextToken"],
        )
        results.extend(result_rds_cluster["Results"])

    parsed_results = [json.loads(item) for item in results]
    public_resources = [
        [
            item["accountId"],
            item["resourceType"],
            item["resourceId"],
            item["configuration"]["endpoint"]["value"],
        ]
        for item in parsed_results
    ]
    deserializer(public_resources, "RDS DB Clusters")


def open_search(AGGREGATOR_NAME, filename):
    result_open_search = client.select_aggregate_resource_config(
        Expression="SELECT  accountId,  resourceType,  resourceId, configuration.DomainEndpoint  WHERE resourceType = 'AWS::OpenSearch::Domain' and not configuration.VPCOptions.SecurityGroupIds LIKE 'sg-%'",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )

    results = result_open_search["Results"]

    while "NextToken" in result_open_search:
        result_open_search = client.select_aggregate_resource_config(
            Expression="SELECT  accountId,  resourceType,  resourceId, configuration.DomainEndpoint  WHERE resourceType = 'AWS::OpenSearch::Domain' and not configuration.VPCOptions.SecurityGroupIds LIKE 'sg-%'",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_open_search["NextToken"],
        )
        results.extend(result_open_search["Results"])

    parsed_results = [json.loads(item) for item in results]
    public_resources = [
        [
            item["accountId"],
            item["resourceType"],
            item["resourceId"],
            item["configuration"]["DomainEndpoint"],
        ]
        for item in parsed_results
    ]
    deserializer(public_resources, "Open Search")


def eks_cluster(AGGREGATOR_NAME, filename):
    result_eks_cluster = client.select_aggregate_resource_config(
        Expression="SELECT  accountId,  resourceType,  resourceId, configuration.Endpoint WHERE resourceType = 'AWS::EKS::Cluster' and configuration.ResourcesVpcConfig.EndpointPublicAccess = TRUE",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )

    results = result_eks_cluster["Results"]

    while "NextToken" in result_eks_cluster:
        result_eks_cluster = client.select_aggregate_resource_config(
            Expression="SELECT  accountId,  resourceType,  resourceId, configuration.Endpoint WHERE resourceType = 'AWS::EKS::Cluster' and configuration.ResourcesVpcConfig.EndpointPublicAccess = TRUE",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_eks_cluster["NextToken"],
        )
        results.extend(result_eks_cluster["Results"])

    parsed_results = [json.loads(item) for item in results]
    public_resources = [
        [
            item["accountId"],
            item["resourceType"],
            item["resourceId"],
            item["configuration"]["Endpoint"],
        ]
        for item in parsed_results
    ]
    deserializer(public_resources, "EKS Cluster")


def rest_api(AGGREGATOR_NAME, filename):
    result_rest_api = client.select_aggregate_resource_config(
        Expression="SELECT  accountId,  resourceType,  resourceId, relationships.resourceId WHERE resourceType = 'AWS::ApiGateway::RestApi' and configuration.endpointConfiguration.types NOT LIKE 'PRIVATE'",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )

    results = result_rest_api["Results"]

    while "NextToken" in result_rest_api:
        result_rest_api = client.select_aggregate_resource_config(
            Expression="SELECT  accountId,  resourceType,  resourceId, relationships.resourceId WHERE resourceType = 'AWS::ApiGateway::RestApi' and configuration.endpointConfiguration.types NOT LIKE 'PRIVATE'",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_rest_api["NextToken"],
        )
        results.extend(result_rest_api["Results"])

    parsed_results = [json.loads(item) for item in results]
    public_resources = []

    for item in parsed_results:
        resource_ids = [
            res_id.get("resourceId", "-")
            for res_id in item.get("relationships", [{"resourceId": "-"}])
        ]
        formatted_url = "-"

        for resource_id in resource_ids:
            match = re.match(
                r"arn:aws:apigateway:(.*)::\/restapis\/(.*)\/stages\/(.*)", resource_id
            )
            if match:
                region, restapi_id, stagename = match.groups()
                formatted_url = f"https://{restapi_id}.execute-api.{region}.amazonaws.com/{stagename}/"

            if formatted_url != "-":
                public_resources.append(
                    [
                        item.get("accountId"),
                        item.get("resourceType"),
                        item.get("resourceId"),
                        formatted_url,
                    ]
                )

    deserializer(public_resources, "Api Gateway Rest Api")


def api_v2(AGGREGATOR_NAME, filename):
    result_api_v2 = client.select_aggregate_resource_config(
        Expression="SELECT  accountId,  resourceType,  resourceId, configuration.apiEndpoint WHERE resourceType = 'AWS::ApiGatewayV2::Api'",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )
    results = result_api_v2["Results"]

    while "NextToken" in result_api_v2:
        result_api_v2 = client.select_aggregate_resource_config(
            Expression="SELECT  accountId,  resourceType,  resourceId, configuration.apiEndpoint WHERE resourceType = 'AWS::ApiGatewayV2::Api'",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_api_v2["NextToken"],
        )
        results.extend(result_api_v2["Results"])

    parsed_results = [json.loads(item) for item in results]
    public_resources = [
        [
            item["accountId"],
            item["resourceType"],
            item["resourceId"],
            item["configuration"]["apiEndpoint"],
        ]
        for item in parsed_results
    ]
    deserializer(public_resources, "Api Gateway V2")


def global_accelerator(AGGREGATOR_NAME, filename):
    result_global_acc = client.select_aggregate_resource_config(
        Expression="SELECT  accountId,  resourceType,  resourceId, configuration.DnsName  WHERE resourceType = 'AWS::GlobalAccelerator::Accelerator'",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )
    results = result_global_acc["Results"]

    while "NextToken" in result_global_acc:
        result_global_acc = client.select_aggregate_resource_config(
            Expression="SELECT  accountId,  resourceType,  resourceId, configuration.DnsName  WHERE resourceType = 'AWS::GlobalAccelerator::Accelerator'",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_global_acc["NextToken"],
        )
        results.extend(result_global_acc["Results"])

    parsed_results = [json.loads(item) for item in results]
    public_resources = [
        [
            item["accountId"],
            item["resourceType"],
            item["resourceId"],
            item["configuration"]["DnsName"],
        ]
        for item in parsed_results
    ]
    deserializer(public_resources, "Global Accelerator")


def redshift(AGGREGATOR_NAME, filename):
    result_redshift = client.select_aggregate_resource_config(
        Expression="SELECT  accountId,  resourceType,  resourceId, configuration.endpoint.address WHERE resourceType = 'AWS::Redshift::Cluster' and configuration.publiclyAccessible = TRUE",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )

    results = result_redshift["Results"]

    while "NextToken" in result_redshift:
        result_redshift = client.select_aggregate_resource_config(
            Expression="SELECT  accountId,  resourceType,  resourceId, configuration.endpoint.address WHERE resourceType = 'AWS::Redshift::Cluster' and configuration.publiclyAccessible = TRUE",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_redshift["NextToken"],
        )
        results.extend(result_redshift["Results"])
    parsed_results = [json.loads(item) for item in results]

    public_resources = [
        [
            item["accountId"],
            item["resourceType"],
            item["resourceId"],
            item["configuration"]["endpoint"]["address"],
        ]
        for item in parsed_results
    ]
    deserializer(public_resources, "Redshift Cluster")


def app_sync(AGGREGATOR_NAME, filename):
    result_app_sync = client.select_aggregate_resource_config(
        Expression="SELECT  accountId,  resourceType,  resourceId, configuration.GraphQLUrl WHERE  resourceType = 'AWS::AppSync::GraphQLApi'",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )
    results = result_app_sync["Results"]

    while "NextToken" in result_app_sync:
        result_app_sync = client.select_aggregate_resource_config(
            Expression="SELECT  accountId,  resourceType,  resourceId, configuration.GraphQLUrl WHERE  resourceType = 'AWS::AppSync::GraphQLApi'",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_app_sync["NextToken"],
        )
        results.extend(result_app_sync["Results"])

    parsed_results = [json.loads(item) for item in results]
    public_resources = [
        [
            item["accountId"],
            item["resourceType"],
            item["resourceId"],
            item["configuration"]["GraphQLUrl"],
        ]
        for item in parsed_results
    ]
    deserializer(public_resources, "AppSync")


def app_runner(AGGREGATOR_NAME, filename):
    result_app_runner = client.select_aggregate_resource_config(
        Expression="SELECT accountId, resourceType, resourceId, configuration.ServiceUrl WHERE resourceType = 'AWS::AppRunner::Service' AND configuration.NetworkConfiguration.IngressConfiguration.IsPubliclyAccessible = TRUE",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )

    results = result_app_runner["Results"]

    while "NextToken" in result_app_runner:
        result_app_runner = client.select_aggregate_resource_config(
            Expression="SELECT accountId, resourceType, resourceId, configuration.ServiceUrl WHERE resourceType = 'AWS::AppRunner::Service' AND configuration.NetworkConfiguration.IngressConfiguration.IsPubliclyAccessible = TRUE",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_app_runner["NextToken"],
        )
        results.extend(result_app_runner["Results"])

    parsed_results = [json.loads(item) for item in results]
    public_resources = [
        [
            item["accountId"],
            item["resourceType"],
            item["resourceId"],
            item["configuration"]["ServiceUrl"],
        ]
        for item in parsed_results
        if item["configuration"]["ServiceUrl"]
    ]
    deserializer(public_resources, "App Runner")


def data_migration_service(AGGREGATOR_NAME, filename):
    result_data_migration_service = client.select_aggregate_resource_config(
        Expression="SELECT accountId, resourceType, resourceId, configuration.ReplicationInstancePublicIpAddresses WHERE resourceType = 'AWS::DMS::ReplicationInstance' AND configuration.PubliclyAccessible = TRUE",
        ConfigurationAggregatorName=AGGREGATOR_NAME,
        Limit=100,
    )
    results = result_data_migration_service["Results"]

    while "NextToken" in result_data_migration_service:
        result_data_migration_service = client.select_aggregate_resource_config(
            Expression="SELECT accountId, resourceType, resourceId, configuration.ReplicationInstancePublicIpAddresses WHERE resourceType = 'AWS::DMS::ReplicationInstance' AND configuration.PubliclyAccessible = TRUE",
            ConfigurationAggregatorName=AGGREGATOR_NAME,
            Limit=100,
            NextToken=result_data_migration_service["NextToken"],
        )
        results.extend(result_data_migration_service["Results"])

    parsed_results = [json.loads(item) for item in results]
    public_resources = [
        [
            item["accountId"],
            item["resourceType"],
            item["resourceId"],
            item["configuration"]["ReplicationInstancePublicIpAddresses"],
        ]
        for item in parsed_results
        if item["configuration"]["ReplicationInstancePublicIpAddresses"]
    ]
    deserializer(public_resources, "Data Migration Service")


def create_report(AGGREGATOR_NAME, filename):
    load_balancer_v2(AGGREGATOR_NAME, filename)
    load_balancer(AGGREGATOR_NAME, filename)
    elastic_ip(AGGREGATOR_NAME, filename)
    cloud_front(AGGREGATOR_NAME, filename)
    ec2(AGGREGATOR_NAME, filename)
    rds_instance(AGGREGATOR_NAME, filename)
    rds_cluster(AGGREGATOR_NAME, filename)
    open_search(AGGREGATOR_NAME, filename)
    eks_cluster(AGGREGATOR_NAME, filename)
    rest_api(AGGREGATOR_NAME, filename)
    api_v2(AGGREGATOR_NAME, filename)
    global_accelerator(AGGREGATOR_NAME, filename)
    redshift(AGGREGATOR_NAME, filename)
    app_sync(AGGREGATOR_NAME, filename)
    app_runner(AGGREGATOR_NAME, filename)
    data_migration_service(AGGREGATOR_NAME, filename)

    print("Report generated " + filename)

    if upload_to_s3(filename):
        print(f"Uploaded {filename} to S3 bucket {BUCKET_NAME}")
    else:
        print(f"Failed to upload {filename} to S3 bucket {BUCKET_NAME}")


def upload_to_s3(filename):
    s3 = boto3.client("s3")

    try:
        s3.upload_file(filename, BUCKET_NAME, f"PublicResources_Report/{report_name}")
        return True
    except Exception as e:
        print(f"Error uploading {filename} to S3: {e}")
        return False


def lambda_handler(event, lambda_context):
    create_report(AGGREGATOR_NAME, filename)

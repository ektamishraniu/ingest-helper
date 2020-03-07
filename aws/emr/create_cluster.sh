#!/bin/bash

aws emr create-cluster --release-label emr-5.19.0 \
  --name 'td-transformation-prof-dev-emr006' \
  --applications Name=Hadoop Name=Hive Name=Spark Name=Sqoop Name=Zookeeper Name=Hue \
  --ec2-attributes KeyName=td-transformation-emr,InstanceProfile=EMR_EC2_DefaultRole,SubnetId=subnet-6b4b2f36,AdditionalMasterSecurityGroups=sg-954e7dde,sg-87ac82f3 \
  --service-role EMR_DefaultRole --instance-groups \
    InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m5.2xlarge \
    InstanceGroupType=CORE,InstanceCount=4,InstanceType=m5.2xlarge \
  --region us-east-1 \
  --log-uri s3://wrktdtransformationlogproddtl001/emr-logs/ \
  --tags "owner=ekta.mishra.niu@gmail.com" "ipt=transformation" \
    "env=dev" "org=techdev" \
  --bootstrap-actions Path=s3://wrktdtransformationprocessproddtl001/cluster-support-files/bootstrap-scripts/taskrunner.sh,Args=AKIAIKAN6M4XJUFNKY7Q,HJ5mEhkDXrW3QpdOWjpJZhfReVmAbCFYwrjUwzas,td-transformation-dev-wg006,us-east-1,s3://wrktdtransformationlogproddtl001/transformation-dev-emr,Name=TaskRunner \
    Path=s3://wrktdtransformationprocessproddtl001/cluster-support-files/bootstrap-scripts/sqoop-support.sh,Name=Sqoop-Support \
    Path=s3://wrktdtransformationprocessproddtl001/cluster-support-files/bootstrap-scripts/python36.sh,Name=Python36 \
  --configurations file://cluster-config.json
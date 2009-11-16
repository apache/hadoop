HBase EC2

This collection of scripts allows you to run HBase clusters on Amazon.com's Elastic Compute Cloud (EC2) service described at:

  http://aws.amazon.com/ec2
  
To get help, type the following in a shell:
  
  bin/hbase-ec2

You need both the EC2 API and AMI tools 

  http://developer.amazonwebservices.com/connect/entry.jspa?externalID=351

  http://developer.amazonwebservices.com/connect/entry.jspa?externalID=368&categoryID=88

installed and on the path. For Ubuntu, "apt-get install ec2-ami-tools ec2-api-tools".  

The hbase-ec2-env.sh script requires some configuration:

- Fill in AWS_ACCOUNT_ID with your EC2 account number

- Fill in AWS_ACCESS_KEY_ID with your EC2 access key

- Fill in AWS_SECRET_ACCESS_KEY with your EC2 secret access key

- Fill in KEY_NAME with the SSH keypair you will use

- Fill in EC2_PRIVATE_KEY with the location of your AWS private key file --
  must begin with 'pk' and end with '.pem'

- Fill in EC2_CERT with the location of your AWS certificate -- must begin
  with 'cert' and end with '.pem'

- Make sure the private part of your AWS SSH keypair exists in the same
  directory as EC2_PRIVATE_KEY with the name id_rsa_${KEYNAME}

HBase EC2

This collection of scripts allows you to run HBase clusters on Amazon.com's Elastic Compute Cloud (EC2) service described at:

  http://aws.amazon.com/ec2
  
To get help, type the following in a shell:
  
  bin/hbase-ec2

You need both the EC2 API and AMI tools 

  http://developer.amazonwebservices.com/connect/entry.jspa?externalID=351

  http://developer.amazonwebservices.com/connect/entry.jspa?externalID=368&categoryID=88

installed and on the path. 

When setting up keypairs on EC2, be sure to name your keypair as 'root'. 

Quick Start:

1) Download and unzip the EC2 AMI and API tools zipfiles.

   For Ubuntu, "apt-get install ec2-ami-tools ec2-api-tools".

2) Put the tools on the path and set EC2_HOME in the environment to point to
   the top level directory of the API tools.

3) Configure src/contrib/ec2/bin/hbase-ec2-env.sh

   Fill in AWS_ACCOUNT_ID with your EC2 account number.

   Fill in AWS_ACCESS_KEY_ID with your EC2 access key.

   Fill in AWS_SECRET_ACCESS_KEY with your EC2 secret access key.

   Fill in EC2_PRIVATE_KEY with the location of your AWS private key file --
   must begin with 'pk' and end with '.pem'.

   Fill in EC2_CERT with the location of your AWS certificate -- must begin
   with 'cert' and end with '.pem'.

   Make sure the private part of your AWS SSH keypair exists in the same
   directory as EC2_PRIVATE_KEY with the name id_rsa_root. Also, insure that
   the permissions on the private key file are 600 (ONLY owner readable/
   writable).

4) ./bin/hbase-ec2 launch-cluster <name> <nr-zoos> <nr-slaves>, e.g

       ./bin/hbase-ec2 launch-cluster testcluster 3 3

5) Once the above command has finished without error, ./bin/hbase-ec2 login
   <name>, e.g.

       ./bin/hbase-ec2 login testcluster

6) Check that the cluster is up and functional:

       hbase shell
       > status 'simple'

   You should see something like:

       3 live servers
         domU-12-31-39-09-75-11.compute-1.internal:60020 1258653694915
           requests=0, regions=1, usedHeap=29, maxHeap=987
         domU-12-31-39-01-AC-31.compute-1.internal:60020 1258653709041
           requests=0, regions=1, usedHeap=29, maxHeap=987
         domU-12-31-39-01-B0-91.compute-1.internal:60020 1258653706411
           requests=0, regions=0, usedHeap=27, maxHeap=987
       0 dead servers


Extra Packages:

It is possible to specify that extra packages be downloaded and installed on
demand when the master and slave instances start. 

   1. Set up a YUM repository. See: http://yum.baseurl.org/wiki/RepoCreate

   2. Host the repository somewhere public. For example, build the
      repository locally and then copy it up to an S3 bucket.

   3. Create a YUM repository descriptor (.repo file). See:
      http://yum.baseurl.org/wiki/RepoCreate

        [myrepo]
        name = MyRepo
        baseurl = http://mybucket.s3.amazonaws.com/myrepo
        enabled = 1

      Upload the .repo file somewhere public, for example, in the root
      directory of the repository,
      mybucket.s3.amazonaws.com/myrepo/myrepo.repo

   4. Configure hbase-ec2-env.sh thus:

      EXTRA_PACKAGES="http://mybucket.s3.amazonaws.com/myrepo/myrepo.repo \
        pkg1 pkg2 pkg3"

When the master and slave instances start, the .repo file will be added to
the Yum repository list and then Yum will be invoked to pull the packages
listed after the URL. 

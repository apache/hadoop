# Set environment variables for running Hadoop on Amazon EC2 here. All are required.

# Your Amazon Account Number.
AWS_ACCOUNT_ID=

# Your Amazon AWS access key.
AWS_ACCESS_KEY_ID=

# Your Amazon AWS secret access key.
AWS_SECRET_ACCESS_KEY=

# The Amazon S3 bucket where the Hadoop AMI you create will be stored.
S3_BUCKET=

# SSH options used when connecting to EC2 instances.
# Change the -i option to be the absolute path to your keypair that you set up in the Amazon Getting Started guide.
SSH_OPTS='-i /home/<yourname>/id_rsa-gsg-keypair -o StrictHostKeyChecking=no'

# Location of EC2 keys.
# The default setting is probably OK if you set up EC2 following the Amazon Getting Started guide.
EC2_KEYDIR=~/.ec2

# The download URL for the Sun JDK. Visit http://java.sun.com/javase/downloads/index_jdk5.jsp and get the URL for the "Linux self-extracting file".
JAVA_BINARY_URL=''

# The version number of the installed JDK.
JAVA_VERSION=1.5.0_11

# The EC2 group to run your cluster in.
GROUP=hadoop-cluster-group

# The version of Hadoop to install.
HADOOP_VERSION=0.10.1

# The hostname of the master node in the cluster. You need to be able to set the DNS for this host to point to the master's IP address.
# See http://www.dyndns.com/services/dns/dyndns/, for example.
MASTER_HOST=

# The number of nodes in your cluster.
NO_INSTANCES=2

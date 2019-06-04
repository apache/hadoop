<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Submarine Installation Guide

## Prerequisites

Please note that the following prerequisites are just an example for you to install Submarine.

You can always choose to install your own version of kernel, different users, different drivers, etc.

### Operating System

The operating system and kernel versions we have tested against are shown in the following table.
The versions in the table are the recommended minimum required versions.

| Environment | Version |
| ------ | ------ |
| Operating System | centos-release-7-5.1804.el7.centos.x86_64 |
| Kernel | 3.10.0-862.el7.x86_64 |

### User & Group

There are specific users and groups recommended to be created to install Hadoop with Docker.

Please create these users if they do not exist.

```
adduser hdfs
adduser mapred
adduser yarn
addgroup hadoop
usermod -aG hdfs,hadoop hdfs
usermod -aG mapred,hadoop mapred
usermod -aG yarn,hadoop yarn
usermod -aG hdfs,hadoop hadoop
groupadd docker
usermod -aG docker yarn
usermod -aG docker hadoop
```

### GCC Version

Check the version of GCC tool (to compile kernel).

```bash
gcc --version
gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-11)
# install if needed
yum install gcc make g++
```

### Kernel header & Kernel devel

```bash
# Approach 1：
yum install kernel-devel-$(uname -r) kernel-headers-$(uname -r)
# Approach 2：
wget http://vault.centos.org/7.3.1611/os/x86_64/Packages/kernel-headers-3.10.0-862.el7.x86_64.rpm
rpm -ivh kernel-headers-3.10.0-862.el7.x86_64.rpm
```

### GPU Servers (Only for Nvidia GPU equipped nodes)

```
lspci | grep -i nvidia

# If the server has gpus, you can get info like this：
04:00.0 3D controller: NVIDIA Corporation Device 1b38 (rev a1)
82:00.0 3D controller: NVIDIA Corporation Device 1b38 (rev a1)
```



### Nvidia Driver Installation (Only for Nvidia GPU equipped nodes)

To make a clean installation, if you have requirements to upgrade GPU drivers.

If nvidia driver / CUDA has been installed before, they should be uninstalled as a first step.

```
# uninstall cuda：
sudo /usr/local/cuda-10.0/bin/uninstall_cuda_10.0.pl

# uninstall nvidia-driver：
sudo /usr/bin/nvidia-uninstall
```

To check GPU version, install nvidia-detect:

```
yum install nvidia-detect
# run 'nvidia-detect -v' to get reqired nvidia driver version：
nvidia-detect -v
Probing for supported NVIDIA devices...
[10de:13bb] NVIDIA Corporation GM107GL [Quadro K620]
This device requires the current xyz.nm NVIDIA driver kmod-nvidia
[8086:1912] Intel Corporation HD Graphics 530
An Intel display controller was also detected
```

Pay attention to `This device requires the current xyz.nm NVIDIA driver kmod-nvidia`.
Download the installer like [NVIDIA-Linux-x86_64-390.87.run](https://www.nvidia.com/object/linux-amd64-display-archive.html).


Some preparatory work for Nvidia driver installation.

The steps below are for Nvidia GPU driver installation, just pasted here for your convenience.

```
# It may take a while to update
yum -y update
yum -y install kernel-devel

yum -y install epel-release
yum -y install dkms

# Disable nouveau
vim /etc/default/grub
# Add the following configuration in “GRUB_CMDLINE_LINUX” part
rd.driver.blacklist=nouveau nouveau.modeset=0

# Generate configuration
grub2-mkconfig -o /boot/grub2/grub.cfg

vim /etc/modprobe.d/blacklist.conf
# Add confiuration:
blacklist nouveau

mv /boot/initramfs-$(uname -r).img /boot/initramfs-$(uname -r)-nouveau.img
dracut /boot/initramfs-$(uname -r).img $(uname -r)
reboot
```

Check whether nouveau is disabled

```
lsmod | grep nouveau  # return null

# install nvidia driver
sh NVIDIA-Linux-x86_64-390.87.run
```

Some options during the installation

```
Install NVIDIA's 32-bit compatibility libraries (Yes)
centos Install NVIDIA's 32-bit compatibility libraries (Yes)
Would you like to run the nvidia-xconfig utility to automatically update your X configuration file... (NO)
```


Check Nvidia driver installation

```
nvidia-smi
```

Reference：
https://docs.nvidia.com/cuda/cuda-installation-guide-linux/index.html



### Docker Installation

The following steps show you how to install docker 18.06.1.ce. You can choose other approaches to install Docker.

```
# Remove old version docker
sudo yum remove docker \
                docker-client \
                docker-client-latest \
                docker-common \
                docker-latest \
                docker-latest-logrotate \
                docker-logrotate \
                docker-engine

# Docker version
export DOCKER_VERSION="18.06.1.ce"
# Setup the repository
sudo yum install -y yum-utils \
  device-mapper-persistent-data \
  lvm2
sudo yum-config-manager \
    --add-repo \
    https://download.docker.com/linux/centos/docker-ce.repo

# Check docker version
yum list docker-ce --showduplicates | sort -r

# Install docker with specified DOCKER_VERSION
sudo yum install -y docker-ce-${DOCKER_VERSION} docker-ce-cli-${DOCKER_VERSION} containerd.io

# Start docker
systemctl start docker

chown hadoop:netease /var/run/docker.sock
chown hadoop:netease /usr/bin/docker
```

Reference：https://docs.docker.com/install/linux/docker-ce/centos/

### Docker Configuration

Add a file, named daemon.json, under the path of /etc/docker/.

Please replace the variables of image_registry_ip, etcd_host_ip, localhost_ip, yarn_dns_registry_host_ip, dns_host_ip with specific IPs according to your environment.

```
{
    "insecure-registries": ["${image_registry_ip}:5000"],
    "cluster-store":"etcd://${etcd_host_ip1}:2379,${etcd_host_ip2}:2379,${etcd_host_ip3}:2379",
    "cluster-advertise":"{localhost_ip}:2375",
    "dns": ["${yarn_dns_registry_host_ip}", "${dns_host_ip1}"],
    "hosts": ["tcp://{localhost_ip}:2375", "unix:///var/run/docker.sock"]
}
```

Restart docker daemon：

```
sudo systemctl restart docker
```



### Check docker version

```bash
$ docker version

Client:
 Version:      18.06.1-ce
 API version:  1.38
 Go version:   go1.10.3
 Git commit:   e68fc7a
 Built:        Tue Aug 21 17:23:03 2018
 OS/Arch:      linux/amd64
 Experimental: false

Server:
 Version:      18.06.1-ce
 API version:  1.38 (minimum version 1.12)
 Go version:   go1.10.3
 Git commit:   e68fc7a
 Built:        Tue Aug 21 17:23:03 2018
 OS/Arch:      linux/amd64
 Experimental: false
```

### Nvidia-docker Installation (Only for Nvidia GPU equipped nodes)

Submarine has already supported nvidia-docker V2

```
# Add the package repositories
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
curl -s -L https://nvidia.github.io/nvidia-container-runtime/$distribution/nvidia-container-runtime.repo | \
  sudo tee /etc/yum.repos.d/nvidia-container-runtime.repo
sudo yum install -y nvidia-docker2-2.0.3-1.docker18.06.1.ce
```

According to `nvidia-driver` version, add folders under the path of  `/var/lib/nvidia-docker/volumes/nvidia_driver/`

```
mkdir /var/lib/nvidia-docker/volumes/nvidia_driver/390.87
# 390.8 is nvidia driver version

mkdir /var/lib/nvidia-docker/volumes/nvidia_driver/390.87/bin
mkdir /var/lib/nvidia-docker/volumes/nvidia_driver/390.87/lib64

cp /usr/bin/nvidia* /var/lib/nvidia-docker/volumes/nvidia_driver/390.87/bin
cp /usr/lib64/libcuda* /var/lib/nvidia-docker/volumes/nvidia_driver/390.87/lib64
cp /usr/lib64/libnvidia* /var/lib/nvidia-docker/volumes/nvidia_driver/390.87/lib64

# Test with nvidia-smi
nvidia-docker run --rm nvidia/cuda:10.0-devel nvidia-smi
```

Test docker, nvidia-docker, nvidia-driver installation

```
# Test 1
nvidia-docker run -rm nvidia/cuda nvidia-smi
```

```
# Test 2
nvidia-docker run -it tensorflow/tensorflow:1.9.0-gpu bash
# In docker container
python
import tensorflow as tf
tf.test.is_gpu_available()
```

If you want to uninstall nvidia-docker V2:
```
sudo yum remove -y nvidia-docker2-2.0.3-1.docker18.06.1.ce
```

Reference:
https://github.com/NVIDIA/nvidia-docker

### Tensorflow Image

There is no need to install CUDNN and CUDA on the servers, because CUDNN and CUDA can be added in the docker images.

We can get or build basic docker images by referring to [Write Dockerfile](WriteDockerfileTF.html).

### Test tensorflow in a docker container

After docker image is built, we can check
Tensorflow environments before submitting a Submarine job.

```shell
$ docker run -it ${docker_image_name} /bin/bash
# >>> In the docker container
$ python
$ python >> import tensorflow as tf
$ python >> tf.__version__
```

If there are some errors, we could check the following configuration.

1. LD_LIBRARY_PATH environment variable

   ```
   echo $LD_LIBRARY_PATH
   /usr/local/cuda/extras/CUPTI/lib64:/usr/local/nvidia/lib:/usr/local/nvidia/lib64
   ```

2. The location of libcuda.so.1, libcuda.so

   ```
   ls -l /usr/local/nvidia/lib64 | grep libcuda.so
   ```

### Etcd Installation

etcd is a distributed, reliable key-value store for the most critical data of a distributed system, Registration and discovery of services used in containers.
You can also choose alternatives like ZooKeeper, Consul or others.

To install Etcd on specified servers, we can run Submarine-installer/install.sh

```shell
$ ./Submarine-installer/install.sh
# Etcd status
systemctl status Etcd.service
```

Check Etcd cluster health

```shell
$ etcdctl cluster-health
member 3adf2673436aa824 is healthy: got healthy result from http://${etcd_host_ip1}:2379
member 85ffe9aafb7745cc is healthy: got healthy result from http://${etcd_host_ip2}:2379
member b3d05464c356441a is healthy: got healthy result from http://${etcd_host_ip3}:2379
cluster is healthy

$ etcdctl member list
3adf2673436aa824: name=etcdnode3 peerURLs=http://${etcd_host_ip1}:2380 clientURLs=http://${etcd_host_ip1}:2379 isLeader=false
85ffe9aafb7745cc: name=etcdnode2 peerURLs=http://${etcd_host_ip2}:2380 clientURLs=http://${etcd_host_ip2}:2379 isLeader=false
b3d05464c356441a: name=etcdnode1 peerURLs=http://${etcd_host_ip3}:2380 clientURLs=http://${etcd_host_ip3}:2379 isLeader=true
```



### Calico Installation

Calico creates and manages a flat three-tier network, and each container is assigned a routable IP address.

We are listing the steps here for your convenience.
You can also choose alternatives like Flannel, OVS or others.

To install Calico on specified servers, we can run Submarine-installer/install.sh

```
systemctl start calico-node.service
systemctl status calico-node.service
```

#### Check Calico Network

```shell
# Run the following command to show all host status in the cluster except localhost.
$ calicoctl node status
Calico process is running.

IPv4 BGP status
+---------------+-------------------+-------+------------+-------------+
| PEER ADDRESS  |     PEER TYPE     | STATE |   SINCE    |    INFO     |
+---------------+-------------------+-------+------------+-------------+
| ${host_ip1} | node-to-node mesh | up    | 2018-09-21 | Established |
| ${host_ip2} | node-to-node mesh | up    | 2018-09-21 | Established |
| ${host_ip3} | node-to-node mesh | up    | 2018-09-21 | Established |
+---------------+-------------------+-------+------------+-------------+

IPv6 BGP status
No IPv6 peers found.
```

Create containers to validate calico network

```
docker network create --driver calico --ipam-driver calico-ipam calico-network
docker run --net calico-network --name workload-A -tid busybox
docker run --net calico-network --name workload-B -tid busybox
docker exec workload-A ping workload-B
```


## Hadoop Installation

### Get Hadoop Release
You can either get Hadoop release binary or compile from source code. Please follow the https://hadoop.apache.org/ guides.


### Start YARN service

```
YARN_LOGFILE=resourcemanager.log ./sbin/yarn-daemon.sh start resourcemanager
YARN_LOGFILE=nodemanager.log ./sbin/yarn-daemon.sh start nodemanager
YARN_LOGFILE=timeline.log ./sbin/yarn-daemon.sh start timelineserver
YARN_LOGFILE=mr-historyserver.log ./sbin/mr-jobhistory-daemon.sh start historyserver
```

### Start YARN registry DNS service

```
sudo YARN_LOGFILE=registrydns.log ./yarn-daemon.sh start registrydns
```

### Test with a MR wordcount job

```
./bin/hadoop jar /home/hadoop/hadoop-current/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.0-SNAPSHOT.jar wordcount /tmp/wordcount.txt /tmp/wordcount-output4
```



## Tensorflow Job with CPU

### Standalone Mode

#### Clean up apps with the same name

Suppose we want to submit a TensorFlow job named standalone-tf, destroy any application with the same name and clean up historical job directories.

```bash
./bin/yarn app -destroy standalone-tf
./bin/hdfs dfs -rmr hdfs://${dfs_name_service}/tmp/cifar-10-jobdir
```
where ${dfs_name_service} is the HDFS name service you use

#### Run a standalone tensorflow job

```bash
./bin/yarn jar /home/hadoop/hadoop-current/share/hadoop/yarn/hadoop-yarn-submarine-3.2.0-SNAPSHOT.jar job run \
 --env DOCKER_JAVA_HOME=/opt/java \
 --env DOCKER_HADOOP_HDFS_HOME=/hadoop-current --name standalone-tf \
 --docker_image tf-1.13.1-cpu:0.0.1 \
 --input_path hdfs://${dfs_name_service}/tmp/cifar-10-data \
 --checkpoint_path hdfs://${dfs_name_service}/user/hadoop/tf-checkpoint \
 --worker_resources memory=4G,vcores=2 --verbose \
 --worker_launch_cmd "python /test/cifar10_estimator/cifar10_main.py --data-dir=%input_path% --job-dir=%checkpoint_path% --train-steps=500 --eval-batch-size=16 --train-batch-size=16 --num-gpus=0"
```

### Distributed Mode

#### Clean up apps with the same name

```bash
./bin/yarn app -destroy distributed-tf
./bin/hdfs dfs -rmr hdfs://${dfs_name_service}/tmp/cifar-10-jobdir
```

#### Run a distributed TensorFlow job

```bash
./bin/yarn jar /home/hadoop/hadoop-current/share/hadoop/yarn/hadoop-yarn-submarine-3.2.0-SNAPSHOT.jar job run \
 --env DOCKER_JAVA_HOME=/opt/java \
 --env DOCKER_HADOOP_HDFS_HOME=/hadoop-current --name distributed-tf \
 --env YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK=calico-network \
 --docker_image tf-1.13.1-cpu:0.0.1 \
 --input_path hdfs://${dfs_name_service}/tmp/cifar-10-data \
 --checkpoint_path hdfs://${dfs_name_service}/user/hadoop/tf-distributed-checkpoint \
 --worker_resources memory=4G,vcores=2 --verbose \
 --num_ps 1 \
 --ps_resources memory=4G,vcores=2 \
 --ps_launch_cmd "python /test/cifar10_estimator/cifar10_main.py --data-dir=%input_path% --job-dir=%checkpoint_path% --num-gpus=0" \
 --num_workers 4 \
 --worker_launch_cmd "python /test/cifar10_estimator/cifar10_main.py --data-dir=%input_path% --job-dir=%checkpoint_path% --train-steps=500 --eval-batch-size=16 --train-batch-size=16 --sync --num-gpus=0"
```


## TensorFlow Job with GPU

### GPU configurations for both ResourceManager and NodeManager

Add the YARN resource configuration file, named resource-types.xml

   ```
   <configuration>
     <property>
       <name>yarn.resource-types</name>
       <value>yarn.io/gpu</value>
     </property>
   </configuration>
   ```

#### GPU configurations for ResourceManager

The scheduler used by ResourceManager must be the capacity scheduler, and yarn.scheduler.capacity.resource-calculator in capacity-scheduler.xml should be DominantResourceCalculator

   ```
   <configuration>
     <property>
       <name>yarn.scheduler.capacity.resource-calculator</name>
       <value>org.apache.hadoop.yarn.util.resource.DominantResourceCalculator</value>
     </property>
   </configuration>
   ```

#### GPU configurations for NodeManager

Add configurations in yarn-site.xml

   ```
   <configuration>
     <property>
       <name>yarn.nodemanager.resource-plugins</name>
       <value>yarn.io/gpu</value>
     </property>
     <!--Use nvidia docker v2-->
     <property>
        <name>yarn.nodemanager.resource-plugins.gpu.docker-plugin</name>
        <value>nvidia-docker-v2</value>
     </property>
   </configuration>
   ```

Add configurations to container-executor.cfg

   ```
   [docker]
   ...
   # Add configurations in `[docker]` part：
   # /usr/bin/nvidia-docker is the path of nvidia-docker command
   # nvidia_driver_375.26 means that nvidia driver version is <version>. nvidia-smi command can be used to check the version
   docker.allowed.volume-drivers=/usr/bin/nvidia-docker
   docker.allowed.devices=/dev/nvidiactl,/dev/nvidia-uvm,/dev/nvidia-uvm-tools,/dev/nvidia1,/dev/nvidia0
   docker.allowed.ro-mounts=nvidia_driver_<version>
   # Use nvidia docker v2
   docker.allowed.runtimes=nvidia

   [gpu]
   module.enabled=true

   [cgroups]
   # /sys/fs/cgroup is the cgroup mount destination
   # /hadoop-yarn is the path yarn creates by default
   root=/sys/fs/cgroup
   yarn-hierarchy=/hadoop-yarn
   ```

### Run a distributed TensorFlow GPU job

```bash
 ./yarn jar /home/hadoop/hadoop-current/share/hadoop/yarn/hadoop-yarn-submarine-3.2.0-SNAPSHOT.jar job run \
 --env DOCKER_JAVA_HOME=/opt/java \
 --env DOCKER_HADOOP_HDFS_HOME=/hadoop-current --name distributed-tf-gpu \
 --env YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK=calico-network \
 --docker_image tf-1.13.1-gpu:0.0.1 \
 --input_path hdfs://${dfs_name_service}/tmp/cifar-10-data \
 --checkpoint_path hdfs://${dfs_name_service}/user/hadoop/tf-distributed-checkpoint \
 --num_ps 0 \
 --ps_resources memory=4G,vcores=2,gpu=0 \
 --ps_launch_cmd "python /test/cifar10_estimator/cifar10_main.py --data-dir=%input_path% --job-dir=%checkpoint_path% --num-gpus=0" \
 --worker_resources memory=4G,vcores=2,gpu=1 --verbose \
 --num_workers 1 \
 --worker_launch_cmd "python /test/cifar10_estimator/cifar10_main.py --data-dir=%input_path% --job-dir=%checkpoint_path% --train-steps=500 --eval-batch-size=16 --train-batch-size=16 --sync --num-gpus=1"
```

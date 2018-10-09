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

### Operating System

The operating system and kernel versions we used are as shown in the following table, which should be minimum required versions:

| Enviroment | Verion |
| ------ | ------ |
| Operating System | centos-release-7-3.1611.el7.centos.x86_64 |
| Kernal | 3.10.0-514.el7.x86_64 |

### User & Group

As there are some specific users and groups need to be created to install hadoop/docker. Please create them if they are missing.

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

Check the version of GCC tool

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
wget http://vault.centos.org/7.3.1611/os/x86_64/Packages/kernel-headers-3.10.0-514.el7.x86_64.rpm
rpm -ivh kernel-headers-3.10.0-514.el7.x86_64.rpm
```

### GPU Servers

```
lspci | grep -i nvidia

# If the server has gpus, you can get info like this：
04:00.0 3D controller: NVIDIA Corporation Device 1b38 (rev a1)
82:00.0 3D controller: NVIDIA Corporation Device 1b38 (rev a1)
```



### Nvidia Driver Installation

If nvidia driver/cuda has been installed before, They should be uninstalled firstly.

```
# uninstall cuda：
sudo /usr/local/cuda-10.0/bin/uninstall_cuda_10.0.pl

# uninstall nvidia-driver：
sudo /usr/bin/nvidia-uninstall
```

To check GPU version, install nvidia-detect

```
yum install nvidia-detect
# run 'nvidia-detect -v' to get reqired nvidia driver version：
nvidia-detect -v
Probing for supported NVIDIA devices...
[10de:13bb] NVIDIA Corporation GM107GL [Quadro K620]
This device requires the current 390.87 NVIDIA driver kmod-nvidia
[8086:1912] Intel Corporation HD Graphics 530
An Intel display controller was also detected
```

Pay attention to `This device requires the current 390.87 NVIDIA driver kmod-nvidia`.
Download the installer [NVIDIA-Linux-x86_64-390.87.run](https://www.nvidia.com/object/linux-amd64-display-archive.html).


Some preparatory work for nvidia driver installation

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


Check nvidia driver installation

```
nvidia-smi
```

Reference：
https://docs.nvidia.com/cuda/cuda-installation-guide-linux/index.html



### Docker Installation

```
yum -y update
yum -y install yum-utils
yum-config-manager --add-repo https://yum.dockerproject.org/repo/main/centos/7
yum -y update

# Show available packages
yum search --showduplicates docker-engine

# Install docker 1.12.5
yum -y --nogpgcheck install docker-engine-1.12.5*
systemctl start docker

chown hadoop:netease /var/run/docker.sock
chown hadoop:netease /usr/bin/docker
```

Reference：https://docs.docker.com/cs-engine/1.12/

### Docker Configuration

Add a file, named daemon.json, under the path of /etc/docker/. Please replace the variables of image_registry_ip, etcd_host_ip, localhost_ip, yarn_dns_registry_host_ip, dns_host_ip with specific ips according to your environments.

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



### Docker EE version

```bash
$ docker version

Client:
 Version:      1.12.5
 API version:  1.24
 Go version:   go1.6.4
 Git commit:   7392c3b
 Built:        Fri Dec 16 02:23:59 2016
 OS/Arch:      linux/amd64

Server:
 Version:      1.12.5
 API version:  1.24
 Go version:   go1.6.4
 Git commit:   7392c3b
 Built:        Fri Dec 16 02:23:59 2016
 OS/Arch:      linux/amd64
```

### Nvidia-docker Installation

Submarine is based on nvidia-docker 1.0 version

```
wget -P /tmp https://github.com/NVIDIA/nvidia-docker/releases/download/v1.0.1/nvidia-docker-1.0.1-1.x86_64.rpm
sudo rpm -i /tmp/nvidia-docker*.rpm
# Start nvidia-docker
sudo systemctl start nvidia-docker

# Check nvidia-docker status：
systemctl status nvidia-docker

# Check nvidia-docker log：
journalctl -u nvidia-docker

# Test nvidia-docker-plugin
curl http://localhost:3476/v1.0/docker/cli
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
nvidia-docker run --rm nvidia/cuda:9.0-devel nvidia-smi
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

[The way to uninstall nvidia-docker 1.0](https://github.com/nvidia/nvidia-docker/wiki/Installation-(version-2.0))

Reference:
https://github.com/NVIDIA/nvidia-docker/tree/1.0



### Tensorflow Image

There is no need to install CUDNN and CUDA on the servers, because CUDNN and CUDA can be added in the docker images. we can get basic docker images by following WriteDockerfile.md.


The basic Dockerfile doesn't support kerberos security. if you need kerberos, you can get write a Dockerfile like this


```shell
FROM nvidia/cuda:9.0-cudnn7-devel-ubuntu16.04

# Pick up some TF dependencies
RUN apt-get update && apt-get install -y --allow-downgrades --no-install-recommends \
        build-essential \
        cuda-command-line-tools-9-0 \
        cuda-cublas-9-0 \
        cuda-cufft-9-0 \
        cuda-curand-9-0 \
        cuda-cusolver-9-0 \
        cuda-cusparse-9-0 \
        curl \
        libcudnn7=7.0.5.15-1+cuda9.0 \
        libfreetype6-dev \
        libpng12-dev \
        libzmq3-dev \
        pkg-config \
        python \
        python-dev \
        rsync \
        software-properties-common \
        unzip \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN export DEBIAN_FRONTEND=noninteractive && apt-get update && apt-get install -yq krb5-user libpam-krb5 && apt-get clean

RUN curl -O https://bootstrap.pypa.io/get-pip.py && \
    python get-pip.py && \
    rm get-pip.py

RUN pip --no-cache-dir install \
        Pillow \
        h5py \
        ipykernel \
        jupyter \
        matplotlib \
        numpy \
        pandas \
        scipy \
        sklearn \
        && \
    python -m ipykernel.kernelspec

# Install TensorFlow GPU version.
RUN pip --no-cache-dir install \
    http://storage.googleapis.com/tensorflow/linux/gpu/tensorflow_gpu-1.8.0-cp27-none-linux_x86_64.whl
RUN apt-get update && apt-get install git -y

RUN apt-get update && apt-get install -y openjdk-8-jdk wget
# Downloadhadoop-3.1.1.tar.gz
RUN wget http://mirrors.hust.edu.cn/apache/hadoop/common/hadoop-3.1.1/hadoop-3.1.1.tar.gz
RUN tar zxf hadoop-3.1.1.tar.gz
RUN mv hadoop-3.1.1 hadoop-3.1.0

# Download jdk which supports kerberos
RUN wget -qO jdk8.tar.gz 'http://${kerberos_jdk_url}/jdk-8u152-linux-x64.tar.gz'
RUN tar xzf jdk8.tar.gz -C /opt
RUN mv /opt/jdk* /opt/java
RUN rm jdk8.tar.gz
RUN update-alternatives --install /usr/bin/java java /opt/java/bin/java 100
RUN update-alternatives --install /usr/bin/javac javac /opt/java/bin/javac 100

ENV JAVA_HOME /opt/java
ENV PATH $PATH:$JAVA_HOME/bin
```


### Test tensorflow in a docker container

After docker image is built, we can check
tensorflow environments before submitting a yarn job.

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

To install Etcd on specified servers, we can run Submarine/install.sh

```shell
$ ./Submarine/install.sh
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

To install Calico on specified servers, we can run Submarine/install.sh

```
systemctl start calico-node.service
systemctl status calico-node.service
```

#### Check Calico Network

```shell
# Run the following command to show the all host status in the cluster except localhost.
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

### Compile hadoop source code

```
mvn package -Pdist -DskipTests -Dtar
```


### Start yarn service

```
YARN_LOGFILE=resourcemanager.log ./sbin/yarn-daemon.sh start resourcemanager
YARN_LOGFILE=nodemanager.log ./sbin/yarn-daemon.sh start nodemanager
YARN_LOGFILE=timeline.log ./sbin/yarn-daemon.sh start timelineserver
YARN_LOGFILE=mr-historyserver.log ./sbin/mr-jobhistory-daemon.sh start historyserver
```

### Start yarn registery dns service

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

Suppose we want to submit a tensorflow job named standalone-tf, destroy any application with the same name and clean up historical job directories.

```bash
./bin/yarn app -destroy standalone-tf
./bin/hdfs dfs -rmr hdfs://${dfs_name_service}/tmp/cifar-10-jobdir
```
where ${dfs_name_service} is the hdfs name service you use

#### Run a standalone tensorflow job

```bash
./bin/yarn jar /home/hadoop/hadoop-current/share/hadoop/yarn/hadoop-yarn-submarine-3.2.0-SNAPSHOT.jar job run \
 --env DOCKER_JAVA_HOME=/opt/java \
 --env DOCKER_HADOOP_HDFS_HOME=/hadoop-3.1.0 --name standalone-tf \
 --docker_image dockerfile-cpu-tf1.8.0-with-models \
 --input_path hdfs://${dfs_name_service}/tmp/cifar-10-data \
 --checkpoint_path hdfs://${dfs_name_service}/user/hadoop/tf-checkpoint \
 --worker_resources memory=4G,vcores=2 --verbose \
 --worker_launch_cmd "python /test/cifar10_estimator/cifar10_main.py --data-dir=hdfs://${dfs_name_service}/tmp/cifar-10-data --job-dir=hdfs://${dfs_name_service}/tmp/cifar-10-jobdir --train-steps=500 --eval-batch-size=16 --train-batch-size=16 --num-gpus=0"
```

### Distributed Mode

#### Clean up apps with the same name

```bash
./bin/yarn app -destroy distributed-tf
./bin/hdfs dfs -rmr hdfs://${dfs_name_service}/tmp/cifar-10-jobdir
```

#### Run a distributed tensorflow job

```bash
./bin/yarn jar /home/hadoop/hadoop-current/share/hadoop/yarn/hadoop-yarn-submarine-3.2.0-SNAPSHOT.jar job run \
 --env DOCKER_JAVA_HOME=/opt/java \
 --env DOCKER_HADOOP_HDFS_HOME=/hadoop-3.1.0 --name distributed-tf \
 --env YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK=calico-network \
 --docker_image dockerfile-cpu-tf1.8.0-with-models \
 --input_path hdfs://${dfs_name_service}/tmp/cifar-10-data \
 --checkpoint_path hdfs://${dfs_name_service}/user/hadoop/tf-distributed-checkpoint \
 --worker_resources memory=4G,vcores=2 --verbose \
 --num_ps 1 \
 --ps_resources memory=4G,vcores=2 \
 --ps_launch_cmd "python /test/cifar10_estimator/cifar10_main.py --data-dir=hdfs://${dfs_name_service}/tmp/cifar-10-data --job-dir=hdfs://${dfs_name_service}/tmp/cifar-10-jobdir --num-gpus=0" \
 --num_workers 4 \
 --worker_launch_cmd "python /test/cifar10_estimator/cifar10_main.py --data-dir=hdfs://${dfs_name_service}/tmp/cifar-10-data --job-dir=hdfs://${dfs_name_service}/tmp/cifar-10-jobdir --train-steps=500 --eval-batch-size=16 --train-batch-size=16 --sync --num-gpus=0"
```


## Tensorflow Job with GPU

### GPU configurations for both resourcemanager and nodemanager

Add the yarn resource configuration file, named resource-types.xml

   ```
   <configuration>
     <property>
       <name>yarn.resource-types</name>
       <value>yarn.io/gpu</value>
     </property>
   </configuration>
   ```

#### GPU configurations for resourcemanager

The scheduler used by resourcemanager must be  capacity scheduler, and yarn.scheduler.capacity.resource-calculator in  capacity-scheduler.xml should be DominantResourceCalculator

   ```
   <configuration>
     <property>
       <name>yarn.scheduler.capacity.resource-calculator</name>
       <value>org.apache.hadoop.yarn.util.resource.DominantResourceCalculator</value>
     </property>
   </configuration>
   ```

#### GPU configurations for nodemanager

Add configurations in yarn-site.xml

   ```
   <configuration>
     <property>
       <name>yarn.nodemanager.resource-plugins</name>
       <value>yarn.io/gpu</value>
     </property>
   </configuration>
   ```

Add configurations in container-executor.cfg

   ```
   [docker]
   ...
   # Add configurations in `[docker]` part：
   # /usr/bin/nvidia-docker is the path of nvidia-docker command
   # nvidia_driver_375.26 means that nvidia driver version is 375.26. nvidia-smi command can be used to check the version
   docker.allowed.volume-drivers=/usr/bin/nvidia-docker
   docker.allowed.devices=/dev/nvidiactl,/dev/nvidia-uvm,/dev/nvidia-uvm-tools,/dev/nvidia1,/dev/nvidia0
   docker.allowed.ro-mounts=nvidia_driver_375.26

   [gpu]
   module.enabled=true

   [cgroups]
   # /sys/fs/cgroup is the cgroup mount destination
   # /hadoop-yarn is the path yarn creates by default
   root=/sys/fs/cgroup
   yarn-hierarchy=/hadoop-yarn
   ```

#### Test with a tensorflow job

Distributed-shell + GPU + cgroup

```bash
 ./yarn jar /home/hadoop/hadoop-current/share/hadoop/yarn/hadoop-yarn-submarine-3.2.0-SNAPSHOT.jar job run \
 --env DOCKER_JAVA_HOME=/opt/java \
 --env DOCKER_HADOOP_HDFS_HOME=/hadoop-3.1.0 --name distributed-tf-gpu \
 --env YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK=calico-network \
 --docker_image gpu-cuda9.0-tf1.8.0-with-models \
 --input_path hdfs://${dfs_name_service}/tmp/cifar-10-data \
 --checkpoint_path hdfs://${dfs_name_service}/user/hadoop/tf-distributed-checkpoint \
 --num_ps 0 \
 --ps_resources memory=4G,vcores=2,gpu=0 \
 --ps_launch_cmd "python /test/cifar10_estimator/cifar10_main.py --data-dir=hdfs://${dfs_name_service}/tmp/cifar-10-data --job-dir=hdfs://${dfs_name_service}/tmp/cifar-10-jobdir --num-gpus=0" \
 --worker_resources memory=4G,vcores=2,gpu=1 --verbose \
 --num_workers 1 \
 --worker_launch_cmd "python /test/cifar10_estimator/cifar10_main.py --data-dir=hdfs://${dfs_name_service}/tmp/cifar-10-data --job-dir=hdfs://${dfs_name_service}/tmp/cifar-10-jobdir --train-steps=500 --eval-batch-size=16 --train-batch-size=16 --sync --num-gpus=1"
```



## Issues:

### Issue 1: Fail to start nodemanager after system reboot

```
2018-09-20 18:54:39,785 ERROR org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor: Failed to bootstrap configured resource subsystems!
org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerException: Unexpected: Cannot create yarn cgroup Subsystem:cpu Mount points:/proc/mounts User:yarn Path:/sys/fs/cgroup/cpu,cpuacct/hadoop-yarn
  at org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandlerImpl.initializePreMountedCGroupController(CGroupsHandlerImpl.java:425)
  at org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsHandlerImpl.initializeCGroupController(CGroupsHandlerImpl.java:377)
  at org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsCpuResourceHandlerImpl.bootstrap(CGroupsCpuResourceHandlerImpl.java:98)
  at org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.CGroupsCpuResourceHandlerImpl.bootstrap(CGroupsCpuResourceHandlerImpl.java:87)
  at org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.resources.ResourceHandlerChain.bootstrap(ResourceHandlerChain.java:58)
  at org.apache.hadoop.yarn.server.nodemanager.LinuxContainerExecutor.init(LinuxContainerExecutor.java:320)
  at org.apache.hadoop.yarn.server.nodemanager.NodeManager.serviceInit(NodeManager.java:389)
  at org.apache.hadoop.service.AbstractService.init(AbstractService.java:164)
  at org.apache.hadoop.yarn.server.nodemanager.NodeManager.initAndStartNodeManager(NodeManager.java:929)
  at org.apache.hadoop.yarn.server.nodemanager.NodeManager.main(NodeManager.java:997)
2018-09-20 18:54:39,789 INFO org.apache.hadoop.service.AbstractService: Service NodeManager failed in state INITED
```

Solution: Grant user yarn the access to  `/sys/fs/cgroup/cpu,cpuacct`, which is the subfolder of cgroup mount destination.

```
chown :yarn -R /sys/fs/cgroup/cpu,cpuacct
chmod g+rwx -R /sys/fs/cgroup/cpu,cpuacct
```

If GPUs are used，the access to cgroup devices folder is neede as well

```
chown :yarn -R /sys/fs/cgroup/devices
chmod g+rwx -R /sys/fs/cgroup/devices
```


### Issue 2: container-executor permission denied

```
2018-09-21 09:36:26,102 WARN org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor: IOException executing command:
java.io.IOException: Cannot run program "/etc/yarn/sbin/Linux-amd64-64/container-executor": error=13, Permission denied
        at java.lang.ProcessBuilder.start(ProcessBuilder.java:1048)
        at org.apache.hadoop.util.Shell.runCommand(Shell.java:938)
        at org.apache.hadoop.util.Shell.run(Shell.java:901)
        at org.apache.hadoop.util.Shell$ShellCommandExecutor.execute(Shell.java:1213)
```

Solution: The permission of `/etc/yarn/sbin/Linux-amd64-64/container-executor` should be 6050

### Issue 3：How to get docker service log

Solution: we can get docker log with the following command

```
journalctl -u docker
```

### Issue 4：docker can't remove containers with errors like `device or resource busy`

```bash
$ docker rm 0bfafa146431
Error response from daemon: Unable to remove filesystem for 0bfafa146431771f6024dcb9775ef47f170edb2f1852f71916ba44209ca6120a: remove /app/docker/containers/0bfafa146431771f6024dcb9775ef47f170edb2f152f71916ba44209ca6120a/shm: device or resource busy
```

Solution: to find which process leads to a `device or resource busy`, we can add a shell script, named `find-busy-mnt.sh`

```bash
#!/bin/bash

# A simple script to get information about mount points and pids and their
# mount namespaces.

if [ $# -ne 1 ];then
echo "Usage: $0 <devicemapper-device-id>"
exit 1
fi

ID=$1

MOUNTS=`find /proc/*/mounts | xargs grep $ID 2>/dev/null`

[ -z "$MOUNTS" ] &&  echo "No pids found" && exit 0

printf "PID\tNAME\t\tMNTNS\n"
echo "$MOUNTS" | while read LINE; do
PID=`echo $LINE | cut -d ":" -f1 | cut -d "/" -f3`
# Ignore self and thread-self
if [ "$PID" == "self" ] || [ "$PID" == "thread-self" ]; then
  continue
fi
NAME=`ps -q $PID -o comm=`
MNTNS=`readlink /proc/$PID/ns/mnt`
printf "%s\t%s\t\t%s\n" "$PID" "$NAME" "$MNTNS"
done
```

Kill the process by pid, which is found by the script

```bash
$ chmod +x find-busy-mnt.sh
./find-busy-mnt.sh 0bfafa146431771f6024dcb9775ef47f170edb2f152f71916ba44209ca6120a
# PID   NAME            MNTNS
# 5007  ntpd            mnt:[4026533598]
$ kill -9 5007
```


### Issue 5：Failed to execute `sudo nvidia-docker run`

```
docker: Error response from daemon: create nvidia_driver_361.42: VolumeDriver.Create: internal error, check logs for details.
See 'docker run --help'.
```

Solution:

```
#check nvidia-docker status
$ systemctl status nvidia-docker
$ journalctl -n -u nvidia-docker
#restart nvidia-docker
systemctl stop nvidia-docker
systemctl start nvidia-docker
```

### Issue 6：Yarn failed to start containers

if the number of GPUs required by applications is larger than the number of GPUs in the cluster, there would be some containers can't be created.


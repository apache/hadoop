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

# Submarine 安装说明

## Prerequisites

### 操作系统

我们使用的操作系统版本是 centos-release-7-3.1611.el7.centos.x86_64, 内核版本是 3.10.0-514.el7.x86_64 ，应该是最低版本了。

| Enviroment | Verion |
| ------ | ------ |
| Operating System | centos-release-7-3.1611.el7.centos.x86_64 |
| Kernal | 3.10.0-514.el7.x86_64 |

### User & Group

如果操作系统中没有这些用户组和用户，必须添加。一部分用户是 hadoop 运行需要，一部分用户是 docker 运行需要。

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

### GCC 版本

```bash
gcc --version
gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-11)
# 如果没有安装请执行以下命令进行安装
yum install gcc make g++
```

### Kernel header & devel

```bash
# 方法一：
yum install kernel-devel-$(uname -r) kernel-headers-$(uname -r)
# 方法二：
wget http://vault.centos.org/7.3.1611/os/x86_64/Packages/kernel-headers-3.10.0-514.el7.x86_64.rpm
rpm -ivh kernel-headers-3.10.0-514.el7.x86_64.rpm
```

### 检查 GPU 版本

```
lspci | grep -i nvidia

# 如果什么都没输出，就说明显卡不对，以下是我的输出：
# 04:00.0 3D controller: NVIDIA Corporation Device 1b38 (rev a1)
# 82:00.0 3D controller: NVIDIA Corporation Device 1b38 (rev a1)
```



### 安装 nvidia 驱动

安装nvidia driver/cuda要确保已安装的nvidia driver/cuda已被清理

```
# 卸载cuda：
sudo /usr/local/cuda-10.0/bin/uninstall_cuda_10.0.pl

# 卸载nvidia-driver：
sudo /usr/bin/nvidia-uninstall
```

安装nvidia-detect，用于检查显卡版本

```
yum install nvidia-detect
# 运行命令 nvidia-detect -v 返回结果：
nvidia-detect -v
Probing for supported NVIDIA devices...
[10de:13bb] NVIDIA Corporation GM107GL [Quadro K620]
This device requires the current 390.87 NVIDIA driver kmod-nvidia
[8086:1912] Intel Corporation HD Graphics 530
An Intel display controller was also detected
```

注意这里的信息 [Quadro K620] 和390.87。
下载 [NVIDIA-Linux-x86_64-390.87.run](https://www.nvidia.com/object/linux-amd64-display-archive.html)


安装前的一系列准备工作

```
# 若系统很久没更新，这句可能耗时较长
yum -y update
yum -y install kernel-devel

yum -y install epel-release
yum -y install dkms

# 禁用nouveau
vim /etc/default/grub  #在“GRUB_CMDLINE_LINUX”中添加内容 rd.driver.blacklist=nouveau nouveau.modeset=0
grub2-mkconfig -o /boot/grub2/grub.cfg # 生成配置
vim /etc/modprobe.d/blacklist.conf # 打开（新建）文件，添加内容blacklist nouveau

mv /boot/initramfs-$(uname -r).img /boot/initramfs-$(uname -r)-nouveau.img
dracut /boot/initramfs-$(uname -r).img $(uname -r)   # 更新配置，并重启
reboot
```

开机后确认是否禁用

```
lsmod | grep nouveau  # 应该返回空

# 开始安装
sh NVIDIA-Linux-x86_64-390.87.run
```

安装过程中，会遇到一些选项：

```
Install NVIDIA's 32-bit compatibility libraries (Yes)
centos Install NVIDIA's 32-bit compatibility libraries (Yes)
Would you like to run the nvidia-xconfig utility to automatically update your X configuration file... (NO)
```


最后查看 nvidia gpu 状态

```
nvidia-smi
```

reference：
https://docs.nvidia.com/cuda/cuda-installation-guide-linux/index.html



### 安装 Docker

```
yum -y update
yum -y install yum-utils
yum-config-manager --add-repo https://yum.dockerproject.org/repo/main/centos/7
yum -y update

# 显示 available 的安装包
yum search --showduplicates docker-engine

# 安装 1.12.5 版本 docker
yum -y --nogpgcheck install docker-engine-1.12.5*
systemctl start docker

chown hadoop:netease /var/run/docker.sock
chown hadoop:netease /usr/bin/docker
```

Reference：https://docs.docker.com/cs-engine/1.12/

### 配置 Docker

在 `/etc/docker/` 目录下，创建`daemon.json`文件, 添加以下配置，变量如image_registry_ip, etcd_host_ip, localhost_ip, yarn_dns_registry_host_ip, dns_host_ip需要根据具体环境，进行修改

```
{
    "insecure-registries": ["${image_registry_ip}:5000"],
    "cluster-store":"etcd://${etcd_host_ip1}:2379,${etcd_host_ip2}:2379,${etcd_host_ip3}:2379",
    "cluster-advertise":"{localhost_ip}:2375",
    "dns": ["${yarn_dns_registry_host_ip}", "${dns_host_ip1}"],
    "hosts": ["tcp://{localhost_ip}:2375", "unix:///var/run/docker.sock"]
}
```

重启 docker daemon：

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

### 安装 nvidia-docker

Hadoop-3.2 的 submarine 使用的是 1.0 版本的 nvidia-docker

```
wget -P /tmp https://github.com/NVIDIA/nvidia-docker/releases/download/v1.0.1/nvidia-docker-1.0.1-1.x86_64.rpm
sudo rpm -i /tmp/nvidia-docker*.rpm
# 启动 nvidia-docker
sudo systemctl start nvidia-docker

# 查看 nvidia-docker 状态：
systemctl status nvidia-docker

# 查看 nvidia-docker 日志：
journalctl -u nvidia-docker

# 查看 nvidia-docker-plugin 是否正常
curl http://localhost:3476/v1.0/docker/cli
```

在 `/var/lib/nvidia-docker/volumes/nvidia_driver/` 路径下，根据 `nvidia-driver` 的版本创建文件夹：

```
mkdir /var/lib/nvidia-docker/volumes/nvidia_driver/390.87
# 其中390.87是nvidia driver的版本号

mkdir /var/lib/nvidia-docker/volumes/nvidia_driver/390.87/bin
mkdir /var/lib/nvidia-docker/volumes/nvidia_driver/390.87/lib64

cp /usr/bin/nvidia* /var/lib/nvidia-docker/volumes/nvidia_driver/390.87/bin
cp /usr/lib64/libcuda* /var/lib/nvidia-docker/volumes/nvidia_driver/390.87/lib64
cp /usr/lib64/libnvidia* /var/lib/nvidia-docker/volumes/nvidia_driver/390.87/lib64

# Test nvidia-smi
nvidia-docker run --rm nvidia/cuda:9.0-devel nvidia-smi
```

测试 docker, nvidia-docker, nvidia-driver 安装

```
# 测试一
nvidia-docker run -rm nvidia/cuda nvidia-smi
```

```
# 测试二
nvidia-docker run -it tensorflow/tensorflow:1.9.0-gpu bash
# 在docker中执行
python
import tensorflow as tf
tf.test.is_gpu_available()
```

卸载 nvidia-docker 1.0 的方法：
https://github.com/nvidia/nvidia-docker/wiki/Installation-(version-2.0)

reference:
https://github.com/NVIDIA/nvidia-docker/tree/1.0



### Tensorflow Image

CUDNN 和 CUDA 其实不需要在物理机上安装，因为 Sumbmarine 中提供了已经包含了CUDNN 和 CUDA 的镜像文件，基础的Dockfile可参见WriteDockerfile.md


上述images无法支持kerberos环境，如果需要kerberos可以使用如下Dockfile

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
# 下载 hadoop-3.1.1.tar.gz
RUN wget http://mirrors.hust.edu.cn/apache/hadoop/common/hadoop-3.1.1/hadoop-3.1.1.tar.gz
RUN tar zxf hadoop-3.1.1.tar.gz
RUN mv hadoop-3.1.1 hadoop-3.1.0

# 下载支持kerberos的jdk安装包
RUN wget -qO jdk8.tar.gz 'http://${kerberos_jdk_url}/jdk-8u152-linux-x64.tar.gz'
RUN tar xzf jdk8.tar.gz -C /opt
RUN mv /opt/jdk* /opt/java
RUN rm jdk8.tar.gz
RUN update-alternatives --install /usr/bin/java java /opt/java/bin/java 100
RUN update-alternatives --install /usr/bin/javac javac /opt/java/bin/javac 100

ENV JAVA_HOME /opt/java
ENV PATH $PATH:$JAVA_HOME/bin
```


### 测试 TF 环境

创建好 docker 镜像后，需要先手动检查 TensorFlow 是否可以正常使用，避免通过 YARN 调度后出现问题，可以执行以下命令

```shell
$ docker run -it ${docker_image_name} /bin/bash
# >>> 进入容器
$ python
$ python >> import tensorflow as tf
$ python >> tf.__version__
```

如果出现问题，可以按照以下路径进行排查

1. 环境变量是否设置正确

   ```
   echo $LD_LIBRARY_PATH
   /usr/local/cuda/extras/CUPTI/lib64:/usr/local/nvidia/lib:/usr/local/nvidia/lib64
   ```

2. libcuda.so.1,libcuda.so是否在LD_LIBRARY_PATH指定的路径中

   ```
   ls -l /usr/local/nvidia/lib64 | grep libcuda.so
   ```

### 安装 Etcd

运行 Submarine/install.sh 脚本，就可以在指定服务器中安装 Etcd 组件和服务自启动脚本。

```shell
$ ./Submarine/install.sh
# 通过如下命令查看 Etcd 服务状态
systemctl status Etcd.service
```

检查 Etcd 服务状态

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
其中，${etcd_host_ip*} 是etcd服务器的ip


### 安装 Calico

运行 Submarine/install.sh 脚本，就可以在指定服务器中安装 Calico 组件和服务自启动脚本。

```
systemctl start calico-node.service
systemctl status calico-node.service
```

#### 检查 Calico 网络

```shell
# 执行如下命令，注意：不会显示本服务器的状态，只显示其他的服务器状态
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

创建docker container，验证calico网络

```
docker network create --driver calico --ipam-driver calico-ipam calico-network
docker run --net calico-network --name workload-A -tid busybox
docker run --net calico-network --name workload-B -tid busybox
docker exec workload-A ping workload-B
```


## 安装 Hadoop

### 编译 Hadoop

```
mvn package -Pdist -DskipTests -Dtar
```



### 启动 YARN服务

```
YARN_LOGFILE=resourcemanager.log ./sbin/yarn-daemon.sh start resourcemanager
YARN_LOGFILE=nodemanager.log ./sbin/yarn-daemon.sh start nodemanager
YARN_LOGFILE=timeline.log ./sbin/yarn-daemon.sh start timelineserver
YARN_LOGFILE=mr-historyserver.log ./sbin/mr-jobhistory-daemon.sh start historyserver
```

### 启动 registery dns 服务

```
sudo YARN_LOGFILE=registrydns.log ./yarn-daemon.sh start registrydns
```



### 测试 wordcount

通过测试最简单的 wordcount ，检查 YARN 是否正确安装

```
./bin/hadoop jar /home/hadoop/hadoop-current/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.0-SNAPSHOT.jar wordcount /tmp/wordcount.txt /tmp/wordcount-output4
```



## 使用CUP的Tensorflow任务

### 单机模式

#### 清理重名程序

```bash
# 每次提交前需要执行：
./bin/yarn app -destroy standalone-tf
# 并删除hdfs路径：
./bin/hdfs dfs -rmr hdfs://${dfs_name_service}/tmp/cifar-10-jobdir
# 确保之前的任务已经结束
```
其中，变量${dfs_name_service}请根据环境，用你的hdfs name service名称替换

#### 执行单机模式的tensorflow任务

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


### 分布式模式

#### 清理重名程序

```bash
# 每次提交前需要执行：
./bin/yarn app -destroy distributed-tf
# 并删除hdfs路径：
./bin/hdfs dfs -rmr hdfs://${dfs_name_service}/tmp/cifar-10-jobdir
# 确保之前的任务已经结束
```

#### 提交分布式模式 tensorflow 任务

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
 --worker_launch_cmd "python /test/cifar10_estimator/cifar10_main.py --data-dir=hdfs://${dfs_name_service}/tmp/cifar-10-data --job-dir=hdfs://${${dfs_name_service}}/tmp/cifar-10-jobdir --train-steps=500 --eval-batch-size=16 --train-batch-size=16 --sync --num-gpus=0"
```


## 使用GPU的Tensorflow任务

### Resourcemanager, Nodemanager 中添加GPU支持

在 yarn 配置文件夹(conf或etc/hadoop)中创建 resource-types.xml，添加：

   ```
   <configuration>
     <property>
       <name>yarn.resource-types</name>
       <value>yarn.io/gpu</value>
     </property>
   </configuration>
   ```

### Resourcemanager 的 GPU 配置

resourcemanager 使用的 scheduler 必须是 capacity scheduler，在 capacity-scheduler.xml 中修改属性：

   ```
   <configuration>
     <property>
       <name>yarn.scheduler.capacity.resource-calculator</name>
       <value>org.apache.hadoop.yarn.util.resource.DominantResourceCalculator</value>
     </property>
   </configuration>
   ```

### Nodemanager 的 GPU 配置

在 nodemanager 的 yarn-site.xml 中添加配置：

   ```
   <configuration>
     <property>
       <name>yarn.nodemanager.resource-plugins</name>
       <value>yarn.io/gpu</value>
     </property>
   </configuration>
   ```

在 container-executor.cfg 中添加配置：

   ```
   [docker]
   ...
   # 在[docker]已有配置中，添加以下内容：
   # /usr/bin/nvidia-docker是nvidia-docker路径
   # nvidia_driver_375.26的版本号375.26，可以使用nvidia-smi查看
   docker.allowed.volume-drivers=/usr/bin/nvidia-docker
   docker.allowed.devices=/dev/nvidiactl,/dev/nvidia-uvm,/dev/nvidia-uvm-tools,/dev/nvidia1,/dev/nvidia0
   docker.allowed.ro-mounts=nvidia_driver_375.26

   [gpu]
   module.enabled=true

   [cgroups]
   # /sys/fs/cgroup是cgroup的mount路径
   # /hadoop-yarn是yarn在cgroup路径下默认创建的path
   root=/sys/fs/cgroup
   yarn-hierarchy=/hadoop-yarn
   ```

### 提交验证

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



## 问题

### 问题一: 操作系统重启导致 nodemanager 启动失败

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

解决方法：使用 `root` 账号给 `yarn` 用户修改 `/sys/fs/cgroup/cpu,cpuacct` 的权限

```
chown :yarn -R /sys/fs/cgroup/cpu,cpuacct
chmod g+rwx -R /sys/fs/cgroup/cpu,cpuacct
```

在支持gpu时，还需cgroup devices路径权限

```
chown :yarn -R /sys/fs/cgroup/devices
chmod g+rwx -R /sys/fs/cgroup/devices
```


### 问题二：container-executor 权限问题

```
2018-09-21 09:36:26,102 WARN org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.privileged.PrivilegedOperationExecutor: IOException executing command:
java.io.IOException: Cannot run program "/etc/yarn/sbin/Linux-amd64-64/container-executor": error=13, Permission denied
        at java.lang.ProcessBuilder.start(ProcessBuilder.java:1048)
        at org.apache.hadoop.util.Shell.runCommand(Shell.java:938)
        at org.apache.hadoop.util.Shell.run(Shell.java:901)
        at org.apache.hadoop.util.Shell$ShellCommandExecutor.execute(Shell.java:1213)
```

`/etc/yarn/sbin/Linux-amd64-64/container-executor` 该文件的权限应为6050

### 问题三：查看系统服务启动日志

```
journalctl -u docker
```

### 问题四：Docker 无法删除容器的问题 `device or resource busy`

```bash
$ docker rm 0bfafa146431
Error response from daemon: Unable to remove filesystem for 0bfafa146431771f6024dcb9775ef47f170edb2f1852f71916ba44209ca6120a: remove /app/docker/containers/0bfafa146431771f6024dcb9775ef47f170edb2f152f71916ba44209ca6120a/shm: device or resource busy
```

编写 `find-busy-mnt.sh` 脚本，检查 `device or resource busy` 状态的容器挂载文件

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

查找占用目录的进程

```bash
$ chmod +x find-busy-mnt.sh
./find-busy-mnt.sh 0bfafa146431771f6024dcb9775ef47f170edb2f152f71916ba44209ca6120a
# PID   NAME            MNTNS
# 5007  ntpd            mnt:[4026533598]
$ kill -9 5007
```


### 问题五：命令sudo nvidia-docker run 报错

```
docker: Error response from daemon: create nvidia_driver_361.42: VolumeDriver.Create: internal error, check logs for details.
See 'docker run --help'.
```

解决方法：

```
#查看nvidia-docker状态，是不是启动有问题，可以使用
$ systemctl status nvidia-docker
$ journalctl -n -u nvidia-docker
#重启下nvidia-docker
systemctl stop nvidia-docker
systemctl start nvidia-docker
```

### 问题六：YARN 启动容器失败

如果你创建的容器数（PS+Work>GPU显卡总数），可能会出现容器创建失败，那是因为在一台服务器上同时创建了超过本机显卡总数的容器。

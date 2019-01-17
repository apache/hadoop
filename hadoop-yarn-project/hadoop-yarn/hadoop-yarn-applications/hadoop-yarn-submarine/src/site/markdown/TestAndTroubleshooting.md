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

#### Test with a tensorflow job

Distributed-shell + GPU + cgroup

```bash
 ./yarn jar /home/hadoop/hadoop-current/share/hadoop/yarn/hadoop-yarn-submarine-3.2.0-SNAPSHOT.jar job run \
 --env DOCKER_JAVA_HOME=/opt/java \
 --env DOCKER_HADOOP_HDFS_HOME=/hadoop-3.1.0 --name distributed-tf-gpu \
 --env YARN_CONTAINER_RUNTIME_DOCKER_CONTAINER_NETWORK=calico-network \
 --worker_docker_image gpu-cuda9.0-tf1.8.0-with-models \
 --ps_docker_image dockerfile-cpu-tf1.8.0-with-models \
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


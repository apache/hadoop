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

# submarine installer

## 项目介绍

介绍 **submarine-installer** 项目之前，首先要说明一下 **Hadoop {Submarine}**  这个项目，**Hadoop {Submarine}**  是 hadoop 3.2 版本中最新发布的机器学习框架子项目，他让 hadoop 支持 `Tensorflow`、`MXNet`、`Caffe`、`Spark` 等多种深度学习框架，提供了机器学习算法开发、分布式模型训练、模型管理和模型发布等全功能的系统框架，结合 hadoop 与身俱来的数据存储和数据处理能力，让数据科学家们能够更好的挖掘和发挥出数据的价值。

hadoop 在 2.9 版本中就已经让 YARN 支持了 Docker 容器的资源调度模式，**Hadoop {Submarine}** 在此基础之上通过 YARN 把分布式深度学习框架以 Docker 容器的方式进行调度和运行起来。

由于分布式深度学习框架需要运行在多个 Docker 的容器之中，并且需要能够让运行在容器之中的各个服务相互协调，完成分布式机器学习的模型训练和模型发布等服务，这其中就会牵涉到 `DNS`、`Docker` 、 `GPU`、`Network`、`显卡`、`操作系统内核` 修改等多个系统工程问题，正确的部署好 **Hadoop {Submarine}**  的运行环境是一件很困难和耗时的事情。

为了降低 hadoop 2.9 以上版本的 docker 等组件的部署难度，所以我们专门开发了这个用来部署 `Hadoop {Submarine} ` 运行时环境的 `submarine-installer` 项目，提供一键安装脚本，也可以分步执行安装、卸载、启动和停止各个组件，同时讲解每一步主要参数配置和注意事项。我们同时还向 hadoop 社区提交了部署 `Hadoop {Submarine} ` 运行时环境的 [中文手册](InstallationGuideChineseVersion.md) 和 [英文手册](InstallationGuide.md) ，帮助用户更容易的部署，发现问题也可以及时解决。

## 先决条件

**submarine-installer** 目前只支持 `centos-release-7-3.1611.el7.centos.x86_64` 以上版本的操作系统中进行使用。

## 配置说明

使用 **submarine-installer** 进行部署之前，你可以参考 [install.conf](install.conf) 文件中已有的配置参数和格式，根据你的使用情况进行如下的参数配置：

+ **DNS 配置项**

  LOCAL_DNS_HOST：服务器端本地 DNS IP 地址配置，可以从 `/etc/resolv.conf` 中查看

  YARN_DNS_HOST：yarn dns server 启动的 IP 地址

+ **ETCD 配置项**

  机器学习是一个计算密度型系统，对数据传输性能要求非常高，所以我们使用了网络效率损耗最小的 ETCD 网络组件，它可以通过 BGP 路由方式支持 overlay 网络，同时在跨机房部署时支持隧道模式。

  你需要选择至少三台以上的服务器作为 ETCD 的运行服务器，这样可以让 `Hadoop {Submarine} ` 有较好的容错性和稳定性。

  在 **ETCD_HOSTS** 配置项中输入作为 ETCD 服务器的IP数组，参数配置一般是这样：

  ETCD_HOSTS=(hostIP1 hostIP2 hostIP3)，注意多个 hostIP 之间请使用空格进行隔开。

+ **DOCKER_REGISTRY 配置项**

  你首先需要安装好一个可用的 docker 的镜像管理仓库，这个镜像仓库用来存放你所需要的各种深度学习框架的镜像文件，然后将镜像仓库的 IP 地址和端口配置进来，参数配置一般是这样：DOCKER_REGISTRY="10.120.196.232:5000"

+ **DOWNLOAD_SERVER 配置项**

  `submarine-installer` 默认都是从网络上直接下载所有的依赖包（例如：GCC、Docker、Nvidia 驱动等等），这往往需要消耗大量的时间，并且在有些服务器不能连接互联网的环境中将无法部署，所以我们在 `submarine-installer` 中内置了 HTTP 下载服务，只需要在一台能够连接互联网的服务器中运行 `submarine-installer` ，就可以为所有其他服务器提供依赖包的下载，只需要你按照以下配置进行操作：

  1. 首先，你需要将 `DOWNLOAD_SERVER_IP` 配置为一台能够连接互联网的服务器IP地址，将 `DOWNLOAD_SERVER_PORT` 配置为一个不会不太常用的端口。
  2. 在  `DOWNLOAD_SERVER_IP` 所在的那台服务器中运行 `submarine-installer/install.sh` 命令后，在安装界面中选择 `[start download server]` 菜单项，`submarine-installer` 将会把部署所有的依赖包全部下载到 `submarine-installer/downloads` 目录中，然后通过 `python -m SimpleHTTPServer ${DOWNLOAD_SERVER_PORT}`  命令启动一个 HTTP 下载服务，不要关闭这台服务器中运行着的 `submarine-installer` 。
  3. 在其他服务器中同样运行 `submarine-installer/install.sh` 命令 ，按照安装界面中的 `[install component]`  菜单依次进行各个组件的安装时，会自动从 `DOWNLOAD_SERVER_IP` 所在的那台服务器下载依赖包进行安装部署。
  4. **DOWNLOAD_SERVER** 另外还有一个用处是，你可以自行把各个依赖包手工下载下来，然后放到其中一台服务器的 `submarine-installer/downloads` 目录中，然后开启 `[start download server]` ，这样就可以为整个集群提供离线安装部署的能力。

+ **YARN_CONTAINER_EXECUTOR_PATH 配置项**

  如何编译 YARN 的 container-executor：你进入到 `hadoop/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager` 目录中执行 `mvn package -Pnative -DskipTests`  命令，将会编译出 `./target/native/target/usr/local/bin/container-executor` 文件。

  你需要将 `container-executor` 文件的完整路径填写在 YARN_CONTAINER_EXECUTOR_PATH 配置项中。

+ **YARN_HIERARCHY 配置项**

  请保持和你所使用的 YARN 集群的 `yarn-site.xml` 配置文件中的 `yarn.nodemanager.linux-container-executor.cgroups.hierarchy` 相同的配置，`yarn-site.xml` 中如果未配置该项，那么默认为 `/hadoop-yarn`。

+ **YARN_NODEMANAGER_LOCAL_DIRS 配置项**

  请保持和你所使用的 YARN 集群的 `yarn-site.xml` 配置文件中的 `yarn.nodemanager.local-dirs` 相同的配置。

+ **YARN_NODEMANAGER_LOG_DIRS 配置项**

  请保持和你所使用的 YARN 集群的 `yarn-site.xml` 配置文件中的 `yarn.nodemanager.log-dirs` 相同的配置。

## 使用说明

**submarine-installer**  完全使用 Shell 脚本编写，不需要安装 ansible 等任何部署工具，避免了不同公司用户的服务器管理规范不同而导致程序不通用，例如：有些机房是不容许 ROOT 用户通过 SHELL 直接进行远程服务器操作等。

**submarine-installer**  的部署过程，完全是通过在菜单中进行选择的操作方式进行的，避免了误操作的同时，你还可以通过各个菜单项目对任意一个组件进行分步执行安装、卸载、启动和停止各个组件，具有很好的灵活性，在部分组件出现问题后，也可以通过 **submarine-installer**  对系统进行诊断和修复。

**submarine-installer**  部署过程中屏幕中会显示日志信息，日志信息一共有三种字体颜色：

+ 红色字体颜色：说明组件安装出现了错误，部署已经终止。

+ 绿色文字颜色：说明组件安装正常，部署正常运行。

+ 蓝色文字颜色：需要你按照提示信息在另外一个 SHELL 终端中进行手工输入命令，一般是修改操作系统内核配置操作，按照提示信息依次操作就可以了。

**启动 submarine-installer**

运行 `submarine-installer/install.sh` 命令启动，部署程序首先会检测服务器中的网卡 IP 地址，如果服务器有多个网卡或配置了多个 IP ，会以列表的形式显示，选择你实际使用的 IP 地址。

**submarine-installer**  菜单说明：

![alt text](./images/submarine-installer.gif "Submarine Installer")

## 部署说明

部署流程如下所示：

1. 参照配置说明，根据你的服务器使用情况配置好 install.conf 文件

2. 将整个 `submarine-installer` 文件夹打包复制到所有的服务器节点中

3. 首先在配置为 **DOWNLOAD_SERVER** 的服务器中

   + 运行 `submarine-installer/install.sh` 命令

   + 在安装界面中选择 `[start download server]` 菜单项，等待下载完各个依赖包后，启动 HTTP 服务

4. 在其他需要进行部署的服务器中

   运行 `submarine-installer/install.sh` 命令，显示的主菜单 **[Main menu]** 中有以下菜单：

   + prepare system environment
   + install component
   + uninstall component
   + start component
   + stop component
   + start download server

5. **prepare system environment**

   + **prepare operation system**

     检查部署服务器的操作系统和版本；

   + **prepare operation system kernel**

     显示操作系统内核更新的操作命令的提示信息，根据你的选择是否自动更新内核版本；

   + **prepare GCC version**

     显示操作系统中现在的 GCC 版本内核更新的操作命令的提示信息和根据你的选择是否自动更新 GCC 版本；

   + **check GPU**

     检查服务器是否能够检测到 GPU 显卡；

   + **prepare user&group**

     显示添加 hadoop 和 docker 的用户和用户组操作命令的提示信息，需要你自己根据提示信息检查服务器中是否存在所需要的用户和用户组；

   + **prepare nvidia environment**

     自动进行操作系统内核和头文件的更新，自动安装 `epel-release` 和 `dkms` ；

     显示修改系统内核参数配置的操作命令的提示信息，需要你另外打开一个终端根据命令顺序执行；

6. install component

   + **instll etcd**

     下载 etcd 的 bin 文件，并安装到 `/usr/bin` 目录中；

     根据  **ETCD_HOSTS** 配置项生成 `etcd.service` 文件， 安装到 `/etc/systemd/system/` 目录中；

   + **instll docker**

     下载 docker 的 RPM 包进行本地安装；

     生成 `daemon.json` 配置文件，安装到 `/etc/docker/` 目录中；

     生成 `docker.service` 配置文件，安装到 `/etc/systemd/system/` 目录中；

   + **instll calico network**

     下载 `calico` 、`calicoctl` 和 `calico-ipam` 文件，安装到 `/usr/bin` 目录中；

     生成 `calicoctl.cfg` 配置文件，安装到 `/etc/calico/` 目录中；

     生成 `calico-node.service` 配置文件，安装到 `/etc/systemd/system/` 目录中；

     安装完毕后，会在容器中会根据 **CALICO_NETWORK_NAME** 配置项自动创建 calico network，并自动创建 2 个 Docker 容器，检查 2 个容器是否能偶互相 PING 通；

   + **instll nvidia driver**

     下载 `nvidia-detect` 文件，在服务器中检测显卡版本；

     根据显卡版本号下载 Nvidia 显卡驱动安装包；

     检测本服务器中是否 `disabled Nouveau` ，如果没有停止安装，那么你需要执行 **[prepare system environment]** 菜单中的 **[prepare nvidia environment]** 子菜单项，按照提示进行操作；

     如果本服务器中已经 `disabled Nouveau` ，那么就会进行本地安装；

   + **instll nvidia docker**

     下载 `nvidia-docker` 的 RPM 安装包并进行安装；

     显示检测 `nvidia-docker` 是否可用的命令提示信息，需要你另外打开一个终端根据命令顺序执行；

   + **instll yarn container-executor**

     根据 **YARN_CONTAINER_EXECUTOR_PATH 配置项**，将 `container-executor` 文件复制到 `/etc/yarn/sbin/Linux-amd64-64/` 目录中；

     根据配置生成 `container-executor.cfg` 文件，复制到 `/etc/yarn/sbin/etc/hadoop/` 目录中；

   + **instll submarine autorun script**

     复制 `submarine.sh` 文件到 `/etc/rc.d/init.d/` 目录中；

     将 `/etc/rc.d/init.d/submarine.sh` 添加到 `/etc/rc.d/rc.local` 系统自启动文件中；

7. uninstall component

   删除指定组件的 BIN 文件和配置文件，不在复述

   - uninstll etcd
   - uninstll docker
   - uninstll calico network
   - uninstll nvidia driver
   - uninstll nvidia docker
   - uninstll yarn container-executor
   - uninstll submarine autorun script

8. start component

   重启指定组件，不在复述

   - start etcd
   - start docker
   - start calico network

9. stop component

   停止指定组件，不在复述

   - stop etcd
   - stop docker
   - stop calico network

10. start download server

   只能在 **DOWNLOAD_SERVER_IP 配置项** 所在的服务器中才能执行本操作；


<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

# Creating Docker Images for Running Tensorflow on YARN

## How to create docker images to run Tensorflow on YARN

Dockerfile to run Tensorflow on YARN need two part:

**Base libraries which Tensorflow depends on**

1) OS base image, for example ```ubuntu:16.04```

2) Tensorflow depended libraries and packages. For example ```python```, ```scipy```. For GPU support, need ```cuda```, ```cudnn```, etc.

3) Tensorflow package.

**Libraries to access HDFS**

1) JDK

2) Hadoop

Here's an example of a base image (w/o GPU support) to install Tensorflow:
```
FROM ubuntu:16.04

# Pick up some TF dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
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

RUN pip --no-cache-dir install \
    http://storage.googleapis.com/tensorflow/linux/cpu/tensorflow-1.13.1-cp27-none-linux_x86_64.whl
```

On top of above image, add files, install packages to access HDFS
```
RUN apt-get update && apt-get install -y openjdk-8-jdk wget
# Install hadoop
ENV HADOOP_VERSION="3.1.2"
RUN wget http://mirrors.hust.edu.cn/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
RUN tar zxf hadoop-${HADOOP_VERSION}.tar.gz
RUN ln -s hadoop-${HADOOP_VERSION} hadoop-current
RUN rm hadoop-${HADOOP_VERSION}.tar.gz
```

Build and push to your own docker registry: Use ```docker build ... ``` and ```docker push ...``` to finish this step.

## Use examples to build your own Tensorflow docker images

We provided following examples for you to build tensorflow docker images.

For Tensorflow 1.13.1 (Precompiled to CUDA 10.x)

- *docker/tensorflow/base/ubuntu-16.04/Dockerfile.cpu.tf_1.13.1*: Tensorflow 1.13.1 supports CPU only.
- *docker/tensorflow/with-cifar10-models/ubuntu-16.04/Dockerfile.cpu.tf_1.13.1*: Tensorflow 1.13.1 supports CPU only, and included models
- *docker/tensorflow/base/ubuntu-16.04/Dockerfile.gpu.tf_1.13.1*: Tensorflow 1.13.1 supports GPU, which is prebuilt to CUDA10.
- *docker/tensorflow/with-cifar10-models/ubuntu-16.04/Dockerfile.gpu.tf_1.13.1*: Tensorflow 1.13.1 supports GPU, which is prebuilt to CUDA10, with models.

## Build Docker images

### Manually build Docker image:

Under `docker/` directory, run `build-all.sh` to build Docker images. It will build following images:

- `tf-1.13.1-gpu-base:0.0.1` for base Docker image which includes Hadoop, Tensorflow, GPU base libraries.
- `tf-1.13.1-gpu-base:0.0.1` for base Docker image which includes Hadoop. Tensorflow.
- `tf-1.13.1-gpu:0.0.1` which includes cifar10 model
- `tf-1.13.1-cpu:0.0.1` which inclues cifar10 model (cpu only).

### Use prebuilt images

(No liability)
You can also use prebuilt images for convenience:

- hadoopsubmarine/tf-1.13.1-gpu:0.0.1
- hadoopsubmarine/tf-1.13.1-cpu:0.0.1

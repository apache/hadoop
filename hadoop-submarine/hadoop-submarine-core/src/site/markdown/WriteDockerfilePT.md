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

# Creating Docker Images for Running PyTorch on YARN

## How to create docker images to run PyTorch on YARN

Dockerfile to run PyTorch on YARN needs two parts:

**Base libraries which PyTorch depends on**

1) OS base image, for example ```ubuntu:16.04```

2) PyTorch dependent libraries and packages. For example ```python```, ```scipy```. For GPU support, you also need ```cuda```, ```cudnn```, etc.

3) PyTorch package.

**Libraries to access HDFS**

1) JDK

2) Hadoop

Here's an example of a base image (with GPU support) to install PyTorch:
```
FROM nvidia/cuda:10.0-cudnn7-devel-ubuntu16.04
ARG PYTHON_VERSION=3.6
RUN apt-get update && apt-get install -y --no-install-recommends \
         build-essential \
         cmake \
         git \
         curl \
         vim \
         ca-certificates \
         libjpeg-dev \
         libpng-dev \
         wget &&\
     rm -rf /var/lib/apt/lists/*


RUN curl -o ~/miniconda.sh -O  https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh  && \
     chmod +x ~/miniconda.sh && \
     ~/miniconda.sh -b -p /opt/conda && \
     rm ~/miniconda.sh && \
     /opt/conda/bin/conda install -y python=$PYTHON_VERSION numpy pyyaml scipy ipython mkl mkl-include cython typing && \
     /opt/conda/bin/conda install -y -c pytorch magma-cuda100 && \
     /opt/conda/bin/conda clean -ya
ENV PATH /opt/conda/bin:$PATH
RUN pip install ninja
# This must be done before pip so that requirements.txt is available
WORKDIR /opt/pytorch
RUN git clone https://github.com/pytorch/pytorch.git
WORKDIR pytorch
RUN git submodule update --init
RUN TORCH_CUDA_ARCH_LIST="3.5 5.2 6.0 6.1 7.0+PTX" TORCH_NVCC_FLAGS="-Xfatbin -compress-all" \
    CMAKE_PREFIX_PATH="$(dirname $(which conda))/../" \
    pip install -v .

WORKDIR /opt/pytorch
RUN git clone https://github.com/pytorch/vision.git && cd vision && pip install -v .

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

## Use examples to build your own PyTorch docker images

We provided some example Dockerfiles for you to build your own PyTorch docker images.

For latest PyTorch

- *docker/pytorch/base/ubuntu-16.04/Dockerfile.gpu.pytorch_latest*: Latest Pytorch that supports GPU, which is prebuilt to CUDA10.
- *docker/pytorch/with-cifar10-models/ubuntu-16.04/Dockerfile.gpu.pytorch_latest*: Latest Pytorch that GPU, which is prebuilt to CUDA10, with models.

## Build Docker images

### Manually build Docker image:

Under `docker/pytorch` directory, run `build-all.sh` to build all Docker images. This command will build the following Docker images:

- `pytorch-latest-gpu-base:0.0.1` for base Docker image which includes Hadoop, PyTorch, GPU base libraries.
- `pytorch-latest-gpu:0.0.1` which includes cifar10 model as well

### Use prebuilt images

(No liability)
You can also use prebuilt images for convenience:

- hadoopsubmarine/pytorch-latest-gpu-base:0.0.1

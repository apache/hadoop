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

Submarine is a project which allows infra engineer / data scientist to run
*unmodified* Tensorflow or PyTorch programs on YARN or Kubernetes.

Goals of Submarine:

- It allows jobs for easy access to data/models in HDFS and other storages.

- Can launch services to serve Tensorflow/MXNet models.

- Supports running distributed Tensorflow jobs with simple configs.

- Supports running standalone PyTorch jobs with simple configs.

- Supports running user-specified Docker images.

- Supports specifying GPU and other resources.

- Supports launching Tensorboard for training jobs (optional, if specified).

- Supports customized DNS name for roles (like tensorboard.$user.$domain:6006)


If you want to deep-dive, please check these resources:

- [QuickStart Guide](QuickStart.html)

- [Examples](Examples.html)

- [How to write Dockerfile for Submarine TensorFlow jobs](WriteDockerfileTF.html)

- [How to write Dockerfile for Submarine PyTorch jobs](WriteDockerfilePT.html)

- [Installation guides](HowToInstall.html)

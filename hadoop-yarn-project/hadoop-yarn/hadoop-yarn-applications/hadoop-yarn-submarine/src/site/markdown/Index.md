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

Submarine is a project which allows infra engineer / data scientist to run *unmodified* Tensorflow programs on YARN.

Goals of Submarine:

- It allows jobs for easy access to data/models in HDFS and other storages.

- Can launch services to serve Tensorflow/MXNet models.

- Support run distributed Tensorflow jobs with simple configs.

- Support run user-specified Docker images.

- Support specify GPU and other resources.

- Support launch tensorboard for training jobs if user specified.

- Support customized DNS name for roles (like tensorboard.$user.$domain:6006)


Click below contents if you want to understand more.

- [QuickStart Guide](QuickStart.html)

- [Examples](Examples.html)

- [How to write Dockerfile for Submarine jobs](WriteDockerfile.html)

- [Developer guide](DeveloperGuide.html)

- [Installation guides](HowToInstall.html)

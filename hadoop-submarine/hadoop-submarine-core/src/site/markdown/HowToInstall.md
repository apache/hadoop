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

# How to Install Dependencies

Submarine project uses YARN Service, Docker container and GPU.
GPU could only be used if a GPU hardware is available and properly configured.

As an administrator, you have to properly setup YARN Service related dependencies, including:
- YARN Registry DNS
- Docker related dependencies, including:
  - Docker binary with expected versions
  - Docker network that allows Docker containers to talk to each other across different nodes

If you would like to use GPU, you need to set up:
- GPU Driver
- Nvidia-docker

For your convenience, we provided some installation documents to help you setup your environment. You can always choose to have them installed in your own way.

Use Submarine installer to install dependencies: [EN](https://github.com/hadoopsubmarine/hadoop-submarine-ecosystem/tree/master/submarine-installer) [CN](https://github.com/hadoopsubmarine/hadoop-submarine-ecosystem/blob/master/submarine-installer/README-CN.md)

Alternatively, you can follow this guide to manually install dependencies: [EN](InstallationGuide.html) [CN](InstallationGuideChineseVersion.html)

Once you have installed all the dependencies, please follow this guide: [TestAndTroubleshooting](TestAndTroubleshooting.html).
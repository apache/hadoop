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

# Overview

```$xslt
              _                              _
             | |                            (_)
  ___  _   _ | |__   _ __ ___    __ _  _ __  _  _ __    ___
 / __|| | | || '_ \ | '_ ` _ \  / _` || '__|| || '_ \  / _ \
 \__ \| |_| || |_) || | | | | || (_| || |   | || | | ||  __/
 |___/ \__,_||_.__/ |_| |_| |_| \__,_||_|   |_||_| |_| \___|

                             ?
 ~~~~~~~~~~~~~~~~~~~~~~~~~~~|^"~~~~~~~~~~~~~~~~~~~~~~~~~o~~~~~~~~~~~
        o                   |                  o      __o
         o                  |                 o     |X__>
       ___o                 |                __o
     (X___>--             __|__            |X__>     o
                         |     \                   __o
                         |      \                |X__>
  _______________________|_______\________________
 <                                                \____________   _
  \                                                            \ (_)
   \    O       O       O                                       >=)
    \__________________________________________________________/ (_)
```

Submarine is a project which allows infra engineer / data scientist to run *unmodified* Tensorflow programs on YARN.

Goals of Submarine:
- It allows jobs easy access data/models in HDFS and other storages.
- Can launch services to serve Tensorflow/MXNet models.
- Support run distributed Tensorflow jobs with simple configs.
- Support run user-specified Docker images.
- Support specify GPU and other resources.
- Support launch tensorboard for training jobs if user specified.
- Support customized DNS name for roles (like tensorboard.$user.$domain:6006)

Please jump to [QuickStart](src/site/markdown/QuickStart.md) guide to quickly understand how to use this framework.

Please jump to [Examples](src/site/markdown/Examples.md) to try other examples like running Distributed Tensorflow Training for CIFAR 10.

If you're a developer, please find [Developer](src/site/markdown/DeveloperGuide.md) guide for more details.

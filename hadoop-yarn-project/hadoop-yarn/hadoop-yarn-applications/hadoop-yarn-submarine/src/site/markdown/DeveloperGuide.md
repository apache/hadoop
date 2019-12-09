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

# Developer Guide

By default, submarine uses YARN service framework as runtime. If you want to add your own implementation. You can add a new `RuntimeFactory` implementation and configure following option to `submarine.xml` (which should be placed under same `$HADOOP_CONF_DIR`)

```
<property>
  <name>submarine.runtime.class</name>
  <value>... full qualified class name for your runtime factory ... </value>
</property>
```

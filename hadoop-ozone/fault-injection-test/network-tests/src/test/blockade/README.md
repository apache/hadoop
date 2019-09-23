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

## Blockade Tests
Following python packages need to be installed before running the tests :

1. blockade
2. pytest==3.2.0

Running test as part of the maven build:

```
mvn clean verify -Pit
```

Running test as part of the released binary:

You can execute all blockade tests with following command:

```
cd $OZONE_HOME
python -m pytest tests/blockade
```

You can also execute specific blockade tests with following command:

```
cd $OZONE_HOME
python -m pytest tests/blockade/< PATH TO PYTHON FILE >
e.g: python -m pytest tests/blockade/test_blockade_datanode_isolation.py
```
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
2. pytest==2.8.7

You can execute all blockade tests with following command-lines:

```
cd $DIRECTORY_OF_OZONE
python -m pytest -s  blockade/
```

You can also execute fewer blockade tests with following command-lines:

```
cd $DIRECTORY_OF_OZONE
python -m pytest -s  blockade/<PATH_TO_PYTHON_FILE>
e.g: python -m pytest -s blockade/test_blockade_datanode_isolation.py
```

You can change the default 'sleep' interval in the tests with following
command-lines:

```
cd $DIRECTORY_OF_OZONE
python -m pytest -s  blockade/ --containerStatusSleep=<SECONDS>

e.g: python -m pytest -s  blockade/ --containerStatusSleep=720
```

By default, second phase of the tests will not be run.
In order to run the second phase of the tests, you can run following
command-lines:

```
cd $DIRECTORY_OF_OZONE
python -m pytest -s  blockade/ --runSecondPhase=true

```
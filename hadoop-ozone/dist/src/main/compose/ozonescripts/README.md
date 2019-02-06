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

# start-ozone environment

This is an example environment to use/test `./sbin/start-ozone.sh` and `./sbin/stop-ozone.sh` scripts.

There are ssh connections between the containers and the start/stop scripts could handle the start/stop process
similar to a real cluster.

To use it, first start the cluster:

```
docker-copmose up -d
```

After a successfull startup (which starts only the ssh daemons) you can start ozone:

```
./start.sh
```

Check it the java processes are started:

```
./ps.sh
```
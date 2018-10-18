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

# Running Zeppelin Notebook On Submarine

This is a simple example about how to run Zeppelin notebook by using Submarine.

## Step 1: Build Docker Image

Go to `src/main/docker/zeppelin-notebook-example`, build the Docker image. Or you can use the prebuilt one: `hadoopsubmarine/zeppelin-on-yarn-gpu:0.0.1`

## Step 2: Launch the notebook on YARN

Submit command to YARN:

`yarn app -destroy zeppelin-notebook;
yarn jar path-to/hadoop-yarn-applications-submarine-3.2.0-SNAPSHOT.jar \
   job run --name zeppelin-notebook \
   --docker_image hadoopsubmarine/zeppelin-on-yarn-gpu:0.0.1 \
   --worker_resources memory=8G,vcores=2,gpu=1 \
   --num_workers 1 \
   -worker_launch_cmd "/usr/local/bin/run_container.sh"`

Once the container got launched, you can go to `YARN services` UI page, access the `zeppelin-notebook` job, and go to the quicklink `notebook` by clicking `...`.

The notebook is secured by admin/admin user name and password.
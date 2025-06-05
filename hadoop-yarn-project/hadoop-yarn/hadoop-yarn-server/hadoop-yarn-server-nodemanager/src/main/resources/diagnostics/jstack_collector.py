# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess
import sys

NUMBER_OF_JSTACK = 3

def get_nodemanager_pid():
    results = run_command("ps aux | grep nodemanager | grep -v grep")
    # ps aux | grep nodemanager | grep -v grep
    # root       414  1.3  1.7 8124480 434520 ?      Sl   11:36   0:52 /usr/lib/jvm/java-8-openjdk//bin/java -Dproc_nodemanager -Djava.net.preferIPv4Stack=true -Dyarn.log.dir=/opt/hadoop/logs -Dyarn.log.file=hadoop.log -Dyarn.home.dir=/opt/hadoop -Dyarn.root.logger=INFO,console -Dhadoop.log.dir=/opt/hadoop/logs -Dhadoop.log.file=hadoop.log -Dhadoop.home.dir=/opt/hadoop -Dhadoop.id.str=root -Dhadoop.root.logger=INFO,console -Dhadoop.policy.file=hadoop-policy.xml -Dhadoop.security.logger=INFO,NullAppender -XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.zip=ALL-UNNAMED --add-opens=java.base/sun.security.util=ALL-UNNAMED --add-opens=java.base/sun.security.x509=ALL-UNNAMED org.apache.hadoop.yarn.server.nodemanager.NodeManager
    pids = []  # Some host may contain more than one NodeManager
    for result in results.strip().splitlines():
        pid = result.split()[1]
        pids.append(pid)

    return pids


def get_app_pid(app_id):

    # results= '''
    #     root       413  1.7  2.0 8355580 512972 ?      Sl   11:21   2:56 /usr/lib/jvm/java-8-openjdk//bin/java -Dproc_nodemanager -Djava.net.preferIPv4Stack=true -Dhadoop.log.dir=/opt/hadoop/logs -Dhadoop.log.file=NODEMANAGER.log -Dyarn.log.dir=/opt/hadoop/logs -Dyarn.log.file=NODEMANAGER.log -Dyarn.home.dir=/opt/hadoop -Dyarn.root.logger=INFO,DRFA -Dhadoop.home.dir=/opt/hadoop -Dhadoop.id.str=root -Dhadoop.root.logger=INFO,DRFA -Dhadoop.policy.file=hadoop-policy.xml -Dhadoop.security.logger=INFO,NullAppender -XX:+IgnoreUnrecognizedVMOptions --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.math=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.text=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.zip=ALL-UNNAMED --add-opens=java.base/sun.security.util=ALL-UNNAMED --add-opens=java.base/sun.security.x509=ALL-UNNAMED --enable-native-access=ALL-UNNAMED org.apache.hadoop.yarn.server.nodemanager.NodeManager
    #     root     41611  4.1  1.9 2414568 470660 ?      Sl   14:08   0:16 /usr/lib/jvm/java-8-openjdk//bin/java -Xmx750m org.apache.hadoop.yarn.applications.distributedshell.ApplicationMaster --container_type GUARANTEED --container_memory 750 --container_vcores 1 --num_containers 500 --priority 0 --appname DistributedShell --homedir hdfs://namenode:9000/user/root
    # '''
    results = run_command("ps aux | grep jvm/java | grep -v -e /bin/bash -e grep")  # TODO: later include "grep app_id" for long java application like mapreduce
    pids = []
    for result in results.strip().splitlines():
        pid = result.split()[1]
        pids.append(pid)

    return pids


def execute_jstack(pids):
    all_jstacks = []

    for pid in pids:
        for i in range(NUMBER_OF_JSTACK):  # Get multiple jstack
            jstack_output = run_command("jstack", pid)
            all_jstacks.append("--- JStack iteration-{} for PID: {} ---\n{}".format(i, pid, jstack_output))

    return "\n".join(all_jstacks)


def run_command(*argv):
    try:
        cmd = " ".join(arg for arg in argv)
        print("Running command with arguments:", cmd)
        response = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, check=True)
        response_str = response.stdout.decode('utf-8')
    except subprocess.CalledProcessError as e:
        response_str = "Unable to run command: {}".format(e)
        print(response_str, file=sys.stderr)
    except Exception as e:
        response_str = "Exception occurred: {}".format(e)
        print(response_str, file=sys.stderr)

    return response_str


def main():

    # app_id = "application_1748517687882_0013"
    if len(sys.argv) > 1:
        app_id = sys.argv[1]
        pids = get_app_pid(app_id)
    else:
        pids = get_nodemanager_pid()

    if not pids:
        print("No active process id in this NodeManager.")
        sys.exit(0)

    jstacks = execute_jstack(pids)
    print(jstacks)  # The Initiated java processBuilder will read this stdout


if __name__ == "__main__":
    main()


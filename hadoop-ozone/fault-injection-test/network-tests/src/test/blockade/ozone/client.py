#!/usr/bin/python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from ozone import util
from ozone.cluster import Command


class OzoneClient:

    __logger__ = logging.getLogger(__name__)

    def __init__(self, cluster):
        self.cluster = cluster
        pass

    def create_volume(self, volume_name):
        OzoneClient.__logger__.info("Creating Volume %s" % volume_name)
        command = [Command.ozone, "sh volume create /%s --user root" % volume_name]
        util.run_docker_command(command, self.cluster.client)

    def create_bucket(self, volume_name, bucket_name):
        OzoneClient.__logger__.info("Creating Bucket %s in Volume %s" % (bucket_name, volume_name))
        command = [Command.ozone, "sh bucket create /%s/%s" % (volume_name, bucket_name)]
        util.run_docker_command(command, self.cluster.client)

    def put_key(self, source_file, volume_name, bucket_name, key_name, replication_factor=None):
        OzoneClient.__logger__.info("Creating Key %s in %s/%s" % (key_name, volume_name, bucket_name))
        exit_code, output = util.run_docker_command(
            "ls %s" % source_file, self.cluster.client)
        assert exit_code == 0, "%s does not exist" % source_file
        command = [Command.ozone, "sh key put /%s/%s/%s %s" %
                   (volume_name, bucket_name, key_name, source_file)]
        if replication_factor:
            command.append("--replication=%s" % replication_factor)

        exit_code, output = util.run_docker_command(command, self.cluster.client)
        assert exit_code == 0, "Ozone put Key failed with output=[%s]" % output

    def get_key(self, volume_name, bucket_name, key_name, file_path='.'):
        OzoneClient.__logger__.info("Reading key %s from %s/%s" % (key_name, volume_name, bucket_name))
        command = [Command.ozone, "sh key get /%s/%s/%s %s" %
                   (volume_name, bucket_name, key_name, file_path)]
        exit_code, output = util.run_docker_command(command, self.cluster.client)
        assert exit_code == 0, "Ozone get Key failed with output=[%s]" % output

    def run_freon(self, num_volumes, num_buckets, num_keys, key_size,
                  replication_type="RATIS", replication_factor="THREE"):
        """
        Runs freon on the cluster.
        """
        command = [Command.freon,
                   " rk",
                   " --numOfVolumes " + str(num_volumes),
                   " --numOfBuckets " + str(num_buckets),
                   " --numOfKeys " + str(num_keys),
                   " --keySize " + str(key_size),
                   " --replicationType " + replication_type,
                   " --factor " + replication_factor]
        return util.run_docker_command(command, self.cluster.client)

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
import time
import re
import subprocess

from ozone.constants import Command

logger = logging.getLogger(__name__)


def wait_until(predicate, timeout, check_frequency=1):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(check_frequency)


def run_docker_command(command, run_on):
    if isinstance(command, list):
        command = ' '.join(command)
    command = [Command.docker,
               "exec " + run_on,
               command]
    return run_command(command)


def run_command(cmd):
    command = cmd
    if isinstance(cmd, list):
        command = ' '.join(cmd)
    logger.info("RUNNING: %s", command)
    all_output = ""
    my_process = subprocess.Popen(command,  stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, shell=True)
    while my_process.poll() is None:
        op = my_process.stdout.readline()
        if op:
            all_output += op
            logger.info(op)
    other_output = my_process.communicate()
    other_output = other_output[0].strip()
    if other_output != "":
        all_output += other_output
    reg = re.compile(r"(\r\n|\n)$")
    logger.debug("Output: %s", all_output)
    all_output = reg.sub("", all_output, 1)
    return my_process.returncode, all_output


def get_checksum(file_path, run_on):
    command = "md5sum  %s" % file_path
    exit_code, output = run_docker_command(command, run_on)
    assert exit_code == 0, "Cant find checksum"
    output_split = output.split("\n")
    result = ""
    for line in output_split:
        if line.find("Warning") >= 0 or line.find("is not a tty") >= 0:
            logger.info("skip this line: %s", line)
        else:
            result = result + line
    checksum = result.split(" ")
    return checksum[0]

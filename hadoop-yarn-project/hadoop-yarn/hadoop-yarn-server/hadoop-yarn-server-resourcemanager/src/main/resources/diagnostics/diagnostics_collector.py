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
from __future__ import print_function

import argparse
import sys, os
import socket
import subprocess
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
import re
import time

TEMP_DIR = "/tmp"
HADOOP_CONF_DIR = None
YARN_SITE_XML = "yarn-site.xml"
RM_WEBAPP_HTTPS_ADDRESS_KEY = "yarn.resourcemanager.webapp.https.address"
RM_WEBAPP_HTTP_ADDRESS_KEY = "yarn.resourcemanager.webapp.address"
RM_ADDRESS = None
RM_SCHEME = "http"

RM_LOG_REGEX = r"(?<=\")\/logs.+?RESOURCEMANAGER.+?(?=\")"
NM_LOG_REGEX = r"(?<=\")\/logs.+?NODEMANAGER.+?(?=\")"
INPUT_TIME_FORMAT = '%a %b %d %H:%M:%S %Z %Y'  # e.g. Wed May 28 07:35:39 UTC 2025
OUTPUT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S,%f'    # e.g. 2025-05-28 11:57:05,435
OUTPUT_TIME_FORMAT_WITHOUT_SECOND = '%Y-%m-%d %H:%M'  # e.g. 2025-05-28 11:57
NUMBER_OF_JSTACK = 3


def application_diagnostic():
    """
        Application Logs, Application Info, Application Attempts
        Multiple JStack of Hanging Containers and NodeManager
        ResourceManager logs during job duration.
        NodeManager logs from NodeManager where hanging containers of jobs run during the duration of containers.
    """

    if args.arguments is None or len(args.arguments) == 0:
        print("Missing application or job id, exiting...")
        sys.exit(os.EX_USAGE)

    app_id = args.arguments[0]

    output_path = create_output_dir(os.path.join(TEMP_DIR, app_id))

    # Get JStack of the hanging containers
    nm_address = get_nodemanager_address(app_id)
    app_jstack = create_request(web_url(RM_SCHEME, nm_address,
                                        "ws/v1/node/apps/{}/jstack".format(app_id)), False)
    write_output(output_path, "application_jstack", app_jstack)

    # Get JStack of the hanging NodeManager
    nm_jstack = create_request(web_url(RM_SCHEME, nm_address, "ws/v1/node/jstack"), False)
    write_output(output_path, "nm_{}_jstack".format(nm_address), nm_jstack)

    # Get application info
    app_info= create_request(rm_url("ws/v1/cluster/apps/{}".format(app_id)))
    write_output(output_path, "application_info", app_info)

    # Get application attempts
    app_attempts = create_request(rm_url("ws/v1/cluster/apps/{}/appattempts".format(app_id)))
    write_output(output_path, "application_attempts", app_attempts)

    # Get start_time and end_time of the application
    start_time, end_time = get_application_time(app_info)

    # Get RM log
    log_address = get_node_log_address(RM_ADDRESS, RM_LOG_REGEX, RM_SCHEME)
    write_output(os.path.join(output_path, "node_log"), "resourcemanager_log",
                 filter_node_log(log_address, start_time, end_time, RM_SCHEME))

    # Get NodeManager logs in the duration of containers belonging to app_id
    if "amHostHttpAddress" in app_info:
        app_info = ET.fromstring(app_info)
        nm_address = app_info.find("amHostHttpAddress").text
        log_address = get_node_log_address(nm_address, NM_LOG_REGEX, RM_SCHEME)
        write_output(os.path.join(output_path, "node_log"), "nodemanager_log",
                     get_container_log(log_address, app_id, RM_SCHEME))

    # Get application log
    command = run_cmd_and_save_output(os.path.join(output_path, "app_logs"), app_id, "yarn", "logs", "-applicationId",
                                      app_id)  # TODO user permission?

    command.communicate()
    return output_path


def scheduler_related_issue():
    """
        ResourceManager Scheduler Logs with DEBUG enabled for 2 minutes.
        Multiple Jstack of ResourceManager
        YARN and Scheduler Configuration
        Cluster Scheduler API /ws/v1/cluster/scheduler and Cluster Nodes API /ws/v1/cluster/nodes response
        Scheduler Activities /ws/v1/cluster/scheduler/bulk-activities response
    """
    output_path = create_output_dir(os.path.join(TEMP_DIR, "scheduler_related_issue" + str(time.time()).split(".")[0]))

    # Multiple JStack of ResourceManager
    rm_pids = get_resourcemanager_pid()
    jstacks_output = get_multiple_jstack(rm_pids)
    write_output(output_path, "jstacks_resourcemanager", jstacks_output)

    # Get Cluster Scheduler Info
    scheduler_info = create_request(rm_url("ws/v1/cluster/scheduler"))
    write_output(output_path, "scheduler_info", scheduler_info)

    # Get Cluster Nodes Info
    nodes_info = create_request(rm_url("ws/v1/cluster/nodes"))
    write_output(output_path, "nodemanager_info", nodes_info)

    # Get Scheduler Activities
    scheduler_activities = create_request(rm_url("ws/v1/cluster/scheduler/bulk-activities"))
    write_output(output_path, "scheduler_activities", scheduler_activities)

    # Get Scheduler Configuration
    scheduler_config = create_request(rm_url("ws/v1/cluster/scheduler-conf"))
    write_output(output_path, "scheduler_configuration", scheduler_config)

    # Get YARN configuration yarn-site.xml
    yarn_conf = run_command("cat", os.path.join(HADOOP_CONF_DIR, YARN_SITE_XML))
    write_output(output_path, "yarn_site", yarn_conf)

    # Get RM Debug log for the last 2 minutes
    enable_debug_log = set_rm_scheduler_log_level("DEBUG")
    print(enable_debug_log)
    log_address = get_node_log_address(RM_ADDRESS, RM_LOG_REGEX, RM_SCHEME)
    start_time, end_time = (format_datetime_no_seconds(datetime.now() - timedelta(seconds=120)),
                            format_datetime_no_seconds(datetime.now()))
    rm_debug_log = filter_node_log(log_address, start_time, end_time, RM_SCHEME)
    write_output(output_path, "rm_debug_log_2min", rm_debug_log)
    enable_info_log = set_rm_scheduler_log_level("INFO")
    print(enable_info_log)

    return output_path

####################################################### Utils Functions ###############################################


def list_issues():
    print("application_diagnostic:appId", "scheduler_related_issue", sep="\n")


def resolve_hadoop_conf_dir():
    for conf_dir in ("/etc/hadoop", "/etc/hadoop/conf"):
        if os.path.isfile(os.path.join(conf_dir, YARN_SITE_XML)):
            return conf_dir

    print("yarn-site.xml not found under /etc/hadoop or /etc/hadoop/conf")
    sys.exit(1)


def web_url(scheme, address, path=""):
    if path:
        return "{}://{}/{}".format(scheme, address, path.lstrip("/"))
    return "{}://{}".format(scheme, address)


def rm_url(path):
    return web_url(RM_SCHEME, RM_ADDRESS, path)


def parse_property_from_conf(conf_file, property_prefix):
    root = ET.parse(os.path.join(HADOOP_CONF_DIR, conf_file))
    matches = []
    for prop in root.findall("property"):
        prop_name = prop.find("name").text
        if prop_name.startswith(property_prefix + "."):  # handle HA environment e.g. yarn.resourcemanager.webapp.address.rm1
            value_elem = prop.find("value")
            if value_elem is not None and value_elem.text:
                matches.append((prop_name, value_elem.text.strip()))
    return matches


def pick_property_value(matches, property_prefix):
    if not matches:
        return None
    if len(matches) == 1:
        return matches[0][1]

    current_host = socket.getfqdn().lower()
    for prop_name, value in matches:
        host_part = value.split(":")[0].lower()
        if host_part == current_host:  # Get the address of the current host
            print("Using {} ({})".format(prop_name, value))
            return value


def resolve_rm_webapp_address():
    global RM_SCHEME
    for property_prefix, scheme in (
        (RM_WEBAPP_HTTPS_ADDRESS_KEY, "https"),
        (RM_WEBAPP_HTTP_ADDRESS_KEY, "http"),
    ):
        matches = parse_property_from_conf(YARN_SITE_XML, property_prefix)
        address = pick_property_value(matches, property_prefix)
        if address:
            RM_SCHEME = scheme
            return address

    print("RM webapp address not found in {}".format(YARN_SITE_XML))
    sys.exit(1)


def create_output_dir(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return dir_path


def write_output(output_path, out_filename, value):
    output_path = create_output_dir(output_path)
    with open(os.path.join(output_path, out_filename), 'w') as f:
        f.write(value)


def run_command(*argv):
    try:
        cmd = " ".join(arg for arg in argv)
        print("Running command with arguments:", cmd)
        response = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, check=True)
        response_str = response.stdout.decode('utf-8')
    except subprocess.CalledProcessError as e:
        response_str = "Command failed with error: {}".format(e)
        print("Unable to run command: ", response_str)
    except Exception as e:
        response_str = "Exception occurred: {}".format(e)
        print("Exception occurred: ", response_str)

    return response_str


def run_cmd_and_save_output(output_path, out_filename, *argv):
    file_path = os.path.join(create_output_dir(output_path), out_filename)
    with open(file_path, 'w') as f:
        return subprocess.Popen(argv, stdout=f)


def build_curl_args(url, xml_type=True):
    curl_args = ["curl", "-sS", "--negotiate", "-u", ":"]
    if url.startswith("https://"):
        curl_args.append("-k")
    if xml_type:
        curl_args.extend(["-H", "Accept: application/xml"])
    curl_args.append(url)
    return curl_args


def create_request(url, xml_type=True):
    curl_args = build_curl_args(url, xml_type)
    try:
        response = subprocess.run(
            curl_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return response.stdout.decode('utf-8')
    except subprocess.CalledProcessError as e:
        response_str = "curl failed: {}".format(e.stderr.decode('utf-8'))
        print("Request failed: ", response_str)
        return response_str


def get_nodemanager_address(app_id):
    app_info = create_request(rm_url("ws/v1/cluster/apps/{}".format(app_id)))
    app_info_xml = ET.fromstring(app_info)
    return app_info_xml.find("amHostHttpAddress").text


def get_node_log_address(node_address, link_regex, scheme="http"):
    try:
        log_page = create_request(web_url(scheme, node_address, "logs/"), False)
        matches = re.findall(link_regex, log_page, re.MULTILINE)
        if not matches:
            return "Warning: No matching log links found at {}://{}/logs/".format(scheme, node_address)
        return node_address + matches[0]
    except Exception as e:
        return "Failed to retrieve node logs address from {}: {}".format(node_address, e)


def filter_node_log(node_log_address, start_time, end_time, scheme="http"):
    url = web_url(scheme, node_log_address)
    return run_command(*build_curl_args(url, xml_type=False), "|", "sed", "-n",
                     "'/{}/,/{}/p'".format(start_time, end_time))


def get_container_log(log_address, app_id, scheme="http"):
    url = web_url(scheme, log_address)
    grep_pattern = re.sub(r"^(job|application)", "container(_e[0-9]+)?", app_id)
    return run_command(*build_curl_args(url, xml_type=False), "|", "grep", "-E",
                       '"{}"'.format(grep_pattern))


def get_application_time(app_info_string):
    app_element = ET.fromstring(app_info_string)
    start_time_epoch = int(app_element.find("startedTime").text)
    finish_time_epoch = int(app_element.find("finishedTime").text)

    start_time_str = datetime.fromtimestamp(start_time_epoch / 1000).strftime(OUTPUT_TIME_FORMAT)[:-4]  # -4, the time conversion is not accurrate
    finish_time_str = datetime.fromtimestamp(finish_time_epoch / 1000).strftime(OUTPUT_TIME_FORMAT)[:-4]

    return start_time_str, finish_time_str


def get_resourcemanager_pid():
    results = run_command("ps", "aux", "|", "grep", "resourcemanager", "|", "grep", "-v", "grep")

    pids = []
    for result in results.strip().splitlines():
        pid = result.split()[1]
        pids.append(pid)

    return pids


def get_multiple_jstack(pids):
    all_jstacks = []

    for pid in pids:
        for i in range(NUMBER_OF_JSTACK):  # Get multiple jstack
            jstack_output = run_command("jstack", pid)
            all_jstacks.append("--- JStack iteration-{} for PID: {} ---\n{}".format(i, pid, jstack_output))

    return "\n".join(all_jstacks)


def set_rm_scheduler_log_level(log_level):
    cmd = ["yarn", "daemonlog", "-setlevel", RM_ADDRESS,
           "org.apache.hadoop.yarn.server.resourcemanager.scheduler", log_level]
    if RM_SCHEME == "https":
        cmd.extend(["-protocol", "https"])
    return run_command(*cmd)


def format_datetime_no_seconds(datetime_obj):
    return datetime_obj.strftime(OUTPUT_TIME_FORMAT_WITHOUT_SECOND)


def main():

    ISSUE_MAP = {
        "application_diagnostic": application_diagnostic,
        "scheduler_related_issue": scheduler_related_issue,
    }

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--list", help="List the available issue types.", action="store_true")
    parser.add_argument("-c", "--command", choices=list(ISSUE_MAP), help="Initiate the diagnostic information collecton"
                                                                         "for diagnosing the selected issue type.")
    parser.add_argument("-a", "--arguments", nargs='*', help="The required arguments for the selected issue type.")
    global args
    args = parser.parse_args()

    if not (args.list or args.command):
        parser.error('No action requested, use --list or --command')

    if args.list:
        list_issues()
        sys.exit(os.EX_OK)

    global HADOOP_CONF_DIR, RM_ADDRESS, RM_SCHEME
    HADOOP_CONF_DIR = resolve_hadoop_conf_dir()
    RM_ADDRESS = resolve_rm_webapp_address()
    print("Using RM webapp at {}://{}".format(RM_SCHEME, RM_ADDRESS))

    selected_option = ISSUE_MAP[args.command]
    print(selected_option())  # print the resulted output path that will be used by the DiagnosticsService.java


if __name__ == "__main__":
    main()
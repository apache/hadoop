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

import argparse
import logging
import os
import re
import socket
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)

_TEMP_DIR = "/tmp"
_HADOOP_CONF_DIR: Optional[str] = None
_YARN_SITE_XML = "yarn-site.xml"
_RM_WEBAPP_HTTPS_ADDRESS_KEY = "yarn.resourcemanager.webapp.https.address"
_RM_WEBAPP_HTTP_ADDRESS_KEY = "yarn.resourcemanager.webapp.address"
_RM_ADDRESS: Optional[str] = None
_NODE_SCHEME = "http"

_RM_LOG_REGEX = r"(?<=\")\/logs.+?RESOURCEMANAGER.+?(?=\")"
_NM_LOG_REGEX = r"(?<=\")\/logs.+?NODEMANAGER.+?(?=\")"
_INPUT_TIME_FORMAT = '%a %b %d %H:%M:%S %Z %Y'  # e.g. Wed May 28 07:35:39 UTC 2025
_OUTPUT_TIME_FORMAT = '%Y-%m-%d %H:%M:%S,%f'    # e.g. 2025-05-28 11:57:05,435
_OUTPUT_TIME_FORMAT_WITHOUT_SECOND = '%Y-%m-%d %H:%M'  # e.g. 2025-05-28 11:57
_NUMBER_OF_JSTACK = 3

args = None


def list_issues() -> None:
    print("application_diagnostic:appId", "scheduler_related_issue", sep="\n")


def application_diagnostic() -> str:
    """
    Application Logs, Application Info, Application Attempts
    Multiple JStack of Hanging Containers and NodeManager
    ResourceManager logs during job duration.
    NodeManager logs from NodeManager where hanging containers of jobs run during the duration of containers.
    """
    if args.arguments is None or len(args.arguments) == 0:
        logger.error("Missing application or job id")
        sys.exit(os.EX_USAGE)

    app_id = args.arguments[0]
    logger.info("Collecting application diagnostics for %s", app_id)
    output_path = _create_output_dir(os.path.join(_TEMP_DIR, app_id))

    nm_address = _get_nodemanager_address(app_id)
    app_jstack = _create_request(
        _web_url(_NODE_SCHEME, nm_address, "ws/v1/node/apps/{}/jstack/{}".format(app_id, _NUMBER_OF_JSTACK)),
        False)
    _write_output(output_path, "application_jstack", app_jstack)

    nm_jstack = _create_request(
        _web_url(_NODE_SCHEME, nm_address, "ws/v1/node/jstack/{}".format(_NUMBER_OF_JSTACK)),
        False)
    _write_output(output_path, "nm_{}_jstack".format(nm_address), nm_jstack)

    app_info = _create_request(_rm_url("ws/v1/cluster/apps/{}".format(app_id)))
    _write_output(output_path, "application_info", app_info)

    app_attempts = _create_request(_rm_url("ws/v1/cluster/apps/{}/appattempts".format(app_id)))
    _write_output(output_path, "application_attempts", app_attempts)

    start_time, end_time = _get_application_time(app_info)

    log_address = _get_node_log_address(_RM_ADDRESS, _RM_LOG_REGEX, _NODE_SCHEME)
    _write_output(os.path.join(output_path, "node_log"), "resourcemanager_log",
                  _filter_node_log(log_address, start_time, end_time, _NODE_SCHEME))

    if "amHostHttpAddress" in app_info:
        app_info_xml = ET.fromstring(app_info)
        nm_address = app_info_xml.find("amHostHttpAddress").text
        log_address = _get_node_log_address(nm_address, _NM_LOG_REGEX, _NODE_SCHEME)
        _write_output(os.path.join(output_path, "node_log"), "nodemanager_log",
                      _get_container_log(log_address, app_id, _NODE_SCHEME))

    command = _run_cmd_and_save_output(os.path.join(output_path, "app_logs"), app_id,
                                       "yarn", "logs", "-applicationId", app_id)
    command.communicate()
    logger.info("Application diagnostics written to %s", output_path)
    return output_path


def scheduler_related_issue() -> str:
    """
    ResourceManager Scheduler Logs with DEBUG enabled for 2 minutes.
    Multiple Jstack of ResourceManager
    YARN and Scheduler Configuration
    Cluster Scheduler API /ws/v1/cluster/scheduler and Cluster Nodes API /ws/v1/cluster/nodes response
    Scheduler Activities /ws/v1/cluster/scheduler/bulk-activities response
    """
    logger.info("Collecting scheduler-related diagnostics")
    output_path = _create_output_dir(
        os.path.join(_TEMP_DIR, "scheduler_related_issue" + str(time.time()).split(".")[0]))

    rm_jstack = _create_request(_rm_url("ws/v1/node/jstack/{}".format(_NUMBER_OF_JSTACK)), False)
    _write_output(output_path, "rm_{}_jstack".format(_RM_ADDRESS), rm_jstack)

    scheduler_info = _create_request(_rm_url("ws/v1/cluster/scheduler"))
    _write_output(output_path, "scheduler_info", scheduler_info)

    nodes_info = _create_request(_rm_url("ws/v1/cluster/nodes"))
    _write_output(output_path, "nodemanager_info", nodes_info)

    scheduler_activities = _create_request(_rm_url("ws/v1/cluster/scheduler/bulk-activities"))
    _write_output(output_path, "scheduler_activities", scheduler_activities)

    scheduler_config = _create_request(_rm_url("ws/v1/cluster/scheduler-conf"))
    _write_output(output_path, "scheduler_configuration", scheduler_config)

    yarn_conf = _run_command("cat", os.path.join(_HADOOP_CONF_DIR, _YARN_SITE_XML))
    _write_output(output_path, "yarn_site", yarn_conf)

    enable_debug_log = _set_rm_scheduler_log_level("DEBUG")
    logger.info("Set RM scheduler log level to DEBUG: %s", enable_debug_log)
    log_address = _get_node_log_address(_RM_ADDRESS, _RM_LOG_REGEX, _NODE_SCHEME)
    start_time, end_time = (_format_datetime_no_seconds(datetime.now() - timedelta(seconds=120)),
                            _format_datetime_no_seconds(datetime.now()))
    rm_debug_log = _filter_node_log(log_address, start_time, end_time, _NODE_SCHEME)
    _write_output(output_path, "rm_debug_log_2min", rm_debug_log)
    enable_info_log = _set_rm_scheduler_log_level("INFO")
    logger.info("Restored RM scheduler log level to INFO: %s", enable_info_log)

    logger.info("Scheduler diagnostics written to %s", output_path)
    return output_path


def _web_url(scheme: str, address: str, path: str = "") -> str:
    if path:
        return "{}://{}/{}".format(scheme, address, path.lstrip("/"))
    return "{}://{}".format(scheme, address)


def _rm_url(path: str) -> str:
    return _web_url(_NODE_SCHEME, _RM_ADDRESS, path)


def _parse_property_from_conf(conf_file: str, property_prefix: str) -> List[Tuple[str, str]]:
    root = ET.parse(os.path.join(_HADOOP_CONF_DIR, conf_file))
    matches = []
    for prop in root.findall("property"):
        prop_name = prop.find("name").text
        if prop_name == property_prefix or prop_name.startswith(property_prefix + "."):  # Handle both HA and non-HA cases
            value_elem = prop.find("value")
            if value_elem is not None and value_elem.text:
                matches.append((prop_name, value_elem.text.strip()))
    return matches


def _get_current_rm_address(matches: List[Tuple[str, str]]) -> Optional[str]:
    if not matches:
        return None
    if len(matches) == 1:
        return matches[0][1]

    current_host = socket.getfqdn().lower()
    for prop_name, value in matches:
        host_part = value.split(":")[0].lower()
        if host_part == current_host:
            logger.info("Using %s (%s)", prop_name, value)
            return value

    prop_name, value = matches[0]
    logger.warning("Multiple RM webapp addresses found; using %s (%s)", prop_name, value)
    return value


def _resolve_rm_webapp_address() -> str:
    global _NODE_SCHEME
    for property_prefix, scheme in (
        (_RM_WEBAPP_HTTPS_ADDRESS_KEY, "https"),
        (_RM_WEBAPP_HTTP_ADDRESS_KEY, "http"),
    ):
        matches = _parse_property_from_conf(_YARN_SITE_XML, property_prefix)
        address = _get_current_rm_address(matches)
        if address:
            _NODE_SCHEME = scheme
            return address

    logger.error("RM webapp address not found in %s", _YARN_SITE_XML)
    sys.exit(1)


def _resolve_hadoop_conf_dir() -> str:
    for conf_dir in ("/etc/hadoop", "/etc/hadoop/conf"):
        if os.path.isfile(os.path.join(conf_dir, _YARN_SITE_XML)):
            return conf_dir

    logger.error("yarn-site.xml not found under /etc/hadoop or /etc/hadoop/conf")
    sys.exit(1)


def _create_output_dir(dir_path: str) -> str:
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return dir_path


def _write_output(output_path: str, out_filename: str, value: str) -> None:
    output_path = _create_output_dir(output_path)
    with open(os.path.join(output_path, out_filename), 'w') as f:
        f.write(value)


def _run_command(*argv: str) -> str:
    try:
        cmd = " ".join(arg for arg in argv)
        logger.debug("Running command: %s", cmd)
        response = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  shell=True, check=True)
        return response.stdout.decode('utf-8')
    except subprocess.CalledProcessError as e:
        logger.warning("Unable to run command with error: %s", e)
    except Exception as e:
        logger.warning("Exception occurred while running command: %s", e)
    return ""

def _run_cmd_and_save_output(output_path: str, out_filename: str, *argv: str) -> subprocess.Popen:
    file_path = os.path.join(_create_output_dir(output_path), out_filename)
    with open(file_path, 'w') as f:
        return subprocess.Popen(argv, stdout=f)


def _build_curl_args(url: str, xml_type: bool = True) -> List[str]:
    curl_args = ["curl", "-sS", "--negotiate", "-u", ":"]
    if url.startswith("https://"):
        curl_args.append("-k")
    if xml_type:
        curl_args.extend(["-H", "Accept: application/xml"])
    curl_args.append(url)
    return curl_args


def _create_request(url: str, xml_type: bool = True) -> str:
    curl_args = _build_curl_args(url, xml_type)
    try:
        response = subprocess.run(
            curl_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        return response.stdout.decode('utf-8')
    except subprocess.CalledProcessError as e:
        response_str = "curl failed: {}".format(e.stderr.decode('utf-8'))
        logger.warning("Request failed: %s", response_str)
        return response_str


def _get_nodemanager_address(app_id: str) -> str:
    app_info = _create_request(_rm_url("ws/v1/cluster/apps/{}".format(app_id)))
    app_info_xml = ET.fromstring(app_info)
    return app_info_xml.find("amHostHttpAddress").text


def _get_node_log_address(node_address: str, link_regex: str, scheme: str = "http") -> str:
    try:
        log_page = _create_request(_web_url(scheme, node_address, "logs/"), False)
        matches = re.findall(link_regex, log_page, re.MULTILINE)
        if not matches:
            return "Warning: No matching log links found at {}://{}/logs/".format(scheme, node_address)
        return node_address + matches[0]
    except Exception as e:
        return "Failed to retrieve node logs address from {}: {}".format(node_address, e)


def _filter_node_log(node_log_address: str, start_time: str, end_time: str,
                     scheme: str = "http") -> str:
    url = _web_url(scheme, node_log_address)
    return _run_command(*_build_curl_args(url, xml_type=False), "|", "sed", "-n",
                        "'/{}/,/{}/p'".format(start_time, end_time))


def _get_container_log(log_address: str, app_id: str, scheme: str = "http") -> str:
    url = _web_url(scheme, log_address)
    grep_pattern = re.sub(r"^(job|application)", "container(_e[0-9]+)?", app_id)
    return _run_command(*_build_curl_args(url, xml_type=False), "|", "grep", "-E",
                        '"{}"'.format(grep_pattern))


def _get_application_time(app_info_string: str) -> Tuple[str, str]:
    app_element = ET.fromstring(app_info_string)
    start_time_epoch = int(app_element.find("startedTime").text)
    finish_time_epoch = int(app_element.find("finishedTime").text)

    start_time_str = datetime.fromtimestamp(start_time_epoch / 1000).strftime(
        _OUTPUT_TIME_FORMAT)[:-4]
    finish_time_str = datetime.fromtimestamp(finish_time_epoch / 1000).strftime(
        _OUTPUT_TIME_FORMAT)[:-4]

    return start_time_str, finish_time_str


def _set_rm_scheduler_log_level(log_level: str) -> str:
    cmd = ["yarn", "daemonlog", "-setlevel", _RM_ADDRESS,
           "org.apache.hadoop.yarn.server.resourcemanager.scheduler", log_level]
    if _NODE_SCHEME == "https":
        cmd.extend(["-protocol", "https"])
    return _run_command(*cmd)


def _format_datetime_no_seconds(datetime_obj: datetime) -> str:
    return datetime_obj.strftime(_OUTPUT_TIME_FORMAT_WITHOUT_SECOND)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    issue_map = {
        "application_diagnostic": application_diagnostic,
        "scheduler_related_issue": scheduler_related_issue,
    }

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--list", help="List the available issue types.", action="store_true")
    parser.add_argument("-c", "--command", choices=list(issue_map),
                        help="Initiate the diagnostic information collection "
                             "for diagnosing the selected issue type.")
    parser.add_argument("-a", "--arguments", nargs='*',
                        help="The required arguments for the selected issue type.")
    global args
    args = parser.parse_args()

    if not (args.list or args.command):
        parser.error('No action requested, use --list or --command')

    if args.list:
        list_issues()
        sys.exit(os.EX_OK)

    global _HADOOP_CONF_DIR, _RM_ADDRESS
    _HADOOP_CONF_DIR = _resolve_hadoop_conf_dir()
    _RM_ADDRESS = _resolve_rm_webapp_address()
    logger.info("Using RM webapp at %s://%s", _NODE_SCHEME, _RM_ADDRESS)

    selected_option = issue_map[args.command]
    print(selected_option())  # DiagnosticsService.java reads the output path from stdout


if __name__ == "__main__":
    main()

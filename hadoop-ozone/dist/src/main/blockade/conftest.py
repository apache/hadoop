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
import os
import time
import subprocess
import pytest

EPOCH_TIME = int(time.time())


def pytest_addoption(parser):
  parser.addoption("--output-dir",
                   action="store",
                   default="/tmp/BlockadeTests",
                   help="location of output directory where output log "
                        "and plot files will be created")
  parser.addoption("--log-format",
                   action="store",
                   default="%(asctime)s|%(levelname)s|%(threadName)s|"
                           "%(filename)s:%(lineno)s -"
                           " %(funcName)s()|%(message)s",
                   help="specify log format")
  parser.addoption("--log-level",
                   action="store",
                   default="info",
                   help="specify log level")
  parser.addoption("--containerStatusSleep",
                   action="store",
                   default="900",
                   help="sleep time before checking container status")
  parser.addoption("--runSecondPhase",
                   action="store",
                   default="false",
                   help="run second phase of the tests")


@pytest.fixture
def run_second_phase(request):
  """
  :param request:
  This function returns if the user has opted for running second phase
  of the tests.
  """
  return request.config.getoption("--runSecondPhase")


def pytest_configure(config):
  global OUTPUT_DIR
  os.environ["CONTAINER_STATUS_SLEEP"] = config.option.containerStatusSleep
  OUTPUT_DIR = "%s/%s" % (config.option.output_dir, EPOCH_TIME)
  try:
    os.makedirs(OUTPUT_DIR)
  except OSError, e:
    raise Exception(e.strerror + ": " + e.filename)
  log_file = os.path.join(OUTPUT_DIR, "output.log")

  if config.option.log_level == "trace":
    loglevel = eval("logging.DEBUG")
  else:
    loglevel = eval("logging." + config.option.log_level.upper())
  logformatter = logging.Formatter(config.option.log_format)
  logging.basicConfig(filename=log_file,
                      filemode='w',
                      level=loglevel,
                      format=config.option.log_format)
  console = logging.StreamHandler()
  console.setLevel(loglevel)
  console.setFormatter(logformatter)
  logging.getLogger('').addHandler(console)


def pytest_report_teststatus(report):
  logger = logging.getLogger('main')
  loc, line, name = report.location
  if report.outcome == 'skipped':
    pass
  elif report.when == 'setup':
    logger.info("RUNNING TEST \"%s\" at location \"%s\" at line number"
                " \"%s\"" % (name, loc, str(line)))
  elif report.when == 'call':
    logger.info("TEST \"%s\" %s in %3.2f seconds" %
                (name, report.outcome.upper(), report.duration))
    log_file_path = "%s/%s_all_docker.log" % \
                    (OUTPUT_DIR, name)
    gather_docker_logs(log_file_path)


def pytest_sessionfinish(session):
  logger = logging.getLogger('main')
  logger.info("ALL TESTS FINISHED")
  logger.info("ALL logs present in following directory: %s", OUTPUT_DIR)


def gather_docker_logs(log_file_path):
  docker_compose_file = os.environ["DOCKER_COMPOSE_FILE"]
  output = subprocess.check_output(["docker-compose", "-f",
                                    docker_compose_file, "logs"])
  with open(log_file_path, "w") as text_file:
    text_file.write(output)

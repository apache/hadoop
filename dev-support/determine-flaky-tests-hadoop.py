#!/usr/bin/env python
#
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
#
# Given a jenkins test job, this script examines all runs of the job done
# within specified period of time (number of days prior to the execution
# time of this script), and reports all failed tests.
#
# The output of this script includes a section for each run that has failed
# tests, with each failed test name listed.
#
# More importantly, at the end, it outputs a summary section to list all failed
# tests within all examined runs, and indicate how many runs a same test
# failed, and sorted all failed tests by how many runs each test failed.
#
# This way, when we see failed tests in PreCommit build, we can quickly tell
# whether a failed test is a new failure, or it failed before and how often it
# failed, so to have idea whether it may just be a flaky test.
#
# Of course, to be 100% sure about the reason of a test failure, closer look
# at the failed test for the specific run is necessary.
#
import sys
import platform
sysversion = sys.hexversion
onward30 = False
if sysversion < 0x020600F0:
  sys.exit("Minimum supported python version is 2.6, the current version is " +
      "Python" + platform.python_version())

if sysversion == 0x030000F0:
  sys.exit("There is a known bug with Python" + platform.python_version() +
      ", please try a different version");

if sysversion < 0x03000000:
  import urllib2
else:
  onward30 = True
  import urllib.request

import datetime
import json as simplejson
import logging
from optparse import OptionParser
import time

# Configuration
DEFAULT_JENKINS_URL = "https://builds.apache.org"
DEFAULT_JOB_NAME = "Hadoop-Common-trunk"
DEFAULT_NUM_PREVIOUS_DAYS = 14

SECONDS_PER_DAY = 86400

# total number of runs to examine
numRunsToExamine = 0

""" Parse arguments """
def parse_args():
  parser = OptionParser()
  parser.add_option("-J", "--jenkins-url", type="string",
                    dest="jenkins_url", help="Jenkins URL",
                    default=DEFAULT_JENKINS_URL)
  parser.add_option("-j", "--job-name", type="string",
                    dest="job_name", help="Job name to look at",
                    default=DEFAULT_JOB_NAME)
  parser.add_option("-n", "--num-days", type="int",
                    dest="num_prev_days", help="Number of days to examine",
                    default=DEFAULT_NUM_PREVIOUS_DAYS)

  (options, args) = parser.parse_args()
  if args:
    parser.error("unexpected arguments: " + repr(args))
  return options

""" Load data from specified url """
def load_url_data(url):
  if onward30:
    ourl = urllib.request.urlopen(url)
    codec = ourl.info().get_param('charset')
    content = ourl.read().decode(codec)
    data = simplejson.loads(content, strict=False)
  else:
    ourl = urllib2.urlopen(url)
    data = simplejson.load(ourl, strict=False)
  return data
 
""" List all builds of the target project. """
def list_builds(jenkins_url, job_name):
  url = "%(jenkins)s/job/%(job_name)s/api/json?tree=builds[url,result,timestamp]" % dict(
      jenkins=jenkins_url,
      job_name=job_name)

  try:
    data = load_url_data(url)

  except:
    logging.error("Could not fetch: %s" % url)
    raise
  return data['builds']

""" Find the names of any tests which failed in the given build output URL. """
def find_failing_tests(testReportApiJson, jobConsoleOutput):
  ret = set()
  try:
    data = load_url_data(testReportApiJson)

  except:
    logging.error("    Could not open testReport, check " +
        jobConsoleOutput + " for why it was reported failed")
    return ret

  for suite in data['suites']:
    for cs in suite['cases']:
      status = cs['status']
      errDetails = cs['errorDetails']
      if (status == 'REGRESSION' or status == 'FAILED' or (errDetails is not None)):
        ret.add(cs['className'] + "." + cs['name'])

  if len(ret) == 0:
    logging.info("    No failed tests in testReport, check " +
        jobConsoleOutput + " for why it was reported failed.")
  return ret

""" Iterate runs of specfied job within num_prev_days and collect results """
def find_flaky_tests(jenkins_url, job_name, num_prev_days):
  global numRunsToExamine
  all_failing = dict()
  # First list all builds
  builds = list_builds(jenkins_url, job_name)

  # Select only those in the last N days
  min_time = int(time.time()) - SECONDS_PER_DAY * num_prev_days
  builds = [b for b in builds if (int(b['timestamp']) / 1000) > min_time]

  # Filter out only those that failed
  failing_build_urls = [(b['url'] , b['timestamp']) for b in builds
      if (b['result'] in ('UNSTABLE', 'FAILURE'))]

  tnum = len(builds)
  num = len(failing_build_urls)
  numRunsToExamine = tnum
  logging.info("    THERE ARE " + str(num) + " builds (out of " + str(tnum)
      + ") that have failed tests in the past " + str(num_prev_days) + " days"
      + ((".", ", as listed below:\n")[num > 0]))

  for failed_build_with_time in failing_build_urls:
    failed_build = failed_build_with_time[0];
    jobConsoleOutput = failed_build + "Console";
    testReport = failed_build + "testReport";
    testReportApiJson = testReport + "/api/json";

    ts = float(failed_build_with_time[1]) / 1000.
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    logging.info("===>%s" % str(testReport) + " (" + st + ")")
    failing = find_failing_tests(testReportApiJson, jobConsoleOutput)
    if failing:
      for ftest in failing:
        logging.info("    Failed test: %s" % ftest)
        all_failing[ftest] = all_failing.get(ftest,0)+1

  return all_failing

def main():
  global numRunsToExamine
  logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

  # set up logger to write to stdout
  soh = logging.StreamHandler(sys.stdout)
  soh.setLevel(logging.INFO)
  logger = logging.getLogger()
  logger.removeHandler(logger.handlers[0])
  logger.addHandler(soh)

  opts = parse_args()
  logging.info("****Recently FAILED builds in url: " + opts.jenkins_url
      + "/job/" + opts.job_name + "")

  all_failing = find_flaky_tests(opts.jenkins_url, opts.job_name,
      opts.num_prev_days)
  if len(all_failing) == 0:
    raise SystemExit(0)
  logging.info("\nAmong " + str(numRunsToExamine) + " runs examined, all failed "
      + "tests <#failedRuns: testName>:")

  # print summary section: all failed tests sorted by how many times they failed
  for tn in sorted(all_failing, key=all_failing.get, reverse=True):
    logging.info("    " + str(all_failing[tn])+ ": " + tn)

if __name__ == "__main__":
  main()

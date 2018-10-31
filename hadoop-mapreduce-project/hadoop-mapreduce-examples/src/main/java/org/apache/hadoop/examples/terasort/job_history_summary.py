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

from __future__ import print_function
import re
import sys

pat = re.compile('(?P<name>[^=]+)="(?P<value>[^"]*)" *')
counterPat = re.compile('(?P<name>[^:]+):(?P<value>[^,]*),?')

def parse(tail):
  result = {}
  for n,v in re.findall(pat, tail):
    result[n] = v
  return result

mapStartTime = {}
mapEndTime = {}
reduceStartTime = {}
reduceShuffleTime = {}
reduceSortTime = {}
reduceEndTime = {}
reduceBytes = {}

for line in sys.stdin:
  words = line.split(" ",1)
  event = words[0]
  attrs = parse(words[1])
  if event == 'MapAttempt':
    if "START_TIME" in attrs:
      mapStartTime[attrs["TASKID"]] = int(attrs["START_TIME"])/1000
    elif "FINISH_TIME" in attrs:
      mapEndTime[attrs["TASKID"]] = int(attrs["FINISH_TIME"])/1000
  elif event == 'ReduceAttempt':
    if "START_TIME" in attrs:
      reduceStartTime[attrs["TASKID"]] = int(attrs["START_TIME"]) / 1000
    elif "FINISH_TIME" in attrs:
      reduceShuffleTime[attrs["TASKID"]] = int(attrs["SHUFFLE_FINISHED"])/1000
      reduceSortTime[attrs["TASKID"]] = int(attrs["SORT_FINISHED"])/1000
      reduceEndTime[attrs["TASKID"]] = int(attrs["FINISH_TIME"])/1000
  elif event == 'Task':
    if attrs["TASK_TYPE"] == "REDUCE" and "COUNTERS" in attrs:
      for n,v in re.findall(counterPat, attrs["COUNTERS"]):
        if n == "File Systems.HDFS bytes written":
          reduceBytes[attrs["TASKID"]] = int(v)

runningMaps = {}
shufflingReduces = {}
sortingReduces = {}
runningReduces = {}
startTime = min(reduce(min, mapStartTime.values()),
                reduce(min, reduceStartTime.values()))
endTime = max(reduce(max, mapEndTime.values()),
              reduce(max, reduceEndTime.values()))

reduces = sorted(reduceBytes.keys())

print("Name reduce-output-bytes shuffle-finish reduce-finish")
for r in reduces:
  print(r, reduceBytes[r], reduceShuffleTime[r] - startTime, end=' ')
  print(reduceEndTime[r] - startTime)

print()

for t in range(startTime, endTime):
  runningMaps[t] = 0
  shufflingReduces[t] = 0
  sortingReduces[t] = 0
  runningReduces[t] = 0

for map in mapStartTime.keys():
  for t in range(mapStartTime[map], mapEndTime[map]):
    runningMaps[t] += 1
for reduce in reduceStartTime.keys():
  for t in range(reduceStartTime[reduce], reduceShuffleTime[reduce]):
    shufflingReduces[t] += 1
  for t in range(reduceShuffleTime[reduce], reduceSortTime[reduce]):
    sortingReduces[t] += 1
  for t in range(reduceSortTime[reduce], reduceEndTime[reduce]):
    runningReduces[t] += 1

print("time maps shuffle merge reduce")
for t in range(startTime, endTime):
  print(t - startTime, runningMaps[t], shufflingReduces[t], sortingReduces[t], end=' ') 
  print(runningReduces[t])

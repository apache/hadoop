/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.tools.rumen;

import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptStartedEvent;
import org.apache.hadoop.mapreduce.jobhistory.TaskAttemptUnsuccessfulCompletionEvent;

public abstract class TaskAttempt20LineEventEmitter extends HistoryEventEmitter {
  static List<SingleEventEmitter> taskEventNonFinalSEEs =
      new LinkedList<SingleEventEmitter>();
  static List<SingleEventEmitter> taskEventFinalSEEs =
      new LinkedList<SingleEventEmitter>();

  static private final int DEFAULT_HTTP_PORT = 80;

  Long originalStartTime = null;
  org.apache.hadoop.mapreduce.TaskType originalTaskType = null;

  static {
    taskEventNonFinalSEEs.add(new TaskAttemptStartedEventEmitter());
    taskEventNonFinalSEEs.add(new TaskAttemptFinishedEventEmitter());
    taskEventNonFinalSEEs
        .add(new TaskAttemptUnsuccessfulCompletionEventEmitter());
  }

  protected TaskAttempt20LineEventEmitter() {
    super();
  }

  static private class TaskAttemptStartedEventEmitter extends
      SingleEventEmitter {
    HistoryEvent maybeEmitEvent(ParsedLine line, String taskAttemptIDName,
        HistoryEventEmitter thatg) {
      if (taskAttemptIDName == null) {
        return null;
      }

      TaskAttemptID taskAttemptID = TaskAttemptID.forName(taskAttemptIDName);

      String startTime = line.get("START_TIME");
      String taskType = line.get("TASK_TYPE");
      String trackerName = line.get("TRACKER_NAME");
      String httpPort = line.get("HTTP_PORT");
      String locality = line.get("LOCALITY");
      if (locality == null) {
        locality = "";
      }
      String avataar = line.get("AVATAAR");
      if (avataar == null) {
        avataar = "";
      }

      if (startTime != null && taskType != null) {
        TaskAttempt20LineEventEmitter that =
            (TaskAttempt20LineEventEmitter) thatg;

        that.originalStartTime = Long.parseLong(startTime);
        that.originalTaskType =
            Version20LogInterfaceUtils.get20TaskType(taskType);

        int port =
            httpPort.equals("") ? DEFAULT_HTTP_PORT : Integer
                .parseInt(httpPort);

        return new TaskAttemptStartedEvent(taskAttemptID,
            that.originalTaskType, that.originalStartTime, trackerName, port, -1,
            locality, avataar);
      }

      return null;
    }
  }

  static private class TaskAttemptFinishedEventEmitter extends
      SingleEventEmitter {
    HistoryEvent maybeEmitEvent(ParsedLine line, String taskAttemptIDName,
        HistoryEventEmitter thatg) {
      if (taskAttemptIDName == null) {
        return null;
      }

      TaskAttemptID taskAttemptID = TaskAttemptID.forName(taskAttemptIDName);

      String finishTime = line.get("FINISH_TIME");
      String status = line.get("TASK_STATUS");

      if (finishTime != null && status != null
          && status.equalsIgnoreCase("success")) {
        String hostName = line.get("HOSTNAME");
        String counters = line.get("COUNTERS");
        String state = line.get("STATE_STRING");

        TaskAttempt20LineEventEmitter that =
            (TaskAttempt20LineEventEmitter) thatg;

        ParsedHost pHost = ParsedHost.parse(hostName);

        return new TaskAttemptFinishedEvent(taskAttemptID,
            that.originalTaskType, status, Long.parseLong(finishTime),
            pHost.getRackName(), pHost.getNodeName(), state, 
            maybeParseCounters(counters));
      }

      return null;
    }
  }

  static private class TaskAttemptUnsuccessfulCompletionEventEmitter extends
      SingleEventEmitter {
    HistoryEvent maybeEmitEvent(ParsedLine line, String taskAttemptIDName,
        HistoryEventEmitter thatg) {
      if (taskAttemptIDName == null) {
        return null;
      }

      TaskAttemptID taskAttemptID = TaskAttemptID.forName(taskAttemptIDName);

      String finishTime = line.get("FINISH_TIME");
      String status = line.get("TASK_STATUS");

      if (finishTime != null && status != null
          && !status.equalsIgnoreCase("success")) {
        String hostName = line.get("HOSTNAME");
        String error = line.get("ERROR");

        TaskAttempt20LineEventEmitter that =
            (TaskAttempt20LineEventEmitter) thatg;

        ParsedHost pHost = ParsedHost.parse(hostName);
        String rackName = null;
        
        // Earlier versions of MR logged on hostnames (without rackname) for
        // unsuccessful attempts
        if (pHost != null) {
          rackName = pHost.getRackName();
          hostName = pHost.getNodeName();
        }
        return new TaskAttemptUnsuccessfulCompletionEvent
          (taskAttemptID,
           that.originalTaskType, status, Long.parseLong(finishTime),
           hostName, -1, rackName, error, null);
      }

      return null;
    }
  }
}

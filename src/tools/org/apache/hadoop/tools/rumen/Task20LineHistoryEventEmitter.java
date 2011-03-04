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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;

public class Task20LineHistoryEventEmitter extends HistoryEventEmitter {

  static List<SingleEventEmitter> nonFinals =
      new LinkedList<SingleEventEmitter>();
  static List<SingleEventEmitter> finals = new LinkedList<SingleEventEmitter>();

  Long originalStartTime = null;
  TaskType originalTaskType = null;

  static {
    nonFinals.add(new TaskStartedEventEmitter());
    nonFinals.add(new TaskUpdatedEventEmitter());

    finals.add(new TaskFinishedEventEmitter());
    finals.add(new TaskFailedEventEmitter());
  }

  protected Task20LineHistoryEventEmitter() {
    super();
  }

  static private class TaskStartedEventEmitter extends SingleEventEmitter {
    HistoryEvent maybeEmitEvent(ParsedLine line, String taskIDName,
        HistoryEventEmitter thatg) {
      if (taskIDName == null) {
        return null;
      }

      TaskID taskID = TaskID.forName(taskIDName);

      String taskType = line.get("TASK_TYPE");
      String startTime = line.get("START_TIME");
      String splits = line.get("SPLITS");

      if (startTime != null && taskType != null) {
        Task20LineHistoryEventEmitter that =
            (Task20LineHistoryEventEmitter) thatg;

        that.originalStartTime = Long.parseLong(startTime);
        that.originalTaskType =
            Version20LogInterfaceUtils.get20TaskType(taskType);

        return new TaskStartedEvent(taskID, that.originalStartTime,
            that.originalTaskType, splits);
      }

      return null;
    }
  }

  static private class TaskUpdatedEventEmitter extends SingleEventEmitter {
    HistoryEvent maybeEmitEvent(ParsedLine line, String taskIDName,
        HistoryEventEmitter thatg) {
      if (taskIDName == null) {
        return null;
      }

      TaskID taskID = TaskID.forName(taskIDName);

      String finishTime = line.get("FINISH_TIME");

      if (finishTime != null) {
        return new TaskUpdatedEvent(taskID, Long.parseLong(finishTime));
      }

      return null;
    }
  }

  static private class TaskFinishedEventEmitter extends SingleEventEmitter {
    HistoryEvent maybeEmitEvent(ParsedLine line, String taskIDName,
        HistoryEventEmitter thatg) {
      if (taskIDName == null) {
        return null;
      }

      TaskID taskID = TaskID.forName(taskIDName);

      String status = line.get("TASK_STATUS");
      String finishTime = line.get("FINISH_TIME");

      String error = line.get("ERROR");

      String counters = line.get("COUNTERS");

      if (finishTime != null && error == null
          && (status != null && status.equalsIgnoreCase("success"))) {
        Counters eventCounters = maybeParseCounters(counters);

        Task20LineHistoryEventEmitter that =
            (Task20LineHistoryEventEmitter) thatg;

        if (that.originalTaskType == null) {
          return null;
        }

        return new TaskFinishedEvent(taskID, Long.parseLong(finishTime),
            that.originalTaskType, status, eventCounters);
      }

      return null;
    }
  }

  static private class TaskFailedEventEmitter extends SingleEventEmitter {
    HistoryEvent maybeEmitEvent(ParsedLine line, String taskIDName,
        HistoryEventEmitter thatg) {
      if (taskIDName == null) {
        return null;
      }

      TaskID taskID = TaskID.forName(taskIDName);

      String status = line.get("TASK_STATUS");
      String finishTime = line.get("FINISH_TIME");

      String taskType = line.get("TASK_TYPE");

      String error = line.get("ERROR");

      if (finishTime != null
          && (error != null || (status != null && !status
              .equalsIgnoreCase("success")))) {
        Task20LineHistoryEventEmitter that =
            (Task20LineHistoryEventEmitter) thatg;

        TaskType originalTaskType =
            that.originalTaskType == null ? Version20LogInterfaceUtils
                .get20TaskType(taskType) : that.originalTaskType;

        return new TaskFailedEvent(taskID, Long.parseLong(finishTime),
            originalTaskType, error, status, null);
      }

      return null;
    }
  }

  @Override
  List<SingleEventEmitter> finalSEEs() {
    return finals;
  }

  @Override
  List<SingleEventEmitter> nonFinalSEEs() {
    return nonFinals;
  }
}

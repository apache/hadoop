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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobACL;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.jobhistory.HistoryEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobFinishedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobInfoChangeEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobInitedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobPriorityChangeEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobStatusChangedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobSubmittedEvent;
import org.apache.hadoop.mapreduce.jobhistory.JobUnsuccessfulCompletionEvent;
import org.apache.hadoop.security.authorize.AccessControlList;

public class Job20LineHistoryEventEmitter extends HistoryEventEmitter {

  static List<SingleEventEmitter> nonFinals =
      new LinkedList<SingleEventEmitter>();
  static List<SingleEventEmitter> finals = new LinkedList<SingleEventEmitter>();

  Long originalSubmitTime = null;

  static {
    nonFinals.add(new JobSubmittedEventEmitter());
    nonFinals.add(new JobPriorityChangeEventEmitter());
    nonFinals.add(new JobStatusChangedEventEmitter());
    nonFinals.add(new JobInitedEventEmitter());
    nonFinals.add(new JobInfoChangeEventEmitter());

    finals.add(new JobUnsuccessfulCompletionEventEmitter());
    finals.add(new JobFinishedEventEmitter());
  }

  Job20LineHistoryEventEmitter() {
    super();
  }

  static private class JobSubmittedEventEmitter extends SingleEventEmitter {
    HistoryEvent maybeEmitEvent(ParsedLine line, String jobIDName,
        HistoryEventEmitter thatg) {
      JobID jobID = JobID.forName(jobIDName);

      if (jobIDName == null) {
        return null;
      }

      String submitTime = line.get("SUBMIT_TIME");
      String jobConf = line.get("JOBCONF");
      String user = line.get("USER");
      if (user == null) {
        user = "nulluser";
      }
      String jobName = line.get("JOBNAME");
      String jobQueueName = line.get("JOB_QUEUE");// could be null
      String workflowId = line.get("WORKFLOW_ID");
      if (workflowId == null) {
        workflowId = "";
      }
      String workflowName = line.get("WORKFLOW_NAME");
      if (workflowName == null) {
        workflowName = "";
      }
      String workflowNodeName = line.get("WORKFLOW_NODE_NAME");
      if (workflowNodeName == null) {
        workflowNodeName = "";
      }
      String workflowAdjacencies = line.get("WORKFLOW_ADJACENCIES");
      if (workflowAdjacencies == null) {
        workflowAdjacencies = "";
      }
      String workflowTags = line.get("WORKFLOW_TAGS");
      if (workflowTags == null) {
        workflowTags = "";
      }
      

      if (submitTime != null) {
        Job20LineHistoryEventEmitter that =
            (Job20LineHistoryEventEmitter) thatg;

        that.originalSubmitTime = Long.parseLong(submitTime);

        Map<JobACL, AccessControlList> jobACLs =
          new HashMap<JobACL, AccessControlList>();
        return new JobSubmittedEvent(jobID, jobName, user,
            that.originalSubmitTime, jobConf, jobACLs, jobQueueName,
            workflowId, workflowName, workflowNodeName, workflowAdjacencies,
            workflowTags);
      }

      return null;
    }
  }

  static private class JobPriorityChangeEventEmitter extends SingleEventEmitter {
    HistoryEvent maybeEmitEvent(ParsedLine line, String jobIDName,
        HistoryEventEmitter thatg) {
      JobID jobID = JobID.forName(jobIDName);

      if (jobIDName == null) {
        return null;
      }

      String priority = line.get("JOB_PRIORITY");

      if (priority != null) {
        return new JobPriorityChangeEvent(jobID, JobPriority.valueOf(priority));
      }

      return null;
    }
  }

  static private class JobInitedEventEmitter extends SingleEventEmitter {
    HistoryEvent maybeEmitEvent(ParsedLine line, String jobIDName,
        HistoryEventEmitter thatg) {
      if (jobIDName == null) {
        return null;
      }

      JobID jobID = JobID.forName(jobIDName);

      String launchTime = line.get("LAUNCH_TIME");
      String status = line.get("JOB_STATUS");
      String totalMaps = line.get("TOTAL_MAPS");
      String totalReduces = line.get("TOTAL_REDUCES");
      String uberized = line.get("UBERIZED");

      if (launchTime != null && totalMaps != null && totalReduces != null) {
        return new JobInitedEvent(jobID, Long.parseLong(launchTime), Integer
            .parseInt(totalMaps), Integer.parseInt(totalReduces), status,
            Boolean.parseBoolean(uberized));
      }

      return null;
    }
  }

  static private class JobStatusChangedEventEmitter extends SingleEventEmitter {
    HistoryEvent maybeEmitEvent(ParsedLine line, String jobIDName,
        HistoryEventEmitter thatg) {
      if (jobIDName == null) {
        return null;
      }

      JobID jobID = JobID.forName(jobIDName);

      String status = line.get("JOB_STATUS");

      if (status != null) {
        return new JobStatusChangedEvent(jobID, status);
      }

      return null;
    }
  }

  static private class JobInfoChangeEventEmitter extends SingleEventEmitter {
    HistoryEvent maybeEmitEvent(ParsedLine line, String jobIDName,
        HistoryEventEmitter thatg) {
      if (jobIDName == null) {
        return null;
      }

      JobID jobID = JobID.forName(jobIDName);

      String launchTime = line.get("LAUNCH_TIME");

      if (launchTime != null) {
        Job20LineHistoryEventEmitter that =
            (Job20LineHistoryEventEmitter) thatg;
        return new JobInfoChangeEvent(jobID, that.originalSubmitTime, Long
            .parseLong(launchTime));
      }

      return null;
    }
  }

  static private class JobUnsuccessfulCompletionEventEmitter extends
      SingleEventEmitter {
    HistoryEvent maybeEmitEvent(ParsedLine line, String jobIDName,
        HistoryEventEmitter thatg) {
      if (jobIDName == null) {
        return null;
      }

      JobID jobID = JobID.forName(jobIDName);

      String finishTime = line.get("FINISH_TIME");

      String status = line.get("JOB_STATUS");

      String finishedMaps = line.get("FINISHED_MAPS");
      String finishedReduces = line.get("FINISHED_REDUCES");

      if (status != null && !status.equalsIgnoreCase("success")
          && finishTime != null && finishedMaps != null
          && finishedReduces != null) {
        return new JobUnsuccessfulCompletionEvent(jobID, Long
            .parseLong(finishTime), Integer.parseInt(finishedMaps), Integer
            .parseInt(finishedReduces), -1, -1, -1, -1, status);
      }

      return null;
    }
  }

  static private class JobFinishedEventEmitter extends SingleEventEmitter {
    HistoryEvent maybeEmitEvent(ParsedLine line, String jobIDName,
        HistoryEventEmitter thatg) {
      if (jobIDName == null) {
        return null;
      }

      JobID jobID = JobID.forName(jobIDName);

      String finishTime = line.get("FINISH_TIME");

      String status = line.get("JOB_STATUS");

      String finishedMaps = line.get("FINISHED_MAPS");
      String finishedReduces = line.get("FINISHED_REDUCES");

      String failedMaps = line.get("FAILED_MAPS");
      String failedReduces = line.get("FAILED_REDUCES");

      String counters = line.get("COUNTERS");

      if (status != null && status.equalsIgnoreCase("success")
          && finishTime != null && finishedMaps != null
          && finishedReduces != null) {
        return new JobFinishedEvent(jobID, Long.parseLong(finishTime), Integer
            .parseInt(finishedMaps), Integer.parseInt(finishedReduces), Integer
            .parseInt(failedMaps), Integer.parseInt(failedReduces), -1, -1,
            null, null, maybeParseCounters(counters));
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

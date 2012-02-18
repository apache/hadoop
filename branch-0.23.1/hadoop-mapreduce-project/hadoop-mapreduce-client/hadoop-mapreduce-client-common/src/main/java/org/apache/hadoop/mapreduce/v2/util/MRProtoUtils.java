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

package org.apache.hadoop.mapreduce.v2.util;

import org.apache.hadoop.mapreduce.v2.api.records.JobState;
import org.apache.hadoop.mapreduce.v2.api.records.Phase;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptCompletionEventStatus;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.JobStateProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.PhaseProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptCompletionEventStatusProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptStateProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskStateProto;
import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskTypeProto;

public class MRProtoUtils {

  /*
   * JobState
   */
  private static String JOB_STATE_PREFIX = "J_";
  public static JobStateProto convertToProtoFormat(JobState e) {
    return JobStateProto.valueOf(JOB_STATE_PREFIX + e.name());
  }
  public static JobState convertFromProtoFormat(JobStateProto e) {
    return JobState.valueOf(e.name().replace(JOB_STATE_PREFIX, ""));
  }
  
  /*
   * Phase
   */
  private static String PHASE_PREFIX = "P_";
  public static PhaseProto convertToProtoFormat(Phase e) {
    return PhaseProto.valueOf(PHASE_PREFIX + e.name());
  }
  public static Phase convertFromProtoFormat(PhaseProto e) {
    return Phase.valueOf(e.name().replace(PHASE_PREFIX, ""));
  }
  
  /*
   * TaskAttemptCompletionEventStatus
   */
  private static String TACE_PREFIX = "TACE_";
  public static TaskAttemptCompletionEventStatusProto convertToProtoFormat(TaskAttemptCompletionEventStatus e) {
    return TaskAttemptCompletionEventStatusProto.valueOf(TACE_PREFIX + e.name());
  }
  public static TaskAttemptCompletionEventStatus convertFromProtoFormat(TaskAttemptCompletionEventStatusProto e) {
    return TaskAttemptCompletionEventStatus.valueOf(e.name().replace(TACE_PREFIX, ""));
  }
  
  /*
   * TaskAttemptState
   */
  private static String TASK_ATTEMPT_STATE_PREFIX = "TA_";
  public static TaskAttemptStateProto convertToProtoFormat(TaskAttemptState e) {
    return TaskAttemptStateProto.valueOf(TASK_ATTEMPT_STATE_PREFIX + e.name());
  }
  public static TaskAttemptState convertFromProtoFormat(TaskAttemptStateProto e) {
    return TaskAttemptState.valueOf(e.name().replace(TASK_ATTEMPT_STATE_PREFIX, ""));
  }
  
  /*
   * TaskState
   */
  private static String TASK_STATE_PREFIX = "TS_";
  public static TaskStateProto convertToProtoFormat(TaskState e) {
    return TaskStateProto.valueOf(TASK_STATE_PREFIX + e.name());
  }
  public static TaskState convertFromProtoFormat(TaskStateProto e) {
    return TaskState.valueOf(e.name().replace(TASK_STATE_PREFIX, ""));
  }
  
  /*
   * TaskType
   */
  public static TaskTypeProto convertToProtoFormat(TaskType e) {
    return TaskTypeProto.valueOf(e.name());
  }
  public static TaskType convertFromProtoFormat(TaskTypeProto e) {
    return TaskType.valueOf(e.name());
  }
}

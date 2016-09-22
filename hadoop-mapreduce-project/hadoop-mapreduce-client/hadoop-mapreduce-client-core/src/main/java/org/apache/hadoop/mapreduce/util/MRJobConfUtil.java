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
package org.apache.hadoop.mapreduce.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * A class that contains utility methods for MR Job configuration.
 */
public final class MRJobConfUtil {
  public static final String REDACTION_REPLACEMENT_VAL = "*********(redacted)";

  /**
   * Redact job configuration properties.
   * @param conf the job configuration to redact
   */
  public static void redact(final Configuration conf) {
    for (String prop : conf.getTrimmedStringCollection(
        MRJobConfig.MR_JOB_REDACTED_PROPERTIES)) {
      conf.set(prop, REDACTION_REPLACEMENT_VAL);
    }
  }

  /**
   * There is no reason to instantiate this utility class.
   */
  private MRJobConfUtil() {
  }

  /**
   * Get the progress heartbeat interval configuration for mapreduce tasks.
   * By default, the value of progress heartbeat interval is a proportion of
   * that of task timeout.
   * @param conf  the job configuration to read from
   * @return the value of task progress report interval
   */
  public static long getTaskProgressReportInterval(final Configuration conf) {
    long taskHeartbeatTimeOut = conf.getLong(
        MRJobConfig.TASK_TIMEOUT, MRJobConfig.DEFAULT_TASK_TIMEOUT_MILLIS);
    return conf.getLong(MRJobConfig.TASK_PROGRESS_REPORT_INTERVAL,
        (long) (TASK_REPORT_INTERVAL_TO_TIMEOUT_RATIO * taskHeartbeatTimeOut));
  }

  public static final float TASK_REPORT_INTERVAL_TO_TIMEOUT_RATIO = 0.01f;
}

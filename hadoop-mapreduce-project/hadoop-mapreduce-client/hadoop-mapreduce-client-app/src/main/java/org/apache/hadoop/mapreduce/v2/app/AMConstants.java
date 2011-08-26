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

package org.apache.hadoop.mapreduce.v2.app;

import org.apache.hadoop.mapreduce.v2.MRConstants;

public interface AMConstants {

  public static final String CONTAINERLAUNCHER_THREADPOOL_SIZE =
      "yarn.mapreduce.containerlauncher.threadpool-size";

  public static final String AM_RM_SCHEDULE_INTERVAL =
      "yarn.appMaster.scheduler.interval";

  public static final int DEFAULT_AM_RM_SCHEDULE_INTERVAL = 2000;

  public static final String AM_TASK_LISTENER_THREADS =
      MRConstants.YARN_MR_PREFIX + "task.listener.threads";

  public static final int DEFAULT_AM_TASK_LISTENER_THREADS = 10;

  public static final String AM_JOB_CLIENT_THREADS =
      MRConstants.YARN_MR_PREFIX + "job.client.threads";

  public static final int DEFAULT_AM_JOB_CLIENT_THREADS = 1;

  public static final String SPECULATOR_CLASS =
      MRConstants.YARN_MR_PREFIX + "speculator.class";

  public static final String TASK_RUNTIME_ESTIMATOR_CLASS =
      MRConstants.YARN_MR_PREFIX + "task.runtime.estimator.class";

  public static final String TASK_ATTEMPT_PROGRESS_RUNTIME_LINEARIZER_CLASS =
      MRConstants.YARN_MR_PREFIX + "task.runtime.linearizer.class";

  public static final String EXPONENTIAL_SMOOTHING_LAMBDA_MILLISECONDS =
      MRConstants.YARN_MR_PREFIX
          + "task.runtime.estimator.exponential.smooth.lambda";

  public static final String EXPONENTIAL_SMOOTHING_SMOOTH_RATE =
      MRConstants.YARN_MR_PREFIX
          + "task.runtime.estimator.exponential.smooth.smoothsrate";

  public static final String RECOVERY_ENABLE = MRConstants.YARN_MR_PREFIX
      + "recovery.enable";
  
  public static final float DEFAULT_REDUCE_RAMP_UP_LIMIT = 0.5f;
  public static final String REDUCE_RAMPUP_UP_LIMIT = MRConstants.YARN_MR_PREFIX
  + "reduce.rampup.limit";
  
  public static final float DEFAULT_REDUCE_PREEMPTION_LIMIT = 0.5f;
  public static final String REDUCE_PREEMPTION_LIMIT = MRConstants.YARN_MR_PREFIX
  + "reduce.preemption.limit";

  public static final String NODE_BLACKLISTING_ENABLE = MRConstants.YARN_MR_PREFIX
  + "node.blacklisting.enable";
  
}

/*
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

package org.apache.hadoop.fs.s3a.commit.impl;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Preconditions;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.BASE;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.JOB_ID_PREFIX;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.MAGIC_PATH_PREFIX;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.TEMP_DATA;

/**
 * These are commit utility methods which import classes from
 * hadoop-mapreduce, and so only work when that module is on the
 * classpath.
 *
 * <b>Do not use in any codepath intended to be used from the S3AFS
 * except in the committers themselves.</b>
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class CommitUtilsWithMR {

  private CommitUtilsWithMR() {
  }

  /**
   * Get the location of magic job attempts.
   * @param out the base output directory.
   * @param jobUUID unique Job ID.
   * @return the location of magic job attempts.
   */
  public static Path getMagicJobAttemptsPath(Path out, String jobUUID) {
    Preconditions.checkArgument(jobUUID != null && !(jobUUID.isEmpty()),
        "Invalid job ID: %s", jobUUID);
    return new Path(out, MAGIC_PATH_PREFIX + jobUUID);
  }

  /**
   * Get the Application Attempt ID for this job.
   * @param context the context to look in
   * @return the Application Attempt ID for a given job, or 0
   */
  public static int getAppAttemptId(JobContext context) {
    return context.getConfiguration().getInt(
        MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
  }

  /**
   * Compute the "magic" path for a job attempt.
   * @param jobUUID unique Job ID.
   * @param appAttemptId the ID of the application attempt for this job.
   * @param dest the final output directory
   * @return the path to store job attempt data.
   */
  public static Path getMagicJobAttemptPath(String jobUUID,
      int appAttemptId,
      Path dest) {
    return new Path(
        getMagicJobAttemptsPath(dest, jobUUID),
        formatAppAttemptDir(jobUUID, appAttemptId));
  }

  /**
   * Compute the "magic" path for a job.
   * @param jobUUID unique Job ID.
   * @param dest the final output directory
   * @return the path to store job attempt data.
   */
  public static Path getMagicJobPath(String jobUUID,
      Path dest) {
    return getMagicJobAttemptsPath(dest, jobUUID);
  }

  /**
   * Build the name of the job directory, without
   * app attempt.
   * This is the path to use for cleanup.
   * @param jobUUID unique Job ID.
   * @return the directory name for the job
   */
  public static String formatJobDir(
      String jobUUID) {
    return JOB_ID_PREFIX + jobUUID;
  }

  /**
   * Build the name of the job attempt directory.
   * @param jobUUID unique Job ID.
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the directory tree for the application attempt
   */
  public static String formatAppAttemptDir(
      String jobUUID,
      int appAttemptId) {
    return formatJobDir(jobUUID) + String.format("/%02d", appAttemptId);
  }

  /**
   * Compute the path where the output of magic task attempts are stored.
   * @param jobUUID unique Job ID.
   * @param dest destination of work
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path where the output of magic task attempts are stored.
   */
  public static Path getMagicTaskAttemptsPath(
      String jobUUID,
      Path dest,
      int appAttemptId) {
    return new Path(getMagicJobAttemptPath(jobUUID, appAttemptId, dest), "tasks");
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   * This path is marked as a base path for relocations, so subdirectory
   * information is preserved.
   * @param context the context of the task attempt.
   * @param jobUUID unique Job ID.
   * @param dest The output path to commit work into
   * @return the path where a task attempt should be stored.
   */
  public static Path getMagicTaskAttemptPath(TaskAttemptContext context,
      String jobUUID,
      Path dest) {
    return new Path(getBaseMagicTaskAttemptPath(context, jobUUID, dest),
        BASE);
  }

  /**
   * Get the base Magic attempt path, without any annotations to mark relative
   * references.
   * If there is an app attempt property in the context configuration, that
   * is included.
   * @param context task context.
   * @param jobUUID unique Job ID.
   * @param dest The output path to commit work into
   * @return the path under which all attempts go
   */
  public static Path getBaseMagicTaskAttemptPath(TaskAttemptContext context,
      String jobUUID,
      Path dest) {
    return new Path(
        getMagicTaskAttemptsPath(jobUUID, dest, getAppAttemptId(context)),
        String.valueOf(context.getTaskAttemptID()));
  }

  /**
   * Compute a path for temporary data associated with a job.
   * This data is <i>not magic</i>
   * @param jobUUID unique Job ID.
   * @param out output directory of job
   * @param appAttemptId the ID of the application attempt for this job.
   * @return the path to store temporary job attempt data.
   */
  public static Path getTempJobAttemptPath(String jobUUID,
      Path out, final int appAttemptId) {
    return new Path(new Path(out, TEMP_DATA),
        formatAppAttemptDir(jobUUID, appAttemptId));
  }

  /**
   * Compute the path where the output of a given task attempt will be placed.
   * @param context task context
   * @param jobUUID unique Job ID.
   * @param out output directory of job
   * @return the path to store temporary job attempt data.
   */
  public static Path getTempTaskAttemptPath(TaskAttemptContext context,
      final String jobUUID, Path out) {
    return new Path(
        getTempJobAttemptPath(jobUUID, out, getAppAttemptId(context)),
        String.valueOf(context.getTaskAttemptID()));
  }

  /**
   * Get a string value of a job ID; returns meaningful text if there is no ID.
   * @param context job context
   * @return a string for logs
   */
  public static String jobIdString(JobContext context) {
    JobID jobID = context.getJobID();
    return jobID != null ? jobID.toString() : "(no job ID)";
  }

  /**
   * Get a job name; returns meaningful text if there is no name.
   * @param context job context
   * @return a string for logs
   */
  public static String jobName(JobContext context) {
    String name = context.getJobName();
    return (name != null && !name.isEmpty()) ? name : "(anonymous)";
  }

  /**
   * Get a configuration option, with any value in the job configuration
   * taking priority over that in the filesystem.
   * This allows for per-job override of FS parameters.
   *
   * Order is: job context, filesystem config, default value
   *
   * @param context job/task context
   * @param fsConf filesystem configuration. Get this from the FS to guarantee
   * per-bucket parameter propagation
   * @param key key to look for
   * @param defVal default value
   * @return the configuration option.
   */
  public static String getConfigurationOption(
      JobContext context,
      Configuration fsConf,
      String key,
      String defVal) {
    return context.getConfiguration().getTrimmed(key,
        fsConf.getTrimmed(key, defVal));
  }
}

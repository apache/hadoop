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

package org.apache.hadoop.fs.s3a.commit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.BASE;
import static org.apache.hadoop.fs.s3a.commit.CommitConstants.MAGIC;
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
   * @return the location of magic job attempts.
   */
  public static Path getMagicJobAttemptsPath(Path out) {
    return new Path(out, MAGIC);
  }

  /**
   * Get the Application Attempt ID for this job.
   * @param context the context to look in
   * @return the Application Attempt ID for a given job.
   */
  public static int getAppAttemptId(JobContext context) {
    return context.getConfiguration().getInt(
        MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
  }

  /**
   * Compute the "magic" path for a job attempt.
   * @param appAttemptId the ID of the application attempt for this job.
   * @param dest the final output directory
   * @return the path to store job attempt data.
   */
  public static Path getMagicJobAttemptPath(int appAttemptId, Path dest) {
    return new Path(getMagicJobAttemptsPath(dest),
        formatAppAttemptDir(appAttemptId));
  }

  /**
   * Format the application attempt directory.
   * @param attemptId attempt ID
   * @return the directory name for the application attempt
   */
  public static String formatAppAttemptDir(int attemptId) {
    return String.format("app-attempt-%04d", attemptId);
  }

  /**
   * Compute the path where the output of magic task attempts are stored.
   * @param context the context of the job with magic tasks.
   * @param dest destination of work
   * @return the path where the output of magic task attempts are stored.
   */
  public static Path getMagicTaskAttemptsPath(JobContext context, Path dest) {
    return new Path(getMagicJobAttemptPath(
        getAppAttemptId(context), dest), "tasks");
  }

  /**
   * Compute the path where the output of a task attempt is stored until
   * that task is committed.
   * This path is marked as a base path for relocations, so subdirectory
   * information is preserved.
   * @param context the context of the task attempt.
   * @param dest The output path to commit work into
   * @return the path where a task attempt should be stored.
   */
  public static Path getMagicTaskAttemptPath(TaskAttemptContext context,
      Path dest) {
    return new Path(getBaseMagicTaskAttemptPath(context, dest), BASE);
  }

  /**
   * Get the base Magic attempt path, without any annotations to mark relative
   * references.
   * @param context task context.
   * @param dest The output path to commit work into
   * @return the path under which all attempts go
   */
  public static Path getBaseMagicTaskAttemptPath(TaskAttemptContext context,
      Path dest) {
    return new Path(getMagicTaskAttemptsPath(context, dest),
          String.valueOf(context.getTaskAttemptID()));
  }

  /**
   * Compute a path for temporary data associated with a job.
   * This data is <i>not magic</i>
   * @param appAttemptId the ID of the application attempt for this job.
   * @param out output directory of job
   * @return the path to store temporary job attempt data.
   */
  public static Path getTempJobAttemptPath(int appAttemptId, Path out) {
    return new Path(new Path(out, TEMP_DATA),
        formatAppAttemptDir(appAttemptId));
  }

  /**
   * Compute the path where the output of a given job attempt will be placed.
   * @param context task context
   * @param out output directory of job
   * @return the path to store temporary job attempt data.
   */
  public static Path getTempTaskAttemptPath(TaskAttemptContext context,
      Path out) {
    return new Path(getTempJobAttemptPath(getAppAttemptId(context), out),
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

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

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;

/**
 * A class that contains utility methods for MR Job configuration.
 */
public final class MRJobConfUtil {
  private static final Logger LOG =
      LoggerFactory.getLogger(MRJobConfUtil.class);
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

  /**
   * Configurations to control the frequency of logging of task Attempt.
   */
  public static final double PROGRESS_MIN_DELTA_FACTOR = 100.0;
  private static volatile Double progressMinDeltaThreshold = null;
  private static volatile Long progressMaxWaitDeltaTimeThreshold = null;

  /**
   * load the values defined from a configuration file including the delta
   * progress and the maximum time between each log message.
   * @param conf
   */
  public static void setTaskLogProgressDeltaThresholds(
      final Configuration conf) {
    if (progressMinDeltaThreshold == null) {
      progressMinDeltaThreshold =
          new Double(PROGRESS_MIN_DELTA_FACTOR
              * conf.getDouble(MRJobConfig.TASK_LOG_PROGRESS_DELTA_THRESHOLD,
              MRJobConfig.TASK_LOG_PROGRESS_DELTA_THRESHOLD_DEFAULT));
    }

    if (progressMaxWaitDeltaTimeThreshold == null) {
      progressMaxWaitDeltaTimeThreshold =
          TimeUnit.SECONDS.toMillis(conf
              .getLong(
                  MRJobConfig.TASK_LOG_PROGRESS_WAIT_INTERVAL_SECONDS,
                  MRJobConfig.TASK_LOG_PROGRESS_WAIT_INTERVAL_SECONDS_DEFAULT));
    }
  }

  /**
   * Retrieves the min delta progress required to log the task attempt current
   * progress.
   * @return the defined threshold in the conf.
   *         returns the default value if
   *         {@link #setTaskLogProgressDeltaThresholds} has not been called.
   */
  public static double getTaskProgressMinDeltaThreshold() {
    if (progressMinDeltaThreshold == null) {
      return PROGRESS_MIN_DELTA_FACTOR
          * MRJobConfig.TASK_LOG_PROGRESS_DELTA_THRESHOLD_DEFAULT;
    }
    return progressMinDeltaThreshold.doubleValue();
  }

  /**
   * Retrieves the min time required to log the task attempt current
   * progress.
   * @return the defined threshold in the conf.
   *         returns the default value if
   *         {@link #setTaskLogProgressDeltaThresholds} has not been called.
   */
  public static long getTaskProgressWaitDeltaTimeThreshold() {
    if (progressMaxWaitDeltaTimeThreshold == null) {
      return TimeUnit.SECONDS.toMillis(
          MRJobConfig.TASK_LOG_PROGRESS_WAIT_INTERVAL_SECONDS_DEFAULT);
    }
    return progressMaxWaitDeltaTimeThreshold.longValue();
  }

  /**
   * Coverts a progress between 0.0 to 1.0 to double format used to log the
   * task attempt.
   * @param progress of the task which is a value between 0.0 and 1.0.
   * @return the double value that is less than or equal to the argument
   *          multiplied by {@link #PROGRESS_MIN_DELTA_FACTOR}.
   */
  public static double convertTaskProgressToFactor(final float progress) {
    return Math.floor(progress * MRJobConfUtil.PROGRESS_MIN_DELTA_FACTOR);
  }

  /**
   * For unit tests, use urandom to avoid the YarnChild  process from hanging
   * on low entropy systems.
   */
  private static final String TEST_JVM_SECURITY_EGD_OPT =
      "-Djava.security.egd=file:/dev/./urandom";

  public static Configuration initEncryptedIntermediateConfigsForTesting(
      Configuration conf) {
    Configuration config =
        (conf == null) ? new Configuration(): conf;
    final String childJVMOpts =
        TEST_JVM_SECURITY_EGD_OPT.concat(" ")
            .concat(config.get("mapred.child.java.opts", " "));
    // Set the jvm arguments.
    config.set("yarn.app.mapreduce.am.admin-command-opts",
        TEST_JVM_SECURITY_EGD_OPT);
    config.set("mapred.child.java.opts", childJVMOpts);
    config.setBoolean("mapreduce.job.encrypted-intermediate-data", true);
    return config;
  }

  /**
   * Set local directories so that the generated folders is subdirectory of the
   * test directories.
   * @param conf
   * @param testRootDir
   * @return
   */
  public static Configuration setLocalDirectoriesConfigForTesting(
      Configuration conf, File testRootDir) {
    Configuration config =
        (conf == null) ? new Configuration(): conf;
    final File hadoopLocalDir = new File(testRootDir, "hadoop-dir");
    // create the directory
    if (!hadoopLocalDir.getAbsoluteFile().mkdirs()) {
      LOG.info("{} directory already exists", hadoopLocalDir.getPath());
    }
    Path mapredHadoopTempDir = new Path(hadoopLocalDir.getPath());
    Path mapredSystemDir = new Path(mapredHadoopTempDir, "system");
    Path stagingDir = new Path(mapredHadoopTempDir, "tmp/staging");
    // Set the temp directories a subdir of the test directory.
    config.set("mapreduce.jobtracker.staging.root.dir", stagingDir.toString());
    config.set("mapreduce.jobtracker.system.dir", mapredSystemDir.toString());
    config.set("mapreduce.cluster.temp.dir", mapredHadoopTempDir.toString());
    config.set("mapreduce.cluster.local.dir",
        new Path(mapredHadoopTempDir, "local").toString());
    return config;
  }
}

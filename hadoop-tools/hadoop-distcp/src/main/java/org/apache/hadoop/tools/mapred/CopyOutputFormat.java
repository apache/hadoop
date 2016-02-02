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

package org.apache.hadoop.tools.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.tools.DistCpConstants;

import java.io.IOException;

/**
 * The CopyOutputFormat is the Hadoop OutputFormat used in DistCp.
 * It sets up the Job's Configuration (in the Job-Context) with the settings
 * for the work-directory, final commit-directory, etc. It also sets the right
 * output-committer.
 * @param <K>
 * @param <V>
 */
public class CopyOutputFormat<K, V> extends TextOutputFormat<K, V> {

  /**
   * Setter for the working directory for DistCp (where files will be copied
   * before they are moved to the final commit-directory.)
   * @param job The Job on whose configuration the working-directory is to be set.
   * @param workingDirectory The path to use as the working directory.
   */
  public static void setWorkingDirectory(Job job, Path workingDirectory) {
    job.getConfiguration().set(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH,
        workingDirectory.toString());
  }

  /**
   * Setter for the final directory for DistCp (where files copied will be
   * moved, atomically.)
   * @param job The Job on whose configuration the working-directory is to be set.
   * @param commitDirectory The path to use for final commit.
   */
  public static void setCommitDirectory(Job job, Path commitDirectory) {
    job.getConfiguration().set(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH,
        commitDirectory.toString());
  }

  /**
   * Getter for the working directory.
   * @param job The Job from whose configuration the working-directory is to
   * be retrieved.
   * @return The working-directory Path.
   */
  public static Path getWorkingDirectory(Job job) {
    return getWorkingDirectory(job.getConfiguration());
  }

  private static Path getWorkingDirectory(Configuration conf) {
    String workingDirectory = conf.get(DistCpConstants.CONF_LABEL_TARGET_WORK_PATH);
    if (workingDirectory == null || workingDirectory.isEmpty()) {
      return null;
    } else {
      return new Path(workingDirectory);
    }
  }

  /**
   * Getter for the final commit-directory.
   * @param job The Job from whose configuration the commit-directory is to be
   * retrieved.
   * @return The commit-directory Path.
   */
  public static Path getCommitDirectory(Job job) {
    return getCommitDirectory(job.getConfiguration());
  }

  private static Path getCommitDirectory(Configuration conf) {
    String commitDirectory = conf.get(DistCpConstants.CONF_LABEL_TARGET_FINAL_PATH);
    if (commitDirectory == null || commitDirectory.isEmpty()) {
      return null;
    } else {
      return new Path(commitDirectory);
    }
  }

  /** {@inheritDoc} */
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
    return new CopyCommitter(getOutputPath(context), context);
  }

  /** {@inheritDoc} */
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException {
    Configuration conf = context.getConfiguration();

    if (getCommitDirectory(conf) == null) {
      throw new IllegalStateException("Commit directory not configured");
    }

    Path workingPath = getWorkingDirectory(conf);
    if (workingPath == null) {
      throw new IllegalStateException("Working directory not configured");
    }

    // get delegation token for outDir's file system
    TokenCache.obtainTokensForNamenodes(context.getCredentials(),
                                        new Path[] {workingPath}, conf);
  }
}

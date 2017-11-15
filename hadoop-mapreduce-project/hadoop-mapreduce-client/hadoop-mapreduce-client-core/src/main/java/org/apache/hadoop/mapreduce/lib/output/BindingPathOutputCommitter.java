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

package org.apache.hadoop.mapreduce.lib.output;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This is a special committer which creates the factory for the committer and
 * runs off that. Why does it exist? So that you can explicitly instantiate
 * a committer by classname and yet still have the actual implementation
 * driven dynamically by the factory options and destination filesystem.
 * This simplifies integration
 * with existing code which takes the classname of a committer.
 * There's no factory for this, as that would lead to a loop.
 *
 * All commit protocol methods and accessors are delegated to the
 * wrapped committer.
 *
 * How to use:
 *
 * <ol>
 *   <li>
 *     In applications which take a classname of committer in
 *     a configuration option, set it to the canonical name of this class
 *     (see {@link #NAME}). When this class is instantiated, it will
 *     use the factory mechanism to locate the configured committer for the
 *     destination.
 *   </li>
 *   <li>
 *     In code, explicitly create an instance of this committer through
 *     its constructor, then invoke commit lifecycle operations on it.
 *     The dynamically configured committer will be created in the constructor
 *     and have the lifecycle operations relayed to it.
 *   </li>
 * </ol>
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class BindingPathOutputCommitter extends PathOutputCommitter {

  /**
   * The classname for use in configurations.
   */
  public static final String NAME
      = BindingPathOutputCommitter.class.getCanonicalName();

  /**
   * The bound committer.
   */
  private final PathOutputCommitter committer;

  /**
   * Instantiate.
   * @param outputPath output path (may be null)
   * @param context task context
   * @throws IOException on any failure.
   */
  public BindingPathOutputCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    committer = PathOutputCommitterFactory.getCommitterFactory(outputPath,
        context.getConfiguration())
        .createOutputCommitter(outputPath, context);
  }

  @Override
  public Path getOutputPath() {
    return committer.getOutputPath();
  }

  @Override
  public Path getWorkPath() throws IOException {
    return committer.getWorkPath();
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    committer.setupJob(jobContext);
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
    committer.setupTask(taskContext);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext)
      throws IOException {
    return committer.needsTaskCommit(taskContext);
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
    committer.commitTask(taskContext);
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {
    committer.abortTask(taskContext);
  }

  @Override
  @SuppressWarnings("deprecation")
  public void cleanupJob(JobContext jobContext) throws IOException {
    super.cleanupJob(jobContext);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    committer.commitJob(jobContext);
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state)
      throws IOException {
    committer.abortJob(jobContext, state);
  }

  @SuppressWarnings("deprecation")
  @Override
  public boolean isRecoverySupported() {
    return committer.isRecoverySupported();
  }

  @Override
  public boolean isCommitJobRepeatable(JobContext jobContext)
      throws IOException {
    return committer.isCommitJobRepeatable(jobContext);
  }

  @Override
  public boolean isRecoverySupported(JobContext jobContext) throws IOException {
    return committer.isRecoverySupported(jobContext);
  }

  @Override
  public void recoverTask(TaskAttemptContext taskContext) throws IOException {
    committer.recoverTask(taskContext);
  }

  @Override
  public boolean hasOutputPath() {
    return committer.hasOutputPath();
  }

  @Override
  public String toString() {
    return "BindingPathOutputCommitter{"
        + "committer=" + committer +
        '}';
  }

  /**
   * Get the inner committer.
   * @return the bonded committer.
   */
  public PathOutputCommitter getCommitter() {
    return committer;
  }
}

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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory;
import org.apache.hadoop.fs.s3a.commit.staging.DirectoryStagingCommitterFactory;
import org.apache.hadoop.fs.s3a.commit.staging.PartitionedStagingCommitterFactory;
import org.apache.hadoop.fs.s3a.commit.staging.StagingCommitterFactory;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;

import static org.apache.hadoop.fs.s3a.commit.CommitConstants.*;

/**
 * The S3A committer factory which chooses the committer based on the
 * specific option chosen in a per-bucket basis from the property
 * {@link CommitConstants#FS_S3A_COMMITTER_NAME}.
 *
 * This should be instantiated by using the property value {@link #CLASSNAME}
 * as the committer for the job, then set the filesystem property
 * {@link CommitConstants#FS_S3A_COMMITTER_NAME} to one of
 * <ul>
 *   <li>{@link CommitConstants#COMMITTER_NAME_FILE}: File committer.</li>
 *   <li>{@link CommitConstants#COMMITTER_NAME_DIRECTORY}:
 *   Staging directory committer.</li>
 *   <li>{@link CommitConstants#COMMITTER_NAME_PARTITIONED}:
 *   Staging partitioned committer.</li>
 *   <li>{@link CommitConstants#COMMITTER_NAME_MAGIC}:
 *   the "Magic" committer</li>
 *   <li>{@link InternalCommitterConstants#COMMITTER_NAME_STAGING}:
 *   the "staging" committer, which isn't intended for use outside tests.</li>
 * </ul>
 * There are no checks to verify that the filesystem is compatible with
 * the committer.
 */
public class S3ACommitterFactory extends AbstractS3ACommitterFactory {

  /**
   * Name of this class: {@value}.
   */
  public static final String CLASSNAME
      = "org.apache.hadoop.fs.s3a.commit.S3ACommitterFactory";

  /**
   * Create a task committer.
   * @param fileSystem destination FS.
   * @param outputPath final output path for work
   * @param context job context
   * @return a committer
   * @throws IOException instantiation failure
   */
  @Override
  public PathOutputCommitter createTaskCommitter(S3AFileSystem fileSystem,
      Path outputPath,
      TaskAttemptContext context) throws IOException {
    AbstractS3ACommitterFactory factory = chooseCommitterFactory(fileSystem,
        outputPath,
        context.getConfiguration());
    return factory != null ?
      factory.createTaskCommitter(fileSystem, outputPath, context)
      : createFileOutputCommitter(outputPath, context);
  }

  /**
   * Choose a committer from the FS and task configurations. Task Configuration
   * takes priority, allowing execution engines to dynamically change
   * committer on a query-by-query basis.
   * @param fileSystem FS
   * @param outputPath destination path
   * @param taskConf configuration from the task
   * @return An S3A committer if chosen, or "null" for the classic value
   * @throws PathCommitException on a failure to identify the committer
   */
  private AbstractS3ACommitterFactory chooseCommitterFactory(
      S3AFileSystem fileSystem,
      Path outputPath,
      Configuration taskConf) throws PathCommitException {
    AbstractS3ACommitterFactory factory;

    // the FS conf will have had its per-bucket values resolved, unlike
    // job/task configurations.
    Configuration fsConf = fileSystem.getConf();

    String name = fsConf.getTrimmed(FS_S3A_COMMITTER_NAME, COMMITTER_NAME_FILE);
    name = taskConf.getTrimmed(FS_S3A_COMMITTER_NAME, name);
    switch (name) {
    case COMMITTER_NAME_FILE:
      factory = null;
      break;
    case COMMITTER_NAME_DIRECTORY:
      factory = new DirectoryStagingCommitterFactory();
      break;
    case COMMITTER_NAME_PARTITIONED:
      factory = new PartitionedStagingCommitterFactory();
      break;
    case COMMITTER_NAME_MAGIC:
      factory = new MagicS3GuardCommitterFactory();
      break;
    case InternalCommitterConstants.COMMITTER_NAME_STAGING:
      factory = new StagingCommitterFactory();
      break;
    default:
      throw new PathCommitException(outputPath,
          "Unknown committer: \"" + name + "\"");
    }
    return factory;
  }
}

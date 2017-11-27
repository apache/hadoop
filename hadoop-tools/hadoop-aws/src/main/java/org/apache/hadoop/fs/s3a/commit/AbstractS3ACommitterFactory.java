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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;

/**
 * Dynamically create the output committer based on subclass type and settings.
 */
public abstract class AbstractS3ACommitterFactory
    extends PathOutputCommitterFactory {
  public static final Logger LOG = LoggerFactory.getLogger(
      AbstractS3ACommitterFactory.class);

  @Override
  public PathOutputCommitter createOutputCommitter(Path outputPath,
      TaskAttemptContext context) throws IOException {
    FileSystem fs = getDestinationFileSystem(outputPath, context);
    PathOutputCommitter outputCommitter;
    if (fs instanceof S3AFileSystem) {
      outputCommitter = createTaskCommitter((S3AFileSystem)fs,
          outputPath, context);
    } else {
      throw new PathCommitException(outputPath,
          "Filesystem not supported by this committer");
    }
    LOG.info("Using Commmitter {} for {}",
        outputCommitter,
        outputPath);
    return outputCommitter;
  }

  /**
   * Get the destination filesystem, returning null if there is none.
   * Code using this must explicitly or implicitly look for a null value
   * in the response.
   * @param outputPath output path
   * @param context job/task context
   * @return the destination filesystem, if it can be determined
   * @throws IOException if the FS cannot be instantiated
   */
  protected FileSystem getDestinationFileSystem(Path outputPath,
      JobContext context)
      throws IOException {
    return outputPath != null ?
          FileSystem.get(outputPath.toUri(), context.getConfiguration())
          : null;
  }

  /**
   * Implementation point: create a task committer for a specific filesystem.
   * @param fileSystem destination FS.
   * @param outputPath final output path for work
   * @param context task context
   * @return a committer
   * @throws IOException any problem, including the FS not supporting
   * the desired committer
   */
  public abstract PathOutputCommitter createTaskCommitter(
      S3AFileSystem fileSystem,
      Path outputPath,
      TaskAttemptContext context) throws IOException;
}

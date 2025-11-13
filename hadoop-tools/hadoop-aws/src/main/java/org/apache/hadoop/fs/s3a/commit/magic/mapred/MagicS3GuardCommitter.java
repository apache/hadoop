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

package org.apache.hadoop.fs.s3a.commit.magic.mapred;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;

import java.io.IOException;

/**
 * This is a mapred wrapper for the MagicS3GuardCommitter.
 */
public class MagicS3GuardCommitter extends OutputCommitter {

  private org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter committer = null;

  private org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter getWrapped(
          JobContext context) throws IOException {
    if (committer == null) {
      committer = (org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter)
        PathOutputCommitterFactory.createCommitter(
          new Path(context.getConfiguration().get("mapred.output.dir")),
          context);
    }
    return committer;
  }

  private org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter getWrapped(
          TaskAttemptContext context) throws IOException {
    if (committer == null) {
      committer = (org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter)
          MagicS3GuardCommitterFactory.createCommitter(
          new Path(context.getConfiguration().get("mapred.output.dir")),
          context);
    }
    return committer;
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    getWrapped(context).setupJob(context);
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    getWrapped(context).setupTask(context);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    return getWrapped(context).needsTaskCommit(context);
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    getWrapped(context).commitTask(context);
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    getWrapped(context).abortTask(context);
  }

  @Override
  public void cleanupJob(JobContext context) throws IOException {
    getWrapped(context).cleanupJob(context);
  }

  @Override
  public void commitJob(JobContext context) throws IOException {
    getWrapped(context).commitJob(context);
  }

  public final Path getWorkPath() {
    return committer.getWorkPath();
  }

  public final Path getOutputPath() {
    return committer.getOutputPath();
  }
}

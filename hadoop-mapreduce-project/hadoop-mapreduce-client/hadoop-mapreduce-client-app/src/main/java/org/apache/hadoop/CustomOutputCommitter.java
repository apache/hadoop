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

package org.apache.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;

public class CustomOutputCommitter extends OutputCommitter {

  public static final String JOB_SETUP_FILE_NAME = "_job_setup";
  public static final String JOB_COMMIT_FILE_NAME = "_job_commit";
  public static final String JOB_ABORT_FILE_NAME = "_job_abort";
  public static final String TASK_SETUP_FILE_NAME = "_task_setup";
  public static final String TASK_ABORT_FILE_NAME = "_task_abort";
  public static final String TASK_COMMIT_FILE_NAME = "_task_commit";

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    writeFile(jobContext.getJobConf(), JOB_SETUP_FILE_NAME);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    super.commitJob(jobContext);
    writeFile(jobContext.getJobConf(), JOB_COMMIT_FILE_NAME);
  }

  @Override
  public void abortJob(JobContext jobContext, int status) 
  throws IOException {
    super.abortJob(jobContext, status);
    writeFile(jobContext.getJobConf(), JOB_ABORT_FILE_NAME);
  }
  
  @Override
  public void setupTask(TaskAttemptContext taskContext) throws IOException {
    writeFile(taskContext.getJobConf(), TASK_SETUP_FILE_NAME);
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext)
      throws IOException {
    return true;
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) throws IOException {
    writeFile(taskContext.getJobConf(), TASK_COMMIT_FILE_NAME);
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) throws IOException {
    writeFile(taskContext.getJobConf(), TASK_ABORT_FILE_NAME);
  }

  private void writeFile(JobConf conf , String filename) throws IOException {
    System.out.println("writing file ----" + filename);
    Path outputPath = FileOutputFormat.getOutputPath(conf);
    FileSystem fs = outputPath.getFileSystem(conf);
    fs.create(new Path(outputPath, filename)).close();
  }
}

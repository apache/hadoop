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
package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestTaskCommit extends HadoopTestCase {

  static class CommitterWithCommitFail extends FileOutputCommitter {
    public void commitTask(TaskAttemptContext context) throws IOException {
      Path taskOutputPath = getTempTaskOutputPath(context);
      TaskAttemptID attemptId = context.getTaskAttemptID();
      JobConf job = context.getJobConf();
      if (taskOutputPath != null) {
        FileSystem fs = taskOutputPath.getFileSystem(job);
        if (fs.exists(taskOutputPath)) {
          throw new IOException();
        }
      }
    }
  }

  public TestTaskCommit() throws IOException {
    super(LOCAL_MR, LOCAL_FS, 1, 1);
  }
  
  public void testCommitFail() throws IOException {
    Path rootDir = 
      new Path(System.getProperty("test.build.data",  "/tmp"), "test");
    final Path inDir = new Path(rootDir, "input");
    final Path outDir = new Path(rootDir, "output");
    JobConf jobConf = createJobConf();
    jobConf.setMaxMapAttempts(1);
    jobConf.setOutputCommitter(CommitterWithCommitFail.class);
    RunningJob rJob = UtilsForTests.runJob(jobConf, inDir, outDir, 1, 0);
    rJob.waitForCompletion();
    assertEquals(JobStatus.FAILED, rJob.getJobState());
  }

  public static void main(String[] argv) throws Exception {
    TestTaskCommit td = new TestTaskCommit();
    td.testCommitFail();
  }
}

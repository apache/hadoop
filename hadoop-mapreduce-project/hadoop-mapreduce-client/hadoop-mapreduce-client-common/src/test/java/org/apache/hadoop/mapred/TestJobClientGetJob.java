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

import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TestJobClientGetJob {
  
  private static Path TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"));
  
  private Path createTempFile(String filename, String contents)
      throws IOException {
    Path path = new Path(TEST_ROOT_DIR, filename);
    Configuration conf = new Configuration();
    FSDataOutputStream os = FileSystem.getLocal(conf).create(path);
    os.writeBytes(contents);
    os.close();
    return path;
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testGetRunningJobFromJobClient() throws Exception {
    JobConf conf = new JobConf();
    conf.set("mapreduce.framework.name", "local");
    FileInputFormat.addInputPath(conf, createTempFile("in", "hello"));
    Path outputDir = new Path(TEST_ROOT_DIR, getClass().getSimpleName());
    outputDir.getFileSystem(conf).delete(outputDir, true);
    FileOutputFormat.setOutputPath(conf, outputDir);
    JobClient jc = new JobClient(conf);
    RunningJob runningJob = jc.submitJob(conf);
    assertNotNull(runningJob, "Running job");
    // Check that the running job can be retrieved by ID
    RunningJob newRunningJob = jc.getJob(runningJob.getID());
    assertNotNull(newRunningJob, "New running job");
  }

}

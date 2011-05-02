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

package org.apache.hadoop.streaming;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.File;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.TaskReport;

/**
 * Tests for the ability of a streaming task to set the status
 * by writing "reporter:status:" lines to stderr. Uses MiniMR
 * since the local jobtracker doesn't track status.
 */
public class TestStreamingStatus extends TestCase {
  private static String TEST_ROOT_DIR =
    new File(System.getProperty("test.build.data","/tmp"))
    .toURI().toString().replace(' ', '+');
  protected String INPUT_FILE = TEST_ROOT_DIR + "/input.txt";
  protected String OUTPUT_DIR = TEST_ROOT_DIR + "/out";
  protected String input = "roses.are.red\nviolets.are.blue\nbunnies.are.pink\n";
  protected String map = StreamUtil.makeJavaCommand(StderrApp.class, new String[]{"3", "0", "0", "true"});

  protected String[] genArgs(int jobtrackerPort) {
    return new String[] {
      "-input", INPUT_FILE,
      "-output", OUTPUT_DIR,
      "-mapper", map,
      "-jobconf", "mapred.map.tasks=1",
      "-jobconf", "mapred.reduce.tasks=0",      
      "-jobconf", "keep.failed.task.files=true",
      "-jobconf", "stream.tmpdir="+System.getProperty("test.build.data","/tmp"),
      "-jobconf", "mapred.job.tracker=localhost:"+jobtrackerPort,
      "-jobconf", "fs.default.name=file:///"
    };
  }
  
  public void makeInput(FileSystem fs) throws IOException {
    Path inFile = new Path(INPUT_FILE);
    DataOutputStream file = fs.create(inFile);
    file.writeBytes(input);
    file.close();
  }

  public void clean(FileSystem fs) {
    try {
      Path outDir = new Path(OUTPUT_DIR);
      fs.delete(outDir, true);
    } catch (Exception e) {}
    try {
      Path inFile = new Path(INPUT_FILE);    
      fs.delete(inFile, false);
    } catch (Exception e) {}
  }
  
  public void testStreamingStatus() throws Exception {
    MiniMRCluster mr = null;
    FileSystem fs = null;
    try {
      mr = new MiniMRCluster(1, "file:///", 3);

      Path inFile = new Path(INPUT_FILE);
      fs = inFile.getFileSystem(mr.createJobConf());
      clean(fs);
      makeInput(fs);
      
      StreamJob job = new StreamJob();
      int failed = job.run(genArgs(mr.getJobTrackerPort()));
      assertEquals(0, failed);

      TaskReport[] reports = job.jc_.getMapTaskReports(job.jobId_);
      assertEquals(1, reports.length);
      assertEquals("starting echo", reports[0].getState());
    } finally {
      if (fs != null) { clean(fs); }
      if (mr != null) { mr.shutdown(); }
    }
  }
}

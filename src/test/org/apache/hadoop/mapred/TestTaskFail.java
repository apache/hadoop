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

import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityReducer;

public class TestTaskFail extends TestCase {
  public static class MapperClass extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {
    String taskid;
    public void configure(JobConf job) {
      taskid = job.get("mapred.task.id");
    }
    public void map (LongWritable key, Text value, 
                     OutputCollector<Text, IntWritable> output, 
                     Reporter reporter) throws IOException {
      if (taskid.endsWith("_0")) {
        throw new IOException();
      } else if (taskid.endsWith("_1")) {
        System.exit(-1);
      } 
    }
  }

  public RunningJob launchJob(JobConf conf,
                              Path inDir,
                              Path outDir,
                              String input) 
  throws IOException {
    // set up the input file system and write input text.
    FileSystem inFs = inDir.getFileSystem(conf);
    FileSystem outFs = outDir.getFileSystem(conf);
    outFs.delete(outDir, true);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      // write input into input file
      DataOutputStream file = inFs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }

    // configure the mapred Job
    conf.setMapperClass(MapperClass.class);        
    conf.setReducerClass(IdentityReducer.class);
    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    String TEST_ROOT_DIR = new Path(System.getProperty("test.build.data",
                                    "/tmp")).toString().replace(' ', '+');
    conf.set("test.build.data", TEST_ROOT_DIR);
    // return the RunningJob handle.
    return new JobClient(conf).submitJob(conf);
  }
		  
  public void testWithDFS() throws IOException {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    try {
      final int taskTrackers = 4;

      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 4, true, null);
      fileSys = dfs.getFileSystem();
      mr = new MiniMRCluster(taskTrackers, fileSys.getUri().toString(), 1);
      JobConf jobConf = mr.createJobConf();
      final Path inDir = new Path("./input");
      final Path outDir = new Path("./output");
      String input = "The quick brown fox\nhas many silly\nred fox sox\n";
      RunningJob job = null;

      job = launchJob(jobConf, inDir, outDir, input);
      // wait for the job to finish.
      while (!job.isComplete());
      assertEquals(JobStatus.SUCCEEDED, job.getJobState());
      
      JobID jobId = job.getID();
      // construct the task id of first map task
      TaskAttemptID attemptId = 
        new TaskAttemptID(new TaskID(jobId, true, 0), 0);
      TaskInProgress tip = mr.getJobTrackerRunner().getJobTracker().
                              getTip(attemptId.getTaskID());
      // this should not be cleanup attempt since the first attempt 
      // fails with an exception
      assertTrue(!tip.isCleanupAttempt(attemptId));
      TaskStatus ts = 
        mr.getJobTrackerRunner().getJobTracker().getTaskStatus(attemptId);
      assertTrue(ts != null);
      assertEquals(TaskStatus.State.FAILED, ts.getRunState());
      
      attemptId =  new TaskAttemptID(new TaskID(jobId, true, 0), 1);
      // this should be cleanup attempt since the second attempt fails
      // with System.exit
      assertTrue(tip.isCleanupAttempt(attemptId));
      ts = mr.getJobTrackerRunner().getJobTracker().getTaskStatus(attemptId);
      assertTrue(ts != null);
      assertEquals(TaskStatus.State.FAILED, ts.getRunState());

    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
  }

  public static void main(String[] argv) throws Exception {
    TestTaskFail td = new TestTaskFail();
    td.testWithDFS();
  }
}

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
import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A JUnit test to test Kill Job & Fail Job functionality with local file
 * system.
 */
public class TestJobKillAndFail extends TestCase {

  private static String TEST_ROOT_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")).toURI().toString().replace(' ', '+');

  static JobID runJobFail(JobConf conf) throws IOException {

    conf.setJobName("testjobfail");
    conf.setMapperClass(FailMapper.class);

    RunningJob job = runJob(conf);
    while (!job.isComplete()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        break;
      }
    }
    // Checking that the Job got failed
    assertEquals(job.getJobState(), JobStatus.FAILED);
    
    return job.getID();
  }

  static JobID runJobKill(JobConf conf) throws IOException {

    conf.setJobName("testjobkill");
    conf.setMapperClass(KillMapper.class);

    RunningJob job = runJob(conf);
    while (job.getJobState() != JobStatus.RUNNING) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        break;
      }
    }
    job.killJob();
    while (job.cleanupProgress() == 0.0f) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException ie) {
        break;
      }
    }
    // Checking that the Job got killed
    assertTrue(job.isComplete());
    assertEquals(job.getJobState(), JobStatus.KILLED);
    
    return job.getID();
  }

  static RunningJob runJob(JobConf conf) throws IOException {

    final Path inDir = new Path(TEST_ROOT_DIR + "/failkilljob/input");
    final Path outDir = new Path(TEST_ROOT_DIR + "/failkilljob/output");

    // run the dummy sleep map
    FileSystem fs = FileSystem.get(conf);
    fs.delete(outDir, true);
    if (!fs.exists(inDir)) {
      fs.mkdirs(inDir);
    }
    String input = "The quick brown fox\n" + "has many silly\n"
        + "red fox sox\n";
    DataOutputStream file = fs.create(new Path(inDir, "part-0"));
    file.writeBytes(input);
    file.close();

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(0);

    JobClient jobClient = new JobClient(conf);
    RunningJob job = jobClient.submitJob(conf);

    return job;

  }

  public void testJobFailAndKill() throws IOException {
    MiniMRCluster mr = null;
    try {
      mr = new MiniMRCluster(2, "file:///", 3);

      // run the TCs
      JobConf conf = mr.createJobConf();
      runJobFail(conf);
      runJobKill(conf);
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
    }
  }

  static class FailMapper extends MapReduceBase implements
      Mapper<WritableComparable, Writable, WritableComparable, Writable> {

    public void map(WritableComparable key, Writable value,
        OutputCollector<WritableComparable, Writable> out, Reporter reporter)
        throws IOException {

      throw new RuntimeException("failing map");
    }
  }

  static class KillMapper extends MapReduceBase implements
      Mapper<WritableComparable, Writable, WritableComparable, Writable> {

    public void map(WritableComparable key, Writable value,
        OutputCollector<WritableComparable, Writable> out, Reporter reporter)
        throws IOException {

      try {
        Thread.sleep(100000);
      } catch (InterruptedException e) {
        // Do nothing
      }
    }
  }
}

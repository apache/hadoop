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
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Test a java-based mapred job with LinuxTaskController running the jobs as a
 * user different from the user running the cluster. See
 * {@link ClusterWithLinuxTaskController}
 */
public class TestJobExecutionAsDifferentUser extends
    ClusterWithLinuxTaskController {

  public void testJobExecution()
      throws Exception {
    if (!shouldRun()) {
      return;
    }
    startCluster();
    submitWordCount(getClusterConf());
  }
  
  private void submitWordCount(JobConf clientConf) throws IOException {
    Path inDir = new Path("testing/wc/input");
    Path outDir = new Path("testing/wc/output");
    JobConf conf = new JobConf(clientConf);
    FileSystem fs = FileSystem.get(conf);
    fs.delete(outDir, true);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }

    DataOutputStream file = fs.create(new Path(inDir, "part-0"));
    file.writeBytes("a b c d e f g h");
    file.close();

    conf.setJobName("wordcount");
    conf.setInputFormat(TextInputFormat.class);

    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);

    conf.setMapperClass(WordCount.MapClass.class);
    conf.setCombinerClass(WordCount.Reduce.class);
    conf.setReducerClass(WordCount.Reduce.class);

    FileInputFormat.setInputPaths(conf, inDir);
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.setNumMapTasks(1);
    conf.setNumReduceTasks(1);
    RunningJob rj = JobClient.runJob(conf);
    assertTrue("Job Failed", rj.isSuccessful());
    assertOwnerShip(outDir);
  }
  
  public void testEnvironment() throws IOException {
    if (!shouldRun()) {
      return;
    }
    startCluster();
    TestMiniMRChildTask childTask = new TestMiniMRChildTask();
    Path inDir = new Path("input1");
    Path outDir = new Path("output1");
    try {
      childTask.runTestTaskEnv(getClusterConf(), inDir, outDir);
    } catch (IOException e) {
      fail("IOException thrown while running enviroment test."
          + e.getMessage());
    } finally {
      FileSystem outFs = outDir.getFileSystem(getClusterConf());
      if (outFs.exists(outDir)) {
        assertOwnerShip(outDir);
        outFs.delete(outDir, true);
      } else {
        fail("Output directory does not exist" + outDir.toString());
      }
    }
  }
}

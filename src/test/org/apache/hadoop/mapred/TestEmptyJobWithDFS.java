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

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;

/**
 * A JUnit test to test Map-Reduce empty jobs Mini-DFS.
 */
public class TestEmptyJobWithDFS extends TestCase {
  
  /**
   * Simple method running a MapReduce job with no input data. Used
   * to test that such a job is successful.
   * @param fileSys
   * @param jobTracker
   * @param conf
   * @param numMaps
   * @param numReduces
   * @return true if the MR job is successful, otherwise false
   * @throws IOException
   */
  public static boolean launchEmptyJob(String fileSys,
                                      String jobTracker,
                                      JobConf conf,
                                      int numMaps,
                                      int numReduces) throws IOException {
      // create an empty input dir
      final Path inDir = new Path("/testing/empty/input");
      final Path outDir = new Path("/testing/empty/output");
      FileSystem fs = FileSystem.getNamed(fileSys, conf);
      fs.delete(outDir);
      if (!fs.mkdirs(inDir)) {
          return false;
      }

      // use WordCount example
      conf.set("fs.default.name", fileSys);
      conf.set("mapred.job.tracker", jobTracker);
      conf.setJobName("empty");
      // use an InputFormat which returns no split
      conf.setInputFormat(EmptyInputFormat.class);
      conf.setOutputKeyClass(Text.class);
      conf.setOutputValueClass(IntWritable.class);
      conf.setMapperClass(IdentityMapper.class);        
      conf.setReducerClass(IdentityReducer.class);
      conf.setInputPath(inDir);
      conf.setOutputPath(outDir);
      conf.setNumMapTasks(numMaps);
      conf.setNumReduceTasks(numReduces);
      
      // run job and wait for completion
      JobClient jc = new JobClient(conf);
      RunningJob runningJob = jc.submitJob(conf);
      while (true) {
          try {
              Thread.sleep(1000);
          } catch (InterruptedException e) {}
          if (runningJob.isComplete()) {
              break;
          }
      }
      // return job result
      return (runningJob.isSuccessful());
  }
  
  /**
   * Test that a job with no input data (and thus with no input split and
   * no map task to execute) is successful.
   * @throws IOException
   */
  public void testEmptyJobWithDFS() throws IOException {
      String namenode = null;
      MiniDFSCluster dfs = null;
      MiniMRCluster mr = null;
      FileSystem fileSys = null;
      try {
          final int taskTrackers = 4;
          final int jobTrackerPort = 60050;
          Configuration conf = new Configuration();
          dfs = new MiniDFSCluster(65315, conf, true);
          fileSys = dfs.getFileSystem();
          namenode = fileSys.getName();
          mr = new MiniMRCluster(jobTrackerPort, 50060, taskTrackers, 
                                 namenode, true, 2);
          final String jobTrackerName = "localhost:" + mr.getJobTrackerPort();
          JobConf jobConf = new JobConf();
          boolean result;
          result = launchEmptyJob(namenode, jobTrackerName, jobConf, 
                                   3, 1);
          assertTrue(result);
          
      } finally {
          if (fileSys != null) { fileSys.close(); }
          if (dfs != null) { dfs.shutdown(); }
          if (mr != null) { mr.shutdown(); }
      }
  }
  
}

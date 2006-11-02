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

import java.io.*;
import java.util.*;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * A JUnit test to test Mini Map-Reduce Cluster with multiple directories
 * and check for correct classpath
 */
public class TestMiniMRClasspath extends TestCase {
  
  
   static String launchWordCount(String fileSys,
                                       String jobTracker,
                                       JobConf conf,
                                       String input,
                                       int numMaps,
                                       int numReduces) throws IOException {
    final Path inDir = new Path("/testing/wc/input");
    final Path outDir = new Path("/testing/wc/output");
    FileSystem fs = FileSystem.getNamed(fileSys, conf);
    fs.delete(outDir);
    if (!fs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      DataOutputStream file = fs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }
    conf.set("fs.default.name", fileSys);
    conf.set("mapred.job.tracker", jobTracker);
    conf.setJobName("wordcount");
    conf.setInputFormat(TextInputFormat.class);
    
    // the keys are words (strings)
    conf.setOutputKeyClass(Text.class);
    // the values are counts (ints)
    conf.setOutputValueClass(IntWritable.class);
    
    conf.set("mapred.mapper.class", "ClassWordCount$MapClass");        
    conf.set("mapred.combine.class", "ClassWordCount$Reduce");
    conf.set("mapred.reducer.class", "ClassWordCount$Reduce");
    conf.setInputPath(inDir);
    conf.setOutputPath(outDir);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReduces);
    //pass a job.jar already included in the hadoop build
    conf.setJar("build/test/testjar/testjob.jar");
    JobClient.runJob(conf);
    StringBuffer result = new StringBuffer();
    {
      Path[] fileList = fs.listPaths(outDir);
      for(int i=0; i < fileList.length; ++i) {
        BufferedReader file = 
          new BufferedReader(new InputStreamReader(fs.open(fileList[i])));
        String line = file.readLine();
        while (line != null) {
          result.append(line);
          result.append("\n");
          line = file.readLine();
        }
        file.close();
      }
    }
    return result.toString();
  }
  
  public void testClassPath() throws IOException {
      String namenode = null;
      MiniDFSCluster dfs = null;
      MiniMRCluster mr = null;
      FileSystem fileSys = null;
      try {
          final int taskTrackers = 4;
          final int jobTrackerPort = 60050;

          Configuration conf = new Configuration();
          dfs = new MiniDFSCluster(65314, conf, true);
          fileSys = dfs.getFileSystem();
          namenode = fileSys.getName();
          mr = new MiniMRCluster(jobTrackerPort, 50060, taskTrackers, 
                                 namenode, true, 3);
          JobConf jobConf = new JobConf();
          String result;
          final String jobTrackerName = "localhost:" + mr.getJobTrackerPort();
          result = launchWordCount(namenode, jobTrackerName, jobConf, 
                                   "The quick brown fox\nhas many silly\n" + 
                                   "red fox sox\n",
                                   3, 1);
          assertEquals("The\t1\nbrown\t1\nfox\t2\nhas\t1\nmany\t1\n" +
                       "quick\t1\nred\t1\nsilly\t1\nsox\t1\n", result);
          
      } finally {
          if (fileSys != null) { fileSys.close(); }
          if (dfs != null) { dfs.shutdown(); }
          if (mr != null) { mr.shutdown();
          }
      }
  }
  
}

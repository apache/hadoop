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
import org.apache.hadoop.examples.WordCount;

/**
 * A JUnit test to test Mini Map-Reduce Cluster with Mini-DFS.
 *
 * @author Milind Bhandarkar
 */
public class TestMiniMRWithDFS extends TestCase {
  
    static final int NUM_MAPS = 10;
    static final int NUM_SAMPLES = 100000;
  
  public static String launchWordCount(String fileSys,
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
    
    conf.setMapperClass(WordCount.MapClass.class);        
    conf.setCombinerClass(WordCount.Reduce.class);
    conf.setReducerClass(WordCount.Reduce.class);
    conf.setInputPath(inDir);
    conf.setOutputPath(outDir);
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReduces);
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
  
  /**
   * Make sure that there are exactly the directories that we expect to find.
   * @param mr the map-reduce cluster
   * @param taskDirs the task ids that should be present
   */
  private static void checkTaskDirectories(MiniMRCluster mr,
                                           String[] jobIds,
                                           String[] taskDirs) {
    mr.waitUntilIdle();
    int trackers = mr.getNumTaskTrackers();
    List neededDirs = new ArrayList(Arrays.asList(taskDirs));
    boolean[] found = new boolean[taskDirs.length];
    for(int i=0; i < trackers; ++i) {
      int numNotDel = 0;
      File localDir = new File(mr.getTaskTrackerLocalDir(i));
      File trackerDir = new File(localDir, "taskTracker");
      assertTrue("local dir " + localDir + " does not exist.", 
                   localDir.isDirectory());
      assertTrue("task tracker dir " + trackerDir + " does not exist.", 
                   trackerDir.isDirectory());
      String contents[] = localDir.list();
      String trackerContents[] = trackerDir.list();
      for(int j=0; j < contents.length; ++j) {
        System.out.println("Local " + localDir + ": " + contents[j]);
      }
      for(int j=0; j < trackerContents.length; ++j) {
        System.out.println("Local jobcache " + trackerDir + ": " + trackerContents[j]);
      }
      for(int fileIdx = 0; fileIdx < contents.length; ++fileIdx) {
        String name = contents[fileIdx];
        if (!("taskTracker".equals(contents[fileIdx]))) {
          int idx = neededDirs.indexOf(name);
          assertTrue("Spurious directory " + name + " found in " +
                     localDir, idx != -1);
          assertTrue("Matching output directory not found " + name +
                     " in " + trackerDir, 
                     new File(new File(new File(trackerDir, "jobcache"), jobIds[idx]), name).isDirectory());
          found[idx] = true;
          numNotDel++;
        }  
      }
    }
    for(int i=0; i< found.length; i++) {
      assertTrue("Directory " + taskDirs[i] + " not found", found[i]);
    }
  }
  
  public void testWithDFS() throws IOException {
      String namenode = null;
      MiniDFSCluster dfs = null;
      MiniMRCluster mr = null;
      FileSystem fileSys = null;
      try {
          final int taskTrackers = 4;
          final int jobTrackerPort = 60050;

          Configuration conf = new Configuration();
          dfs = new MiniDFSCluster(65314, conf, 4, true);
          fileSys = dfs.getFileSystem();
          namenode = fileSys.getName();
          mr = new MiniMRCluster(jobTrackerPort, 50060, taskTrackers, 
                                 namenode, true);
          final String jobTrackerName = "localhost:" + mr.getJobTrackerPort();
          double estimate = PiEstimator.launch(NUM_MAPS, NUM_SAMPLES, 
                                               jobTrackerName, namenode);
          double error = Math.abs(Math.PI - estimate);
          assertTrue("Error in PI estimation "+error+" exceeds 0.01", (error < 0.01));
          checkTaskDirectories(mr, new String[]{}, new String[]{});
          
          // Run a word count example
          JobConf jobConf = new JobConf();
          // Keeping tasks that match this pattern
          jobConf.setKeepTaskFilesPattern("task_[0-9]*_m_000001_.*");
          String result;
          result = launchWordCount(namenode, jobTrackerName, jobConf, 
                                   "The quick brown fox\nhas many silly\n" + 
                                   "red fox sox\n",
                                   3, 1);
          assertEquals("The\t1\nbrown\t1\nfox\t2\nhas\t1\nmany\t1\n" +
                       "quick\t1\nred\t1\nsilly\t1\nsox\t1\n", result);
          checkTaskDirectories(mr, new String[]{"job_0002"}, new String[]{"task_0002_m_000001_0"});
          
      } finally {
          if (fileSys != null) { fileSys.close(); }
          if (dfs != null) { dfs.shutdown(); }
          if (mr != null) { mr.shutdown();
          }
      }
  }
  
}

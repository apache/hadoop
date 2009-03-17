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

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * A JUnit test to test Mini Map-Reduce Cluster with Mini-DFS.
 */
public class TestMiniMRWithDFS extends TestCase {
  private static final Log LOG =
    LogFactory.getLog(TestMiniMRWithDFS.class.getName());
  
  static final int NUM_MAPS = 10;
  static final int NUM_SAMPLES = 100000;
  
  public static class TestResult {
    public String output;
    public RunningJob job;
    TestResult(RunningJob job, String output) {
      this.job = job;
      this.output = output;
    }
  }
  public static TestResult launchWordCount(JobConf conf,
                                           Path inDir,
                                           Path outDir,
                                           String input,
                                           int numMaps,
                                           int numReduces) throws IOException {
    FileSystem inFs = inDir.getFileSystem(conf);
    FileSystem outFs = outDir.getFileSystem(conf);
    outFs.delete(outDir, true);
    if (!inFs.mkdirs(inDir)) {
      throw new IOException("Mkdirs failed to create " + inDir.toString());
    }
    {
      DataOutputStream file = inFs.create(new Path(inDir, "part-0"));
      file.writeBytes(input);
      file.close();
    }
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
    conf.setNumMapTasks(numMaps);
    conf.setNumReduceTasks(numReduces);
    RunningJob job = JobClient.runJob(conf);
    return new TestResult(job, readOutput(outDir, conf));
  }

  public static String readOutput(Path outDir, 
                                  JobConf conf) throws IOException {
    FileSystem fs = outDir.getFileSystem(conf);
    StringBuffer result = new StringBuffer();
    {
      
      Path[] fileList = FileUtil.stat2Paths(fs.listStatus(outDir,
                                   new OutputLogFilter()));
      for(int i=0; i < fileList.length; ++i) {
        LOG.info("File list[" + i + "]" + ": "+ fileList[i]);
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
  static void checkTaskDirectories(MiniMRCluster mr,
                                           String[] jobIds,
                                           String[] taskDirs) {
    mr.waitUntilIdle();
    int trackers = mr.getNumTaskTrackers();
    List<String> neededDirs = new ArrayList<String>(Arrays.asList(taskDirs));
    boolean[] found = new boolean[taskDirs.length];
    for(int i=0; i < trackers; ++i) {
      int numNotDel = 0;
      File localDir = new File(mr.getTaskTrackerLocalDir(i));
      LOG.debug("Tracker directory: " + localDir);
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
          LOG.debug("Looking at " + name);
          assertTrue("Spurious directory " + name + " found in " +
                     localDir, false);
        }
      }
      for (int idx = 0; idx < neededDirs.size(); ++idx) {
        String name = neededDirs.get(idx);
        if (new File(new File(new File(trackerDir, "jobcache"),
                              jobIds[idx]), name).isDirectory()) {
          found[idx] = true;
          numNotDel++;
        }  
      }
    }
    for(int i=0; i< found.length; i++) {
      assertTrue("Directory " + taskDirs[i] + " not found", found[i]);
    }
  }

  public static void runPI(MiniMRCluster mr, JobConf jobconf) throws IOException {
    LOG.info("runPI");
    double estimate = org.apache.hadoop.examples.PiEstimator.estimate(
        NUM_MAPS, NUM_SAMPLES, jobconf).doubleValue();
    double error = Math.abs(Math.PI - estimate);
    assertTrue("Error in PI estimation "+error+" exceeds 0.01", (error < 0.01));
    checkTaskDirectories(mr, new String[]{}, new String[]{});
  }

  public static void runWordCount(MiniMRCluster mr, JobConf jobConf) 
  throws IOException {
    LOG.info("runWordCount");
    // Run a word count example
    // Keeping tasks that match this pattern
    String pattern = 
      TaskAttemptID.getTaskAttemptIDsPattern(null, null, true, 1, null);
    jobConf.setKeepTaskFilesPattern(pattern);
    TestResult result;
    final Path inDir = new Path("./wc/input");
    final Path outDir = new Path("./wc/output");
    String input = "The quick brown fox\nhas many silly\nred fox sox\n";
    result = launchWordCount(jobConf, inDir, outDir, input, 3, 1);
    assertEquals("The\t1\nbrown\t1\nfox\t2\nhas\t1\nmany\t1\n" +
                 "quick\t1\nred\t1\nsilly\t1\nsox\t1\n", result.output);
    JobID jobid = result.job.getID();
    TaskAttemptID taskid = new TaskAttemptID(new TaskID(jobid, true, 1),0);
    checkTaskDirectories(mr, new String[]{jobid.toString()}, 
                         new String[]{taskid.toString()});
    // test with maps=0
    jobConf = mr.createJobConf();
    input = "owen is oom";
    result = launchWordCount(jobConf, inDir, outDir, input, 0, 1);
    assertEquals("is\t1\noom\t1\nowen\t1\n", result.output);
    Counters counters = result.job.getCounters();
    long hdfsRead = 
      counters.findCounter(Task.FILESYSTEM_COUNTER_GROUP, 
          Task.getFileSystemCounterNames("hdfs")[0]).getCounter();
    long hdfsWrite = 
      counters.findCounter(Task.FILESYSTEM_COUNTER_GROUP, 
          Task.getFileSystemCounterNames("hdfs")[1]).getCounter();
    assertEquals(result.output.length(), hdfsWrite);
    assertEquals(input.length(), hdfsRead);

    // Run a job with input and output going to localfs even though the 
    // default fs is hdfs.
    {
      FileSystem localfs = FileSystem.getLocal(jobConf);
      String TEST_ROOT_DIR =
        new File(System.getProperty("test.build.data","/tmp"))
        .toString().replace(' ', '+');
      Path localIn = localfs.makeQualified
                        (new Path(TEST_ROOT_DIR + "/local/in"));
      Path localOut = localfs.makeQualified
                        (new Path(TEST_ROOT_DIR + "/local/out"));
      result = launchWordCount(jobConf, localIn, localOut,
                               "all your base belong to us", 1, 1);
      assertEquals("all\t1\nbase\t1\nbelong\t1\nto\t1\nus\t1\nyour\t1\n", 
                   result.output);
      assertTrue("outputs on localfs", localfs.exists(localOut));

    }
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

      runPI(mr, mr.createJobConf());
      runWordCount(mr, mr.createJobConf());
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();
      }
    }
  }
  
  public void testWithDFSWithDefaultPort() throws IOException {
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    FileSystem fileSys = null;
    try {
      final int taskTrackers = 4;

      Configuration conf = new Configuration();
      // start a dfs with the default port number
      dfs = new MiniDFSCluster(
          NameNode.DEFAULT_PORT, conf, 4, true, true, null, null);
      fileSys = dfs.getFileSystem();
      mr = new MiniMRCluster(taskTrackers, fileSys.getUri().toString(), 1);

      JobConf jobConf = mr.createJobConf();
      TestResult result;
      final Path inDir = new Path("./wc/input");
      final Path outDir = new Path("hdfs://" +
          dfs.getNameNode().getNameNodeAddress().getHostName() +
          ":" + NameNode.DEFAULT_PORT +"/./wc/output");
      String input = "The quick brown fox\nhas many silly\nred fox sox\n";
      result = launchWordCount(jobConf, inDir, outDir, input, 3, 1);
      assertEquals("The\t1\nbrown\t1\nfox\t2\nhas\t1\nmany\t1\n" +
                   "quick\t1\nred\t1\nsilly\t1\nsox\t1\n", result.output);
      final Path outDir2 = new Path("hdfs:/test/wc/output2");
      jobConf.set("fs.default.name", "hdfs://localhost:" + NameNode.DEFAULT_PORT);
      result = launchWordCount(jobConf, inDir, outDir2, input, 3, 1);
      assertEquals("The\t1\nbrown\t1\nfox\t2\nhas\t1\nmany\t1\n" +
                   "quick\t1\nred\t1\nsilly\t1\nsox\t1\n", result.output);
    } catch (java.net.BindException be) {
      LOG.info("Skip the test this time because can not start namenode on port "
          + NameNode.DEFAULT_PORT, be);
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown();
      }
    }
  }
}

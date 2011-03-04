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
import java.util.ArrayList;
import java.util.List;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

/**
 * Validates removal of user-created-files(and set non-writable permissions) in
 * tasks under taskWorkDir by TT with LinuxTaskController.
 */
public class TestChildTaskDirs extends ClusterWithLinuxTaskController {
  private static final Log LOG = LogFactory.getLog(TestChildTaskDirs.class);
  private static final File TEST_DIR = 
    new File(System.getProperty("test.build.data", "/tmp"), "child-dirs");
  private static final String MY_DIR = "my-test-dir";
  private static final String MY_FILE = "my-test-file";
  private static final LocalDirAllocator LOCAL_DIR_ALLOC = 
    new LocalDirAllocator("mapred.local.dir");

  public static Test suite() {
    TestSetup setup = 
      new TestSetup(new TestSuite(TestChildTaskDirs.class)) {
      protected void setUp() throws Exception {
        TEST_DIR.mkdirs();
      }
      protected void tearDown() throws Exception {
        FileUtil.fullyDelete(TEST_DIR);
      }
    };
    return setup;
  }

  class InlineCleanupQueue extends CleanupQueue {
    List<String> stalePaths = new ArrayList<String>();

    public InlineCleanupQueue() {
      // do nothing
    }

    @Override
    public void addToQueue(PathDeletionContext... contexts) {
      // delete paths in-line
      for (PathDeletionContext context : contexts) {
        try {
          if (!deletePath(context)) {
            LOG.warn("Stale path " + context.fullPath);
            stalePaths.add(context.fullPath);
          }
        } catch (IOException e) {
          LOG.warn("Caught exception while deleting path "
              + context.fullPath);
          LOG.info(StringUtils.stringifyException(e));
          stalePaths.add(context.fullPath);
        }
      }
    }
  }

  // Mapper that creates dirs
  // job-id/
  //   -attempt-id/
  //      -work/
  //         -my-test-dir(555)
  //            -my-test-file(555)
  static class CreateDir extends MapReduceBase implements
      Mapper<WritableComparable, Writable, WritableComparable, Writable> {

    File taskWorkDir = null;
    public void map(WritableComparable key, Writable value,
        OutputCollector<WritableComparable, Writable> out, Reporter reporter)
        throws IOException {
      File subDir = new File(taskWorkDir, MY_DIR);
      LOG.info("Child folder : " + subDir);
      subDir.mkdirs();
      File newFile = new File(subDir, MY_FILE);
      LOG.info("Child file : " + newFile);
      newFile.createNewFile();

      // Set the permissions of my-test-dir and my-test-dir/my-test-file to 555
      try {
        FileUtil.chmod(subDir.getAbsolutePath(), "a=rx", true);
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
    
    @Override
    public void configure(JobConf conf) {
      String jobId = conf.get("mapred.job.id");
      String taskId = conf.get("mapred.task.id");
      String taskDir = TaskTracker.getLocalTaskDir(jobId, taskId);
      try {
        Path taskDirPath = 
          LOCAL_DIR_ALLOC.getLocalPathForWrite(taskDir, conf);
        taskWorkDir = new File(taskDirPath.toString(), "work");
        LOG.info("Task work-dir : " + taskWorkDir.toString());
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  public void testChildDirCleanup() throws Exception {
    LOG.info("Testing if the dirs created by the child process is cleaned up properly");

    if (!shouldRun()) {
      return;
    }

    // start the cluster
    startCluster();

    // make sure that only one tracker is configured
    if (mrCluster.getNumTaskTrackers() != 1) {
      throw new Exception("Cluster started with " 
        + mrCluster.getNumTaskTrackers() + " instead of 1");
    }
    
    // configure a job
    JobConf jConf = getClusterConf();
    jConf.setJobName("Mkdir job");
    jConf.setMapperClass(CreateDir.class);
    jConf.setNumMapTasks(1);
    jConf.setNumReduceTasks(0);

    FileSystem fs = FileSystem.get(jConf);
    Path inDir = new Path("in");
    Path outDir = new Path("out");
    if (fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    if (!fs.exists(inDir)) {
      fs.mkdirs(inDir);
    }
    String input = "The quick brown fox";
    DataOutputStream file = fs.create(new Path(inDir, "part-0"));
    file.writeBytes(input);
    file.close();

    jConf.setInputFormat(TextInputFormat.class);
    jConf.setOutputKeyClass(LongWritable.class);
    jConf.setOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(jConf, inDir);
    FileOutputFormat.setOutputPath(jConf, outDir);

    // set inline cleanup queue in TT
    mrCluster.getTaskTrackerRunner(0).getTaskTracker().directoryCleanupThread =
      new InlineCleanupQueue();

    JobClient jobClient = new JobClient(jConf);
    RunningJob job = jobClient.submitJob(jConf);

    JobID id = job.getID();
    
    // wait for the job to finish
    job.waitForCompletion();
    
    JobInProgress jip = 
      mrCluster.getJobTrackerRunner().getJobTracker().getJob(id);
    String attemptId = 
      jip.getMapTasks()[0].getTaskStatuses()[0].getTaskID().toString();
    
    String taskTrackerLocalDir = 
      mrCluster.getTaskTrackerRunner(0).getLocalDir();
    
    String taskDir = TaskTracker.getLocalTaskDir(id.toString(), attemptId);
    Path taskDirPath = new Path(taskTrackerLocalDir, taskDir);
    LOG.info("Checking task dir " + taskDirPath);
    FileSystem localFS = FileSystem.getLocal(jConf);
    assertFalse("task dir still exists", localFS.exists(taskDirPath));
  }
}

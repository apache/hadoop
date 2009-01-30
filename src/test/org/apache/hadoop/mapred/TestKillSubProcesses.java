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
import java.util.Random;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ProcessTree;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A JUnit test to test Kill Job that has tasks with children and checks if the
 * children(subprocesses of java task) are also killed when a task is killed.
 */
public class TestKillSubProcesses extends TestCase {

  private static volatile Log LOG = LogFactory
            .getLog(TestKillSubProcesses.class);

  private static String TEST_ROOT_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")).toURI().toString().replace(' ', '+');

  static JobClient jobClient = null;

  static MiniMRCluster mr = null;
  static Path scriptDir = new Path(TEST_ROOT_DIR + "/killjob");

  // number of levels in the subtree of subprocesses of map task
  static int numLevelsOfSubProcesses = 6;

  /**
   * Runs a job, kills the job and verifies if the map task and its
   * subprocesses are also killed properly or not.
   */
  static JobID runJobKill(JobTracker jt, JobConf conf) throws IOException {

    conf.setJobName("testkillsubprocesses");
    conf.setMapperClass(KillMapperWithChild.class);

    RunningJob job = runJob(conf);
    while (job.getJobState() != JobStatus.RUNNING) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        LOG.warn("sleep is interrupted:" + ie);
        break;
      }
    }
    String pid = null;
    String scriptDirName = scriptDir.toString().substring(5);

    // get the taskAttemptID of the map task and use it to get the pid
    // of map task from pid file
    TaskReport[] mapReports = jobClient.getMapTaskReports(job.getID());

    JobInProgress jip = jt.getJob(job.getID());
    for(TaskReport tr : mapReports) {
      TaskInProgress tip = jip.getTaskInProgress(tr.getTaskID());
      assertTrue(tip.isRunning());

      // for this tip, get active tasks of all attempts
      while(tip.getActiveTasks().size() == 0) {
        //wait till the activeTasks Tree is built
        try {
          Thread.sleep(500);
        } catch (InterruptedException ie) {
          LOG.warn("sleep is interrupted:" + ie);
          break;
        }
      }

      for (Iterator<TaskAttemptID> it = 
        tip.getActiveTasks().keySet().iterator(); it.hasNext();) {
        TaskAttemptID id = it.next();
        LOG.info("taskAttemptID of map task is " + id);

        String localDir = mr.getTaskTrackerLocalDir(0); // TT with index 0
        LOG.info("localDir is " + localDir);
        JobConf confForThisTask = new JobConf(conf);
        confForThisTask.set("mapred.local.dir", localDir);//set the localDir

        Path pidFilePath = TaskTracker.getPidFilePath(id, confForThisTask);
        while (pidFilePath == null) {
          //wait till the pid file is created
          try {
            Thread.sleep(500);
          } catch (InterruptedException ie) {
            LOG.warn("sleep is interrupted:" + ie);
            break;
          }
          pidFilePath = TaskTracker.getPidFilePath(id, confForThisTask);
        }

        pid = ProcessTree.getPidFromPidFile(pidFilePath.toString());
        LOG.info("pid of map task is " + pid);

        // Checking if the map task is alive
        assertTrue(ProcessTree.isAlive(pid));
        LOG.info("The map task is alive before killJob, as expected.");
      }
    }

    // Checking if the descendant processes of map task are alive
    if(ProcessTree.isSetsidAvailable) {
      String childPid = ProcessTree.getPidFromPidFile(
                               scriptDirName + "/childPidFile" + 0);
      while(childPid == null) {
        try {
          Thread.sleep(500);
        } catch (InterruptedException ie) {
          LOG.warn("sleep is interrupted:" + ie);
          break;
        }
        childPid = ProcessTree.getPidFromPidFile(
                               scriptDirName + "/childPidFile" + 0);
      }

      // As childPidFile0(leaf process in the subtree of processes with
      // map task as root) is created, all other child pid files should
      // have been created already(See the script for details).
      // Now check if the descendants of map task are alive.
      for(int i=0; i <= numLevelsOfSubProcesses; i++) {
        childPid = ProcessTree.getPidFromPidFile(
                               scriptDirName + "/childPidFile" + i);
        LOG.info("pid of the descendant process at level " + i +
                 "in the subtree of processes(with the map task as the root)" +
                 " is " + childPid);
        assertTrue("Unexpected: The subprocess at level " + i +
                   " in the subtree is not alive before killJob",
                   ProcessTree.isAlive(childPid));
      }
    }

    // kill the job now
    job.killJob();

    while (job.cleanupProgress() == 0.0f) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException ie) {
        LOG.warn("sleep is interrupted:" + ie);
        break;
      }
    }

    // Checking that the Job got killed
    assertTrue(job.isComplete());
    assertEquals(job.getJobState(), JobStatus.KILLED);

    // Checking if the map task got killed or not
    assertTrue(!ProcessTree.isAlive(pid));
    LOG.info("The map task is not alive after killJob, as expected.");

    // Checking if the descendant processes of map task are killed properly
    if(ProcessTree.isSetsidAvailable) {
      for(int i=0; i <= numLevelsOfSubProcesses; i++) {
        String childPid = ProcessTree.getPidFromPidFile(
                               scriptDirName + "/childPidFile" + i);
        LOG.info("pid of the descendant process at level " + i +
                 "in the subtree of processes(with the map task as the root)" +
                 " is " + childPid);
        assertTrue("Unexpected: The subprocess at level " + i +
                   " in the subtree is alive after killJob",
                   !ProcessTree.isAlive(childPid));
      }
    }

    return job.getID();
  }

  static RunningJob runJob(JobConf conf) throws IOException {

    final Path inDir = new Path(TEST_ROOT_DIR + "/killjob/input");
    final Path outDir = new Path(TEST_ROOT_DIR + "/killjob/output");

    FileSystem fs = FileSystem.get(conf);
    if(fs.exists(outDir)) {
      fs.delete(outDir, true);
    }
    if(fs.exists(scriptDir)) {
      fs.delete(scriptDir, true);
    }
    if (!fs.exists(inDir)) {
      fs.mkdirs(inDir);
    }
    // create input file
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
    conf.set("test.build.data", TEST_ROOT_DIR);

    jobClient = new JobClient(conf);
    RunningJob job = jobClient.submitJob(conf);

    return job;

  }

  public void testJobKill() throws IOException {
    JobConf conf=null;
    try {
      mr = new MiniMRCluster(1, "file:///", 1);

      // run the TCs
      conf = mr.createJobConf();
      JobTracker jt = mr.getJobTrackerRunner().getJobTracker();
      runJobKill(jt, conf);
    } finally {
      if (mr != null) {
        mr.shutdown();
      }
      FileSystem fs = FileSystem.get(conf);
      if(fs.exists(scriptDir)) {
        fs.delete(scriptDir, true);
      }
    }
  }

  static class KillMapperWithChild extends MapReduceBase implements
      Mapper<WritableComparable, Writable, WritableComparable, Writable> {
    public void configure(JobConf conf) {
      try {
        FileSystem fs = FileSystem.get(conf);
        TEST_ROOT_DIR = conf.get("test.build.data").toString().substring(5);
        scriptDir = new Path(TEST_ROOT_DIR + "/killjob");

        if(ProcessTree.isSetsidAvailable) {
          // create shell script
          Random rm = new Random();
          Path scriptPath = new Path(scriptDir, "_shellScript_" + rm.nextInt() + ".sh");
          String shellScript = scriptPath.toString();
          String script =
             "echo $$ > " + scriptDir.toString() + "/childPidFile" + "$1\n" +
             "echo hello\nsleep 1\n" +
             "if [ $1 != 0 ]\nthen\n" +
             " sh " + shellScript + " $(($1-1))\n" +
             "else\n" +
             " while true\n do\n" +
             "  sleep 2\n" +
             " done\n" +
             "fi";
          DataOutputStream file = fs.create(scriptPath);
          file.writeBytes(script);
          file.close();

          LOG.info("Calling script from map task : " + shellScript);
          Runtime.getRuntime().exec(shellScript + " " +
                                    numLevelsOfSubProcesses);
        }
      } catch (Exception e) {
        LOG.warn("Exception in KillMapperWithChild.configure: " +
                 StringUtils.stringifyException(e));
      }
    }
    public void map(WritableComparable key, Writable value,
        OutputCollector<WritableComparable, Writable> out, Reporter reporter)
        throws IOException {

      try {
        while(true) {//just wait till kill happens
          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {
        LOG.warn("Exception in KillMapperWithChild.map:" + e);
      }
    }
  }
}

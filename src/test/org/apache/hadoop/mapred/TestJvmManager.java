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
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.TrackerDistributedCacheManager;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.mapred.JvmManager.JvmManagerForType;
import org.apache.hadoop.mapred.JvmManager.JvmManagerForType.JvmRunner;
import org.apache.hadoop.mapred.TaskTracker.LocalStorage;
import org.apache.hadoop.mapred.TaskTracker.RunningJob;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.mapred.UtilsForTests.InlineCleanupQueue;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.tasktracker.userlogs.UserLogManager;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class TestJvmManager {
  private static final Log LOG = LogFactory.getLog(TestJvmManager.class);
  private static File TEST_DIR = new File(System.getProperty("test.build.data",
      "/tmp"), TestJvmManager.class.getSimpleName());
  private static int MAP_SLOTS = 1;
  private static int REDUCE_SLOTS = 1;
  private TaskTracker tt;
  private JvmManager jvmManager;
  private JobConf ttConf;
  private boolean threadCaughtException = false;
  private String user;
  private final String sleepScriptName = Shell.WINDOWS ? "SLEEP.cmd" : "SLEEP";
  private final String sleepCommand = "sleep 60\n";
  private final String lsScriptName = Shell.WINDOWS ? "LS.cmd" : "LS";
  private final String lsCommand =
      StringUtils.join(" ", Shell.getGetPermissionCommand());

  @Before
  public void setUp() {
    TEST_DIR.mkdirs();
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.fullyDelete(TEST_DIR);
  }

  public TestJvmManager() throws Exception {
    user = UserGroupInformation.getCurrentUser().getShortUserName();
    tt = new TaskTracker();
    ttConf = new JobConf();
    ttConf.setLong("mapred.tasktracker.tasks.sleeptime-before-sigkill", 2000);
    tt.setConf(ttConf);
    tt.setMaxMapSlots(MAP_SLOTS);
    tt.setMaxReduceSlots(REDUCE_SLOTS);
    TaskController dtc;
    tt.setTaskController((dtc = new DefaultTaskController()));
    Configuration conf = new Configuration();
    dtc.setConf(conf);
    LocalDirAllocator ldirAlloc =
        new LocalDirAllocator(JobConf.MAPRED_LOCAL_DIR_PROPERTY);
    tt.getTaskController().setup(ldirAlloc, new LocalStorage(ttConf.getLocalDirs()));
    JobID jobId = new JobID("test", 0);
    jvmManager = new JvmManager(tt);
    tt.setJvmManagerInstance(jvmManager);
    tt.setUserLogManager(new UserLogManager(ttConf));
    tt.setCleanupThread(new InlineCleanupQueue());
  }

  // write a shell script to execute the command.
  private File writeScript(String fileName, String cmd, File pidFile) throws IOException {
    File script = new File(TEST_DIR, fileName);
    FileOutputStream out = new FileOutputStream(script);
    // write pid into a file and ignore SIGTERM
    String command = Shell.WINDOWS
        // On Windows we pass back the attempt id that was passed to the task
        // through the environment.
      ? "echo %ATTEMPT_ID% > " + pidFile.toString() + "\r\n"
      : "echo $$ >" + pidFile.toString() + ";\n trap '' 15\n";
    out.write((command).getBytes());
    // write the actual command it self.
    out.write(cmd.getBytes());
    out.close();
    script.setExecutable(true);
    return script;
  }
  
  // Create an empty shell script to run from tasks.
  private File writeEmptyScript(String fileName) throws IOException {
    File script = new File(TEST_DIR, fileName);
    script.createNewFile();
    script.setExecutable(true);
    return script;
  }
  
  /**
   * Tests the jvm kill from JvmRunner and JvmManager simultaneously.
   * 
   * Starts a process, which sleeps for 60 seconds, in a thread.
   * Calls JvmRunner.kill() in a thread.
   * Also calls JvmManager.taskKilled().
   * Makes sure that the jvm is killed and JvmManager could launch another task
   * properly.
   * @throws Exception
   */
  @Test
  public void testJvmKill() throws Exception {
    JvmManagerForType mapJvmManager = jvmManager
        .getJvmManagerForType(TaskType.MAP);
    // launch a jvm
    JobConf taskConf = new JobConf(ttConf);
    TaskAttemptID attemptID = new TaskAttemptID("test", 0, true, 0, 0);
    Task task = new MapTask(null, attemptID, 0, null, MAP_SLOTS);
    task.setUser(user);
    task.setConf(taskConf);
    TaskInProgress tip = tt.new TaskInProgress(task, taskConf);
    File pidFile = new File(TEST_DIR, "pid");
    RunningJob rjob = new RunningJob(attemptID.getJobID());
    TaskController taskController = new DefaultTaskController();
    taskController.setConf(ttConf);
    rjob.distCacheMgr = 
      new TrackerDistributedCacheManager(ttConf, taskController).
      newTaskDistributedCacheManager(attemptID.getJobID(), taskConf);
    final TaskRunner taskRunner = task.createRunner(tt, tip, rjob);
    // launch a jvm which sleeps for 60 seconds
    final Vector<String> setup = new Vector<String>(1);
    setup.add((Shell.WINDOWS ? "set " : "export ")
      + "ATTEMPT_ID=" + attemptID.toString());
    final Vector<String> vargs = new Vector<String>(2);
    vargs.add(writeScript(sleepScriptName, sleepCommand, pidFile).getAbsolutePath());
    final File workDir = new File(TEST_DIR, "work");
    final File stdout = new File(TEST_DIR, "stdout");
    final File stderr = new File(TEST_DIR, "stderr");

    // launch the process and wait in a thread, till it finishes
    Thread launcher = new Thread() {
      public void run() {
        try {
          taskRunner.launchJvmAndWait(setup, vargs, stdout, stderr, 100,
              workDir);
        } catch (InterruptedException e) {
          e.printStackTrace();
          return;
        } catch (IOException e) {
          e.printStackTrace();
          setThreadCaughtException();
        }
      }
    };
    launcher.start();
    // wait till the jvm is launched
    // this loop waits for at most 1 second
    for (int i = 0; i < 10; i++) {
      if (pidFile.exists()) {
        break;
      }
      UtilsForTests.waitFor(100);
    }
    // assert that the process is launched
    assertTrue("pidFile is not present", pidFile.exists());
    
    // imitate Child code.
    // set pid in jvmManager
    BufferedReader in = new  BufferedReader(new FileReader(pidFile));
    String pid = in.readLine();
    in.close();
    JVMId jvmid = mapJvmManager.runningTaskToJvm.get(taskRunner);
    jvmManager.setPidToJvm(jvmid, pid);

    // kill JvmRunner
    final JvmRunner jvmRunner = mapJvmManager.jvmIdToRunner.get(jvmid);
    Thread killer = new Thread() {
      public void run() {
        try {
          jvmRunner.kill();
        } catch (IOException e) {
          e.printStackTrace();
          setThreadCaughtException();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    };
    killer.start();
    
    //wait for a while so that killer thread is started.
    Thread.sleep(100);

    // kill the jvm externally
    taskRunner.kill();

    assertTrue(jvmRunner.killed);

    // launch another jvm and see it finishes properly
    attemptID = new TaskAttemptID("test", 0, true, 0, 1);
    task = new MapTask(null, attemptID, 0, null, MAP_SLOTS);
    task.setUser(user);
    task.setConf(taskConf);
    tip = tt.new TaskInProgress(task, taskConf);
    TaskRunner taskRunner2 = task.createRunner(tt, tip, rjob);
    // build dummy vargs to call ls
    Vector<String> vargs2 = new Vector<String>(1);
    vargs2.add(writeScript(lsScriptName, lsCommand, pidFile).getAbsolutePath());
    File workDir2 = new File(TEST_DIR, "work2");
    File stdout2 = new File(TEST_DIR, "stdout2");
    File stderr2 = new File(TEST_DIR, "stderr2");
    taskRunner2.launchJvmAndWait(null, vargs2, stdout2, stderr2, 100, workDir2);
    // join all the threads
    killer.join();
    jvmRunner.join();
    launcher.join();
    assertFalse("Thread caught unexpected IOException", 
                 threadCaughtException);
  }
  private void setThreadCaughtException() {
    threadCaughtException = true;
  }
  
  /**
   * Test launchJvmAndWait overloads, and verify performance difference does
   * not exceed TIME_DIFF_THRESHOLD
   * 
   * More details:
   * - Create a Task that will simply invoke an empty script
   * - Run the task using the default launchJvmAndWait – (RUN_JVM_COUNT) times
   *    and measure total time 
   * - Run the task using the overload of launchJvmAndWait which takes a list
   *    of classpath entries – (RUN_JVM_COUNT) times, and measure total time
   * - Assert the time difference is not more than TIME_DIFF_THRESHOLD
   */
  @Test
  public void testJvmLaunchWithClasspathPerf() throws Exception {
    
    final int RUN_JVM_COUNT = 200;
    final int TIME_DIFF_THRESHOLD = 10;
    
    String jvmTaskCmdName = Shell.WINDOWS ? "writeToFile.cmd" : "writeToFile";
    final Vector<String> vargs = new Vector<String>(2);
    vargs.add(writeEmptyScript(jvmTaskCmdName).getAbsolutePath());
    
    final File workDir = new File(TEST_DIR, "work");
    final File stdout = new File(TEST_DIR, "stdout");
    final File stderr = new File(TEST_DIR, "stderr");

    // Ensure all files are deleted from previous tests
    if (workDir.exists()) {
      FileUtil.fullyDelete(workDir);
    }
    if (stdout.exists()) {
      stdout.delete();
    }
    if (stderr.exists()) {
      stderr.delete();
    }
   
    TaskRunner taskRunner;
    
    // Create a class-path list
    List<String> classPaths = new ArrayList<String>();
    for (int clsPathElemnts = 0; clsPathElemnts < 10; ++clsPathElemnts) {
      classPaths.add(TEST_DIR.getPath());
      classPaths.add(workDir.getPath());
      classPaths.add(stdout.getPath());
      classPaths.add(stderr.getPath());
    }
    
    // Get the start time before launching the tasks
    long startTime = 0;
    long endTime = 0;
    long totalTimeNoJar = 0;
    long totalTimeWithJar = 0;

    // Test launching the the task without constructing classpath jar
    for (int iNoJar = 0; iNoJar < RUN_JVM_COUNT; ++iNoJar) {
      // Create a new task and name it using the current run count
      taskRunner = prepareNewTask(0, iNoJar, 0);
      
      // Vargs are changed by the below overload of launchJvmAndWait which add
      // the classpath as args, so we use a copy of vargs with every iteration
      Vector<String> vargsCopy = new Vector<String>(vargs);
      
      // Get start time
      startTime = System.currentTimeMillis();
      
      // Launch the the task without constructing classpath jar
      taskRunner.launchJvmAndWait(null, vargsCopy, null, stdout, stderr, 
          100, workDir);

      // Get end time
      endTime = System.currentTimeMillis();
      
      totalTimeNoJar = totalTimeNoJar + (endTime - startTime);
      
      // Clean generated files
      workDir.delete();
      stdout.delete();
      stderr.delete();
    }
    
    // Test launching the the task with constructing classpath jar
    for (int iWithJar = 0; iWithJar < RUN_JVM_COUNT; ++iWithJar) {
      // Create a new task and name it using the current run count
      taskRunner = prepareNewTask(0, iWithJar, 1);
      
      // Vargs are changed by the below overload of launchJvmAndWait which add
      // the classpath as args, so we use a copy of vargs with every iteration
      Vector<String> vargsCopy = new Vector<String>(vargs);
      
      // Get start time
      startTime = System.currentTimeMillis();
      
      // Launch the the task constructing classpath jar    
      taskRunner.launchJvmAndWait(null, vargsCopy, classPaths, stdout, stderr, 
          100, workDir);
      
      // Get end time
      endTime = System.currentTimeMillis();
      
      totalTimeWithJar = totalTimeWithJar + (endTime - startTime);
      
      // Clean generated files
      workDir.delete();
      stdout.delete();
      stderr.delete();
    }

    // Measure the time difference
    double timeDiffMilli = Math.abs(totalTimeWithJar - totalTimeNoJar);
    double diffPercentage = (timeDiffMilli / totalTimeNoJar) * 100;
    
    // Log results
    LOG.info(
        "Time taken for launchJvmAndWait without classpath (milli seconds): "
        + totalTimeNoJar);
    LOG.info(
        "Time taken for launchJvmAndWait with classpath (milli seconds): "
        + totalTimeWithJar);
    LOG.info("Time difference is: " + diffPercentage + "%");
    
    // Verify difference does not exceed TIME_DIFF_THRESHOLD
    assertTrue(diffPercentage <= TIME_DIFF_THRESHOLD);
  }
  
  /**
   * Helper function to create a new task with the given jobId, ttaskId,
   * and attemptId
   * Returns the TaskRunner used to launch the task
   */
  private TaskRunner prepareNewTask(int jobId, int ttaskId, int attemptId)
      throws IOException{
    JobConf taskConf = new JobConf(ttConf);
    TaskAttemptID attemptID = new TaskAttemptID("test", jobId, true, ttaskId,
        attemptId);
    Task task = new MapTask(null, attemptID, 0, null, MAP_SLOTS);
    task.setUser(user);
    task.setConf(taskConf);
    TaskInProgress tip = tt.new TaskInProgress(task, taskConf);
    RunningJob rjob = new RunningJob(attemptID.getJobID());
    TaskController taskController = new DefaultTaskController();
    taskController.setConf(ttConf);
    rjob.distCacheMgr = 
      new TrackerDistributedCacheManager(ttConf, taskController).
          newTaskDistributedCacheManager(attemptID.getJobID(), taskConf);
    return task.createRunner(tt, tip, rjob);
  }
}

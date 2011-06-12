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
import java.util.Vector;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.mapred.JvmManager.JvmManagerForType;
import org.apache.hadoop.mapred.JvmManager.JvmManagerForType.JvmRunner;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

public class TestJvmManager {
  private static File TEST_DIR = new File(System.getProperty("test.build.data",
      "/tmp"), TestJvmManager.class.getSimpleName());
  private static int MAP_SLOTS = 1;
  private static int REDUCE_SLOTS = 1;
  private TaskTracker tt;
  private JvmManager jvmManager;
  private JobConf ttConf;

  @Before
  public void setUp() {
    TEST_DIR.mkdirs();
  }

  @After
  public void tearDown() throws IOException {
    FileUtil.fullyDelete(TEST_DIR);
  }

  public TestJvmManager() throws Exception {
    tt = new TaskTracker();
    ttConf = new JobConf();
    ttConf.setLong(TTConfig.TT_SLEEP_TIME_BEFORE_SIG_KILL, 2000);
    tt.setConf(ttConf);
    tt.setMaxMapSlots(MAP_SLOTS);
    tt.setMaxReduceSlots(REDUCE_SLOTS);
    tt.setTaskController(new DefaultTaskController());
    jvmManager = new JvmManager(tt);
    tt.setJvmManagerInstance(jvmManager);
  }

  // write a shell script to execute the command.
  private File writeScript(String fileName, String cmd, File pidFile) throws IOException {
    File script = new File(TEST_DIR, fileName);
    FileOutputStream out = new FileOutputStream(script);
    // write pid into a file
    out.write(("echo $$ >" + pidFile.toString() + ";").getBytes());
    // ignore SIGTERM
    out.write(("trap '' 15\n").getBytes());
    // write the actual command it self.
    out.write(cmd.getBytes());
    out.close();
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
    TaskAttemptID attemptID = new TaskAttemptID("test", 0, TaskType.MAP, 0, 0);
    Task task = new MapTask(null, attemptID, 0, null, MAP_SLOTS);
    task.setConf(taskConf);
    TaskInProgress tip = tt.new TaskInProgress(task, taskConf);
    File pidFile = new File(TEST_DIR, "pid");
    final TaskRunner taskRunner = task.createRunner(tt, tip);
    // launch a jvm which sleeps for 60 seconds
    final Vector<String> vargs = new Vector<String>(2);
    vargs.add(writeScript("SLEEP", "sleep 60\n", pidFile).getAbsolutePath());
    final File workDir = new File(TEST_DIR, "work");
    workDir.mkdir();
    final File stdout = new File(TEST_DIR, "stdout");
    final File stderr = new File(TEST_DIR, "stderr");

    // launch the process and wait in a thread, till it finishes
    Thread launcher = new Thread() {
      public void run() {
        try {
          taskRunner.launchJvmAndWait(null, vargs, stdout, stderr, 100,
              workDir, null);
        } catch (InterruptedException e) {
          e.printStackTrace();
          return;
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
        jvmRunner.kill();
      }
    };
    killer.start();
    
    //wait for a while so that killer thread is started.
    Thread.sleep(100);

    // kill the jvm externally
    taskRunner.kill();

    assertTrue(jvmRunner.killed);

    // launch another jvm and see it finishes properly
    attemptID = new TaskAttemptID("test", 0, TaskType.MAP, 0, 1);
    task = new MapTask(null, attemptID, 0, null, MAP_SLOTS);
    task.setConf(taskConf);
    tip = tt.new TaskInProgress(task, taskConf);
    TaskRunner taskRunner2 = task.createRunner(tt, tip);
    // build dummy vargs to call ls
    Vector<String> vargs2 = new Vector<String>(1);
    vargs2.add(writeScript("LS", "ls", pidFile).getAbsolutePath());
    File workDir2 = new File(TEST_DIR, "work2");
    workDir.mkdir();
    File stdout2 = new File(TEST_DIR, "stdout2");
    File stderr2 = new File(TEST_DIR, "stderr2");
    taskRunner2.launchJvmAndWait(null, vargs2, stdout2, stderr2, 100, workDir2,
        null);
    // join all the threads
    killer.join();
    jvmRunner.join();
    launcher.join();
  }
}

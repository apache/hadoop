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
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.server.tasktracker.TTConfig;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.UserGroupInformation;
import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDebugScript {

  // base directory which is used by the debug script
  private static final String BASE_DIR = new File(System.getProperty(
      "test.build.data", "/tmp")).getAbsolutePath();

  // script directory which is passed as dummy input + where debugscript
  // is written.
  private static final String SCRIPT_DIR = new File(BASE_DIR, "debugscript")
      .getAbsolutePath();

  // script which is used as debug script.
  private static final String SCRIPT_FILE = new File(SCRIPT_DIR,
      "debugscript.sh").getAbsolutePath();

  // making an assumption we have bash in path. Same as TaskLog. 
  // The debug script just accesses the stderr file of the task
  // and does a 'cat' on it
  private static final String SCRIPT_CONTENT = "cat $2";

  @Before
  public void setup() throws Exception {
    setupDebugScriptDirs();
  }

  @After
  public void tearDown() throws Exception {
    cleanupDebugScriptDirs();
  }

  /**
   * Cleanup method which is used to delete the files folder which are generated
   * by the testcase.
   * 
   */
  static void cleanupDebugScriptDirs() {
    File scriptFile = new File(SCRIPT_FILE);
    scriptFile.delete();
    File scriptDir = new File(SCRIPT_DIR);
    scriptDir.delete();
  }

  /**
   * Setup method which is used to create necessary files and folder for the
   * testcase.
   * 
   * @throws Exception
   */
  static void setupDebugScriptDirs() throws Exception {
    File scriptDir = new File(SCRIPT_DIR);
    if (!scriptDir.exists()) {
      scriptDir.mkdirs();
    }
    scriptDir.setExecutable(true, false);
    scriptDir.setReadable(true, false);
    scriptDir.setWritable(true, false);
    File scriptFile = new File(SCRIPT_FILE);
    PrintWriter writer = new PrintWriter(scriptFile);
    writer.println(SCRIPT_CONTENT);
    writer.flush();
    writer.close();
    scriptFile.setExecutable(true, false);
    scriptFile.setReadable(true, false);
  }

  /**
   * Main test case which checks proper execution of the testcase.
   * 
   * @throws Exception
   */
  @Test
  public void testDebugScript() throws Exception {
    JobConf conf = new JobConf();
    conf.setLong(TTConfig.TT_SLEEP_TIME_BEFORE_SIG_KILL, 0L);
    MiniMRCluster mrCluster = new MiniMRCluster(1, "file:///", 1, null, null, conf);
    Path inputPath = new Path(SCRIPT_DIR);
    Path outputPath = new Path(SCRIPT_DIR, "task_output");
    
    // Run a failing mapper so debug script is launched.
    JobID jobId = runFailingMapJob(mrCluster.createJobConf(), inputPath,
        outputPath);
    // construct the task id of first map task of failmap
    TaskAttemptID taskId = new TaskAttemptID(
        new TaskID(jobId,TaskType.MAP, 0), 0);
    // verify if debug script was launched correctly and ran correctly.
    verifyDebugScriptOutput(taskId);
  }

  /**
   * Method which verifies if debug script ran and ran correctly.
   * 
   * @param taskId
   * @param expectedUser
   *          expected user id from debug script
   * @throws Exception
   */
  static void verifyDebugScriptOutput(TaskAttemptID taskId) throws Exception {
    verifyDebugScriptOutput(taskId, null, null, null);
  }
  /**
   * Method which verifies if debug script ran and ran correctly.
   * 
   * @param taskId
   * @param expectedUser
   *          expected user id from debug script
   * @param expectedPerms the expected permissions on the debugout file
   * @throws Exception
   */
  static void verifyDebugScriptOutput(TaskAttemptID taskId, String expectedUser, 
      String expectedGroup, String expectedPerms) throws Exception {
    File output = TaskLog.getRealTaskLogFileLocation(taskId, false,
        TaskLog.LogName.DEBUGOUT);
    // Check the presence of the output file if the script is to be run.
    assertTrue("Output file does not exists. DebugScript has not been run",
          output.exists());
    // slurp the output from file, which is one line
    BufferedReader reader = new BufferedReader(new FileReader(output));
    String out = reader.readLine();
    // close the file.
    reader.close();
    // Check if there is any output
    assertNotNull("DebugScript didn't generate output.", out);
    assertTrue(out.contains("failing map"));
    if (expectedPerms != null && expectedUser != null) {
      //check whether the debugout file ownership/permissions are as expected
      TestTaskTrackerLocalization.checkFilePermissions(output.getAbsolutePath(),
          expectedPerms, expectedUser, expectedGroup);
    }
  }

  /**
   * Method to run a failing mapper on a given Cluster.
   * 
   * @param conf
   *          the JobConf for the job
   * @param inputPath
   *          input path for the job.
   * @param outputDir
   *          output directory for job.
   * @throws IOException
   */
  static JobID runFailingMapJob(JobConf conf, Path inputPath, Path outputDir)
      throws IOException {
    conf.setMapDebugScript(SCRIPT_FILE);
    conf.setMaxMapAttempts(0);
    conf.set("mapred.committer.job.setup.cleanup.needed", "false");
    RunningJob rJob = UtilsForTests.runJobFail(conf, inputPath, outputDir);
    return rJob.getID();
  }
}

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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;

import junit.framework.TestCase;

/**
 * Test to verify localization of a job and localization of a task on a
 * TaskTracker.
 * 
 */
public class TestTaskTrackerLocalization extends TestCase {

  private File TEST_ROOT_DIR;
  private File ROOT_MAPRED_LOCAL_DIR;
  private File HADOOP_LOG_DIR;

  private int numLocalDirs = 6;
  private static final Log LOG =
      LogFactory.getLog(TestTaskTrackerLocalization.class);

  protected TaskTracker tracker;
  protected JobConf trackerFConf;
  protected JobID jobId;
  protected TaskAttemptID taskId;
  protected Task task;
  protected String[] localDirs;
  protected static LocalDirAllocator lDirAlloc =
      new LocalDirAllocator("mapred.local.dir");

  @Override
  protected void setUp()
      throws Exception {
    TEST_ROOT_DIR =
        new File(System.getProperty("test.build.data", "/tmp"),
            "testTaskTrackerLocalization");
    if (!TEST_ROOT_DIR.exists()) {
      TEST_ROOT_DIR.mkdirs();
    }

    ROOT_MAPRED_LOCAL_DIR = new File(TEST_ROOT_DIR, "mapred/local");
    ROOT_MAPRED_LOCAL_DIR.mkdirs();

    HADOOP_LOG_DIR = new File(TEST_ROOT_DIR, "logs");
    HADOOP_LOG_DIR.mkdir();
    System.setProperty("hadoop.log.dir", HADOOP_LOG_DIR.getAbsolutePath());

    trackerFConf = new JobConf();
    trackerFConf.set("fs.default.name", "file:///");
    localDirs = new String[numLocalDirs];
    for (int i = 0; i < numLocalDirs; i++) {
      localDirs[i] = new File(ROOT_MAPRED_LOCAL_DIR, "0_" + i).getPath();
    }
    trackerFConf.setStrings("mapred.local.dir", localDirs);

    // Create the job jar file
    File jobJarFile = new File(TEST_ROOT_DIR, "jobjar-on-dfs.jar");
    JarOutputStream jstream =
        new JarOutputStream(new FileOutputStream(jobJarFile));
    ZipEntry ze = new ZipEntry("lib/lib1.jar");
    jstream.putNextEntry(ze);
    jstream.closeEntry();
    ze = new ZipEntry("lib/lib2.jar");
    jstream.putNextEntry(ze);
    jstream.closeEntry();
    jstream.finish();
    jstream.close();
    trackerFConf.setJar(jobJarFile.toURI().toString());

    // Create the job configuration file
    File jobConfFile = new File(TEST_ROOT_DIR, "jobconf-on-dfs.xml");
    FileOutputStream out = new FileOutputStream(jobConfFile);
    trackerFConf.writeXml(out);
    out.close();

    // Set up the TaskTracker
    tracker = new TaskTracker();
    tracker.setConf(trackerFConf);
    tracker.systemFS = FileSystem.getLocal(trackerFConf); // for test case

    // Set up the task to be localized
    String jtIdentifier = "200907202331";
    jobId = new JobID(jtIdentifier, 1);
    taskId =
        new TaskAttemptID(jtIdentifier, jobId.getId(), true, 1, 0);
    task =
        new MapTask(jobConfFile.toURI().toString(), taskId, 1, null, 1);

    TaskController taskController = new DefaultTaskController();
    taskController.setConf(trackerFConf);
    taskController.setup();
  }

  @Override
  protected void tearDown()
      throws Exception {
    FileUtil.fullyDelete(TEST_ROOT_DIR);
  }

  protected static String[] getFilePermissionAttrs(String path)
      throws IOException {
    String output = Shell.execCommand("stat", path, "-c", "%A:%U:%G");
    return output.split(":|\n");
  }

  static void checkFilePermissions(String path, String expectedPermissions,
      String expectedOwnerUser, String expectedOwnerGroup)
      throws IOException {
    String[] attrs = getFilePermissionAttrs(path);
    assertTrue("File attrs length is not 3 but " + attrs.length,
        attrs.length == 3);
    assertTrue("Path " + path + " has the permissions " + attrs[0]
        + " instead of the expected " + expectedPermissions, attrs[0]
        .equals(expectedPermissions));
    assertTrue("Path " + path + " is not user owned not by "
        + expectedOwnerUser + " but by " + attrs[1], attrs[1]
        .equals(expectedOwnerUser));
    assertTrue("Path " + path + " is not group owned not by "
        + expectedOwnerGroup + " but by " + attrs[2], attrs[2]
        .equals(expectedOwnerGroup));
  }

  /**
   * Verify the task-controller's setup functionality
   * 
   * @throws IOException
   * @throws LoginException
   */
  public void testTaskControllerSetup()
      throws IOException,
      LoginException {
    // Task-controller is already set up in the test's setup method. Now verify.
    UserGroupInformation ugi = UserGroupInformation.login(new JobConf());
    for (String localDir : localDirs) {

      // Verify the local-dir itself.
      File lDir = new File(localDir);
      assertTrue("localDir " + lDir + " doesn't exists!", lDir.exists());
      checkFilePermissions(lDir.getAbsolutePath(), "drwxr-xr-x", ugi
          .getUserName(), ugi.getGroupNames()[0]);

      // Verify the distributed cache dir.
      File distributedCacheDir =
          new File(localDir, TaskTracker.getDistributedCacheDir());
      assertTrue("distributed cache dir " + distributedCacheDir
          + " doesn't exists!", distributedCacheDir.exists());
      checkFilePermissions(distributedCacheDir.getAbsolutePath(),
          "drwxr-xr-x", ugi.getUserName(), ugi.getGroupNames()[0]);

      // Verify the job cache dir.
      File jobCacheDir = new File(localDir, TaskTracker.getJobCacheSubdir());
      assertTrue("jobCacheDir " + jobCacheDir + " doesn't exists!",
          jobCacheDir.exists());
      checkFilePermissions(jobCacheDir.getAbsolutePath(), "drwxr-xr-x", ugi
          .getUserName(), ugi.getGroupNames()[0]);
    }

    // Verify the pemissions on the userlogs dir
    File taskLog = TaskLog.getUserLogDir();
    checkFilePermissions(taskLog.getAbsolutePath(), "drwxr-xr-x", ugi
        .getUserName(), ugi.getGroupNames()[0]);
  }

  /**
   * Test job localization on a TT. Tests localization of job.xml, job.jar and
   * corresponding setting of configuration.
   * 
   * @throws IOException
   * @throws LoginException
   */
  public void testJobLocalization()
      throws IOException,
      LoginException {

    // /////////// The main method being tested
    JobConf localizedJobConf = tracker.localizeJobFiles(task);
    // ///////////

    // Check the directory structure
    for (String dir : localDirs) {

      File localDir = new File(dir);
      assertTrue("mapred.local.dir " + localDir + " isn'task created!",
          localDir.exists());

      File taskTrackerSubDir = new File(localDir, TaskTracker.SUBDIR);
      assertTrue("taskTracker sub-dir in the local-dir " + localDir
          + "is not created!", taskTrackerSubDir.exists());

      File jobCache = new File(taskTrackerSubDir, TaskTracker.JOBCACHE);
      assertTrue("jobcache in the taskTrackerSubdir " + taskTrackerSubDir
          + " isn'task created!", jobCache.exists());

      File jobDir = new File(jobCache, jobId.toString());
      assertTrue("job-dir in " + jobCache + " isn'task created!", jobDir
          .exists());

      // check the private permissions on the job directory
      UserGroupInformation ugi = UserGroupInformation.login(localizedJobConf);
      checkFilePermissions(jobDir.getAbsolutePath(), "drwx------", ugi
          .getUserName(), ugi.getGroupNames()[0]);
    }

    // check the localization of job.xml
    LocalDirAllocator lDirAlloc = new LocalDirAllocator("mapred.local.dir");

    assertTrue("job.xml is not localized on this TaskTracker!!", lDirAlloc
        .getLocalPathToRead(TaskTracker.getLocalJobConfFile(jobId.toString()),
            trackerFConf) != null);

    // check the localization of job.jar
    Path jarFileLocalized =
        lDirAlloc.getLocalPathToRead(TaskTracker.getJobJarFile(jobId
            .toString()), trackerFConf);
    assertTrue("job.jar is not localized on this TaskTracker!!",
        jarFileLocalized != null);
    assertTrue("lib/lib1.jar is not unjarred on this TaskTracker!!", new File(
        jarFileLocalized.getParent() + Path.SEPARATOR + "lib/lib1.jar")
        .exists());
    assertTrue("lib/lib2.jar is not unjarred on this TaskTracker!!", new File(
        jarFileLocalized.getParent() + Path.SEPARATOR + "lib/lib2.jar")
        .exists());

    // check the creation of job work directory
    assertTrue("job-work dir is not created on this TaskTracker!!", lDirAlloc
        .getLocalPathToRead(TaskTracker.getJobWorkDir(jobId.toString()),
            trackerFConf) != null);

    // Check the setting of job.local.dir and job.jar which will eventually be
    // used by the user's task
    boolean jobLocalDirFlag = false, mapredJarFlag = false;
    String localizedJobLocalDir =
        localizedJobConf.get(TaskTracker.JOB_LOCAL_DIR);
    String localizedJobJar = localizedJobConf.getJar();
    for (String localDir : localizedJobConf.getStrings("mapred.local.dir")) {
      if (localizedJobLocalDir.equals(localDir + Path.SEPARATOR
          + TaskTracker.getJobWorkDir(jobId.toString()))) {
        jobLocalDirFlag = true;
      }
      if (localizedJobJar.equals(localDir + Path.SEPARATOR
          + TaskTracker.getJobJarFile(jobId.toString()))) {
        mapredJarFlag = true;
      }
    }
    assertTrue(TaskTracker.JOB_LOCAL_DIR
        + " is not set properly to the target users directory : "
        + localizedJobLocalDir, jobLocalDirFlag);
    assertTrue(
        "mapred.jar is not set properly to the target users directory : "
            + localizedJobJar, mapredJarFlag);
  }

  /**
   * Test task localization on a TT.
   * 
   * @throws IOException
   * @throws LoginException
   */
  public void testTaskLocalization()
      throws IOException,
      LoginException {

    JobConf localizedJobConf = tracker.localizeJobFiles(task);

    TaskInProgress tip = tracker.new TaskInProgress(task, trackerFConf);
    tip.setJobConf(localizedJobConf);

    // ////////// The central method being tested
    tip.localizeTask(task);
    // //////////

    // check the functionality of localizeTask
    for (String dir : trackerFConf.getStrings("mapred.local.dir")) {
      assertTrue("attempt-dir in localDir " + dir + " is not created!!",
          new File(dir, TaskTracker.getLocalTaskDir(jobId.toString(), taskId
              .toString())).exists());
    }

    Path workDir =
        lDirAlloc.getLocalPathToRead(TaskTracker.getTaskWorkDir(task
            .getJobID().toString(), task.getTaskID().toString(), task
            .isTaskCleanupTask()), trackerFConf);
    assertTrue("atttempt work dir for " + taskId.toString()
        + " is not created in any of the configured dirs!!", workDir != null);

    TaskRunner runner = task.createRunner(tracker, tip);

    // /////// Few more methods being tested
    runner.setupChildTaskConfiguration(lDirAlloc);
    TaskRunner.createChildTmpDir(new File(workDir.toUri().getPath()),
        localizedJobConf);
    File[] logFiles = TaskRunner.prepareLogFiles(task.getTaskID());
    // ///////

    // Make sure the task-conf file is created
    Path localTaskFile =
        lDirAlloc.getLocalPathToRead(TaskTracker.getTaskConfFile(task
            .getJobID().toString(), task.getTaskID().toString(), task
            .isTaskCleanupTask()), trackerFConf);
    assertTrue("Task conf file " + localTaskFile.toString()
        + " is not created!!", new File(localTaskFile.toUri().getPath())
        .exists());

    // /////// One more method being tested. This happens in child space.
    JobConf localizedTaskConf = new JobConf(localTaskFile);
    TaskRunner.setupChildMapredLocalDirs(task, localizedTaskConf);
    // ///////

    // Make sure that the mapred.local.dir is sandboxed
    for (String childMapredLocalDir : localizedTaskConf
        .getStrings("mapred.local.dir")) {
      assertTrue("Local dir " + childMapredLocalDir + " is not sandboxed !!",
          childMapredLocalDir.endsWith(TaskTracker.getLocalTaskDir(jobId
              .toString(), taskId.toString(), false)));
    }

    // Make sure task task.getJobFile is changed and pointed correctly.
    assertTrue(task.getJobFile().endsWith(
        TaskTracker
            .getTaskConfFile(jobId.toString(), taskId.toString(), false)));

    // Make sure that the tmp directories are created
    assertTrue("tmp dir is not created in workDir "
        + workDir.toUri().getPath(),
        new File(workDir.toUri().getPath(), "tmp").exists());

    // Make sure that the log are setup properly
    File logDir =
        new File(HADOOP_LOG_DIR, TaskLog.USERLOGS_DIR_NAME + Path.SEPARATOR
            + task.getTaskID().toString());
    assertTrue("task's log dir " + logDir.toString() + " doesn't exist!",
        logDir.exists());
    UserGroupInformation ugi = UserGroupInformation.login(localizedJobConf);
    checkFilePermissions(logDir.getAbsolutePath(), "drwx------", ugi
        .getUserName(), ugi.getGroupNames()[0]);

    File expectedStdout = new File(logDir, TaskLog.LogName.STDOUT.toString());
    assertTrue("stdout log file is improper. Expected : "
        + expectedStdout.toString() + " Observed : " + logFiles[0].toString(),
        expectedStdout.toString().equals(logFiles[0].toString()));
    File expectedStderr =
        new File(logDir, Path.SEPARATOR + TaskLog.LogName.STDERR.toString());
    assertTrue("stderr log file is improper. Expected : "
        + expectedStderr.toString() + " Observed : " + logFiles[1].toString(),
        expectedStderr.toString().equals(logFiles[1].toString()));
  }
}

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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JvmManager.JvmEnv;
import org.apache.hadoop.mapred.TaskController.JobInitializationContext;
import org.apache.hadoop.mapred.TaskController.TaskControllerContext;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.mapred.UtilsForTests.InlineCleanupQueue;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.server.tasktracker.Localizer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Shell;

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
  protected UserGroupInformation taskTrackerUGI;
  protected TaskController taskController;
  protected JobConf trackerFConf;
  private JobConf localizedJobConf;
  protected JobID jobId;
  protected TaskAttemptID taskId;
  protected Task task;
  protected String[] localDirs;
  protected static LocalDirAllocator lDirAlloc =
      new LocalDirAllocator("mapred.local.dir");
  protected Path attemptWorkDir;
  protected File[] attemptLogFiles;
  protected JobConf localizedTaskConf;

  /**
   * Dummy method in this base class. Only derived classes will define this
   * method for checking if a test can be run.
   */
  protected boolean canRun() {
    return true;
  }

  @Override
  protected void setUp()
      throws Exception {
    if (!canRun()) {
      return;
    }
    TEST_ROOT_DIR =
        new File(System.getProperty("test.build.data", "/tmp"), getClass()
            .getSimpleName());
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

    // Create the job configuration file. Same as trackerConf in this test.
    JobConf jobConf = trackerFConf;


    // JobClient uploads the job jar to the file system and sets it in the
    // jobConf.
    uploadJobJar(jobConf);

    // JobClient uploads the jobConf to the file system.
    File jobConfFile = uploadJobConf(jobConf);

    // Set up the TaskTracker
    tracker = new TaskTracker();
    tracker.setConf(trackerFConf);

    // for test case system FS is the local FS

    tracker.systemFS = FileSystem.getLocal(trackerFConf);
    tracker.systemDirectory = new Path(TEST_ROOT_DIR.getAbsolutePath());
    tracker.setLocalFileSystem(tracker.systemFS);
    
    taskTrackerUGI = UserGroupInformation.getCurrentUser();

    // Set up the task to be localized
    String jtIdentifier = "200907202331";
    jobId = new JobID(jtIdentifier, 1);
    
    TaskTracker.RunningJob rjob = new TaskTracker.RunningJob(jobId);
    rjob.ugi = UserGroupInformation.getCurrentUser();
    tracker.runningJobs.put(jobId, rjob);
    
    taskId =
        new TaskAttemptID(jtIdentifier, jobId.getId(), true, 1, 0);
    task =
        new MapTask(jobConfFile.toURI().toString(), taskId, 1, null, 1);
    task.setConf(jobConf); // Set conf. Set user name in particular.
    task.setUser(UserGroupInformation.getCurrentUser().getUserName());

    // create jobTokens file
    uploadJobTokensFile();

    taskController = new DefaultTaskController();
    taskController.setConf(trackerFConf);
    taskController.setup();

    tracker.setTaskController(taskController);
    tracker.setLocalizer(new Localizer(tracker.getLocalFileSystem(), localDirs,
                                       taskController));
  }

  /**
   * @param jobConf
   * @throws IOException
   * @throws FileNotFoundException
   */
  private void uploadJobJar(JobConf jobConf)
      throws IOException,
      FileNotFoundException {
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
    jobConf.setJar(jobJarFile.toURI().toString());
  }

  /**
   * @param jobConf
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  protected File uploadJobConf(JobConf jobConf)
      throws FileNotFoundException,
      IOException {
    File jobConfFile = new File(TEST_ROOT_DIR, "jobconf-on-dfs.xml");
    FileOutputStream out = new FileOutputStream(jobConfFile);
    jobConf.writeXml(out);
    out.close();
    return jobConfFile;
   }
  
  /**
   * create fake JobTokens file
   * @return
   * @throws IOException
   */
  protected void uploadJobTokensFile() throws IOException {

    File dir = new File(TEST_ROOT_DIR, jobId.toString());
    if(!dir.exists())
      assertTrue("faild to create dir="+dir.getAbsolutePath(), dir.mkdirs());

    File jobTokenFile = new File(dir, TokenCache.JOB_TOKEN_HDFS_FILE);
    FileOutputStream fos = new FileOutputStream(jobTokenFile);
    java.io.DataOutputStream out = new java.io.DataOutputStream(fos);
    Token<JobTokenIdentifier> jt = new Token<JobTokenIdentifier>();
    jt.write(out); // writing empty file, we don't need the keys for this test
    out.close();
  }

  @Override
  protected void tearDown()
      throws Exception {
    if (!canRun()) {
      return;
    }
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
    assertTrue("Path " + path + " is user owned not by " + expectedOwnerUser
        + " but by " + attrs[1], attrs[1].equals(expectedOwnerUser));
    assertTrue("Path " + path + " is group owned not by " + expectedOwnerGroup
        + " but by " + attrs[2], attrs[2].equals(expectedOwnerGroup));
  }

  /**
   * Verify the task-controller's setup functionality
   * 
   * @throws IOException
   */
  public void testTaskControllerSetup()
      throws IOException {
    if (!canRun()) {
      return;
    }
    // Task-controller is already set up in the test's setup method. Now verify.
    for (String localDir : localDirs) {

      // Verify the local-dir itself.
      File lDir = new File(localDir);
      assertTrue("localDir " + lDir + " doesn't exists!", lDir.exists());
      checkFilePermissions(lDir.getAbsolutePath(), "drwxr-xr-x", task
          .getUser(), taskTrackerUGI.getGroupNames()[0]);
    }

    // Verify the pemissions on the userlogs dir
    File taskLog = TaskLog.getUserLogDir();
    checkFilePermissions(taskLog.getAbsolutePath(), "drwxr-xr-x", task
        .getUser(), taskTrackerUGI.getGroupNames()[0]);
  }

  /**
   * Test the localization of a user on the TT.
   * 
   * @throws IOException
   */
  public void testUserLocalization()
      throws IOException {
    if (!canRun()) {
      return;
    }
    // /////////// The main method being tested
    tracker.getLocalizer().initializeUserDirs(task.getUser());
    // ///////////

    // Check the directory structure and permissions
    checkUserLocalization();

    // For the sake of testing re-entrancy of initializeUserDirs(), we remove
    // the user directories now and make sure that further calls of the method
    // don't create directories any more.
    for (String dir : localDirs) {
      File userDir = new File(dir, TaskTracker.getUserDir(task.getUser()));
      FileUtil.fullyDelete(userDir);
    }

    // Now call the method again.
    tracker.getLocalizer().initializeUserDirs(task.getUser());

    // Files should not be created now and so shouldn't be there anymore.
    for (String dir : localDirs) {
      File userDir = new File(dir, TaskTracker.getUserDir(task.getUser()));
      assertFalse("Unexpectedly, user-dir " + userDir.getAbsolutePath()
          + " exists!", userDir.exists());
    }
  }

  protected void checkUserLocalization()
      throws IOException {
    for (String dir : localDirs) {

      File localDir = new File(dir);
      assertTrue("mapred.local.dir " + localDir + " isn'task created!",
          localDir.exists());

      File taskTrackerSubDir = new File(localDir, TaskTracker.SUBDIR);
      assertTrue("taskTracker sub-dir in the local-dir " + localDir
          + "is not created!", taskTrackerSubDir.exists());

      File userDir = new File(taskTrackerSubDir, task.getUser());
      assertTrue("user-dir in taskTrackerSubdir " + taskTrackerSubDir
          + "is not created!", userDir.exists());
      checkFilePermissions(userDir.getAbsolutePath(), "drwx------", task
          .getUser(), taskTrackerUGI.getGroupNames()[0]);

      File jobCache = new File(userDir, TaskTracker.JOBCACHE);
      assertTrue("jobcache in the userDir " + userDir + " isn't created!",
          jobCache.exists());
      checkFilePermissions(jobCache.getAbsolutePath(), "drwx------", task
          .getUser(), taskTrackerUGI.getGroupNames()[0]);

      // Verify the distributed cache dir.
      File distributedCacheDir =
          new File(localDir, TaskTracker
              .getPrivateDistributedCacheDir(task.getUser()));
      assertTrue("distributed cache dir " + distributedCacheDir
          + " doesn't exists!", distributedCacheDir.exists());
      checkFilePermissions(distributedCacheDir.getAbsolutePath(),
          "drwx------", task.getUser(), taskTrackerUGI.getGroupNames()[0]);
    }
  }

  /**
   * Test job localization on a TT. Tests localization of job.xml, job.jar and
   * corresponding setting of configuration. Also test
   * {@link TaskController#initializeJob(JobInitializationContext)}
   * 
   * @throws IOException
   */
  public void testJobLocalization()
      throws Exception {
    if (!canRun()) {
      return;
    }
    tracker.getLocalizer().initializeUserDirs(task.getUser());

    // /////////// The main method being tested
    localizedJobConf = tracker.localizeJobFiles(task, 
        new TaskTracker.RunningJob(task.getJobID()));
    // ///////////

    // Now initialize the job via task-controller so as to set
    // ownership/permissions of jars, job-work-dir
    JobInitializationContext context = new JobInitializationContext();
    context.jobid = jobId;
    context.user = task.getUser();
    context.workDir =
        new File(localizedJobConf.get(TaskTracker.JOB_LOCAL_DIR));

    // /////////// The method being tested
    taskController.initializeJob(context);
    // ///////////

    checkJobLocalization();
  }

  protected void checkJobLocalization()
      throws IOException {
    // Check the directory structure
    for (String dir : localDirs) {

      File localDir = new File(dir);
      File taskTrackerSubDir = new File(localDir, TaskTracker.SUBDIR);
      File userDir = new File(taskTrackerSubDir, task.getUser());
      File jobCache = new File(userDir, TaskTracker.JOBCACHE);

      File jobDir = new File(jobCache, jobId.toString());
      assertTrue("job-dir in " + jobCache + " isn't created!", jobDir.exists());

      // check the private permissions on the job directory
      checkFilePermissions(jobDir.getAbsolutePath(), "drwx------", task
          .getUser(), taskTrackerUGI.getGroupNames()[0]);
    }

    // check the localization of job.xml
    assertTrue("job.xml is not localized on this TaskTracker!!", lDirAlloc
        .getLocalPathToRead(TaskTracker.getLocalJobConfFile(task.getUser(),
            jobId.toString()), trackerFConf) != null);

    // check the localization of job.jar
    Path jarFileLocalized =
        lDirAlloc.getLocalPathToRead(TaskTracker.getJobJarFile(task.getUser(),
            jobId.toString()), trackerFConf);
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
        .getLocalPathToRead(TaskTracker.getJobWorkDir(task.getUser(), jobId
            .toString()), trackerFConf) != null);

    // Check the setting of job.local.dir and job.jar which will eventually be
    // used by the user's task
    boolean jobLocalDirFlag = false, mapredJarFlag = false;
    String localizedJobLocalDir =
        localizedJobConf.get(TaskTracker.JOB_LOCAL_DIR);
    String localizedJobJar = localizedJobConf.getJar();
    for (String localDir : localizedJobConf.getStrings("mapred.local.dir")) {
      if (localizedJobLocalDir.equals(localDir + Path.SEPARATOR
          + TaskTracker.getJobWorkDir(task.getUser(), jobId.toString()))) {
        jobLocalDirFlag = true;
      }
      if (localizedJobJar.equals(localDir + Path.SEPARATOR
          + TaskTracker.getJobJarFile(task.getUser(), jobId.toString()))) {
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
   */
  public void testTaskLocalization()
      throws Exception {
    if (!canRun()) {
      return;
    }
    tracker.getLocalizer().initializeUserDirs(task.getUser());
    localizedJobConf = tracker.localizeJobFiles(task, 
        new TaskTracker.RunningJob(task.getJobID()));

    // Now initialize the job via task-controller so as to set
    // ownership/permissions of jars, job-work-dir
    JobInitializationContext jobContext = new JobInitializationContext();
    jobContext.jobid = jobId;
    jobContext.user = task.getUser();
    jobContext.workDir =
        new File(localizedJobConf.get(TaskTracker.JOB_LOCAL_DIR));
    taskController.initializeJob(jobContext);

    TaskInProgress tip = tracker.new TaskInProgress(task, trackerFConf);
    tip.setJobConf(localizedJobConf);

    // ////////// The central method being tested
    tip.localizeTask(task);
    // //////////

    // check the functionality of localizeTask
    for (String dir : trackerFConf.getStrings("mapred.local.dir")) {
      File attemptDir =
          new File(dir, TaskTracker.getLocalTaskDir(task.getUser(), jobId
              .toString(), taskId.toString()));
      assertTrue("attempt-dir " + attemptDir + " in localDir " + dir
          + " is not created!!", attemptDir.exists());
    }

    attemptWorkDir =
        lDirAlloc.getLocalPathToRead(TaskTracker.getTaskWorkDir(
            task.getUser(), task.getJobID().toString(), task.getTaskID()
                .toString(), task.isTaskCleanupTask()), trackerFConf);
    assertTrue("atttempt work dir for " + taskId.toString()
        + " is not created in any of the configured dirs!!",
        attemptWorkDir != null);

    TaskRunner runner = task.createRunner(tracker, tip);

    // /////// Few more methods being tested
    runner.setupChildTaskConfiguration(lDirAlloc);
    TaskRunner.createChildTmpDir(new File(attemptWorkDir.toUri().getPath()),
        localizedJobConf);
    attemptLogFiles = TaskRunner.prepareLogFiles(task.getTaskID());

    // Make sure the task-conf file is created
    Path localTaskFile =
        lDirAlloc.getLocalPathToRead(TaskTracker.getTaskConfFile(task
            .getUser(), task.getJobID().toString(), task.getTaskID()
            .toString(), task.isTaskCleanupTask()), trackerFConf);
    assertTrue("Task conf file " + localTaskFile.toString()
        + " is not created!!", new File(localTaskFile.toUri().getPath())
        .exists());

    // /////// One more method being tested. This happens in child space.
    localizedTaskConf = new JobConf(localTaskFile);
    TaskRunner.setupChildMapredLocalDirs(task, localizedTaskConf);
    // ///////

    // Initialize task via TaskController
    TaskControllerContext taskContext =
        new TaskController.TaskControllerContext();
    taskContext.env =
        new JvmEnv(null, null, null, null, -1, new File(localizedJobConf
            .get(TaskTracker.JOB_LOCAL_DIR)), null, localizedJobConf);
    taskContext.task = task;
    // /////////// The method being tested
    taskController.initializeTask(taskContext);
    // ///////////

    checkTaskLocalization();
  }

  protected void checkTaskLocalization()
      throws IOException {
    // Make sure that the mapred.local.dir is sandboxed
    for (String childMapredLocalDir : localizedTaskConf
        .getStrings("mapred.local.dir")) {
      assertTrue("Local dir " + childMapredLocalDir + " is not sandboxed !!",
          childMapredLocalDir.endsWith(TaskTracker.getLocalTaskDir(task
              .getUser(), jobId.toString(), taskId.toString(), false)));
    }

    // Make sure task task.getJobFile is changed and pointed correctly.
    assertTrue(task.getJobFile().endsWith(
        TaskTracker.getTaskConfFile(task.getUser(), jobId.toString(), taskId
            .toString(), false)));

    // Make sure that the tmp directories are created
    assertTrue("tmp dir is not created in workDir "
        + attemptWorkDir.toUri().getPath(), new File(attemptWorkDir.toUri()
        .getPath(), "tmp").exists());

    // Make sure that the logs are setup properly
    File logDir =
        new File(HADOOP_LOG_DIR, TaskLog.USERLOGS_DIR_NAME + Path.SEPARATOR
            + task.getTaskID().toString());
    assertTrue("task's log dir " + logDir.toString() + " doesn't exist!",
        logDir.exists());
    checkFilePermissions(logDir.getAbsolutePath(), "drwx------", task
        .getUser(), taskTrackerUGI.getGroupNames()[0]);

    File expectedStdout = new File(logDir, TaskLog.LogName.STDOUT.toString());
    assertTrue("stdout log file is improper. Expected : "
        + expectedStdout.toString() + " Observed : "
        + attemptLogFiles[0].toString(), expectedStdout.toString().equals(
        attemptLogFiles[0].toString()));
    File expectedStderr =
        new File(logDir, Path.SEPARATOR + TaskLog.LogName.STDERR.toString());
    assertTrue("stderr log file is improper. Expected : "
        + expectedStderr.toString() + " Observed : "
        + attemptLogFiles[1].toString(), expectedStderr.toString().equals(
        attemptLogFiles[1].toString()));
  }

  /**
   * Validates the removal of $taskid and $tasid/work under mapred-local-dir
   * in cases where those directories cannot be deleted without adding
   * write permission to the newly created directories under $taskid and
   * $taskid/work
   * Also see TestSetupWorkDir.createFileAndSetPermissions for details
   */
  void validateRemoveFiles(boolean needCleanup, boolean jvmReuse,
                           TaskInProgress tip) throws IOException {
    // create files and set permissions 555. Verify if task controller sets
    // the permissions for TT to delete the taskDir or workDir
    String dir = (!needCleanup || jvmReuse) ?
        TaskTracker.getTaskWorkDir(task.getUser(), task.getJobID().toString(),
          taskId.toString(), task.isTaskCleanupTask())
      : TaskTracker.getLocalTaskDir(task.getUser(), task.getJobID().toString(),
          taskId.toString(), task.isTaskCleanupTask());

    Path[] paths = tracker.getLocalFiles(localizedJobConf, dir);
    for (Path p : paths) {
      if (tracker.getLocalFileSystem().exists(p)) {
        TestSetupWorkDir.createFileAndSetPermissions(localizedJobConf, p);
      }
    }

    InlineCleanupQueue cleanupQueue = new InlineCleanupQueue();
    tracker.setCleanupThread(cleanupQueue);

    tip.removeTaskFiles(needCleanup, taskId);

    if (jvmReuse) {
      // work dir should still exist and cleanup queue should be empty
      assertTrue("cleanup queue is not empty after removeTaskFiles() in case "
          + "of jvm reuse.", cleanupQueue.isQueueEmpty());
      boolean workDirExists = false;
      for (Path p : paths) {
        if (tracker.getLocalFileSystem().exists(p)) {
          workDirExists = true;
        }
      }
      assertTrue("work dir does not exist in case of jvm reuse", workDirExists);

      // now try to delete the work dir and verify that there are no stale paths
      JvmManager.deleteWorkDir(tracker, task);
    }
    tracker.removeJobFiles(task.getUser(), jobId.toString());

    assertTrue("Some task files are not deleted!! Number of stale paths is "
        + cleanupQueue.stalePaths.size(), cleanupQueue.stalePaths.size() == 0);
  }

  /**
   * Validates if task cleanup is done properly for a succeeded task
   * @throws IOException
   */
  public void testTaskCleanup()
      throws Exception {
    if (!canRun()) {
      return;
    }
    testTaskCleanup(false, false);// no needCleanup; no jvmReuse
  }

  /**
   * Validates if task cleanup is done properly for a task that is not succeeded
   * @throws IOException
   */
  public void testFailedTaskCleanup()
  throws Exception {
    if (!canRun()) {
      return;
    }
    testTaskCleanup(true, false);// needCleanup; no jvmReuse
  }

  /**
   * Validates if task cleanup is done properly for a succeeded task
   * @throws IOException
   */
  public void testTaskCleanupWithJvmUse()
      throws Exception {
    if (!canRun()) {
      return;
    }
    testTaskCleanup(false, true);// no needCleanup; jvmReuse
  }

  /**
   * Validates if task cleanup is done properly
   */
  private void testTaskCleanup(boolean needCleanup, boolean jvmReuse)
      throws Exception {
    // Localize job and localize task.
    tracker.getLocalizer().initializeUserDirs(task.getUser());
    localizedJobConf = tracker.localizeJobFiles(task, 
        new TaskTracker.RunningJob(task.getJobID()));
    if (jvmReuse) {
      localizedJobConf.setNumTasksToExecutePerJvm(2);
    }
    // Now initialize the job via task-controller so as to set
    // ownership/permissions of jars, job-work-dir
    JobInitializationContext jobContext = new JobInitializationContext();
    jobContext.jobid = jobId;
    jobContext.user = localizedJobConf.getUser();
    jobContext.workDir =
        new File(localizedJobConf.get(TaskTracker.JOB_LOCAL_DIR));
    taskController.initializeJob(jobContext);
    TaskInProgress tip = tracker.new TaskInProgress(task, trackerFConf);
    tip.setJobConf(localizedJobConf);
    tip.localizeTask(task);
    Path workDir =
        lDirAlloc.getLocalPathToRead(TaskTracker.getTaskWorkDir(
            task.getUser(), task.getJobID().toString(), task.getTaskID()
                .toString(), task.isTaskCleanupTask()), trackerFConf);
    TaskRunner runner = task.createRunner(tracker, tip);
    tip.setTaskRunner(runner);
    runner.setupChildTaskConfiguration(lDirAlloc);
    TaskRunner.createChildTmpDir(new File(workDir.toUri().getPath()),
        localizedJobConf);
    TaskRunner.prepareLogFiles(task.getTaskID());
    Path localTaskFile =
        lDirAlloc.getLocalPathToRead(TaskTracker.getTaskConfFile(task
            .getUser(), task.getJobID().toString(), task.getTaskID()
            .toString(), task.isTaskCleanupTask()), trackerFConf);
    JobConf localizedTaskConf = new JobConf(localTaskFile);
    TaskRunner.setupChildMapredLocalDirs(task, localizedTaskConf);
    TaskControllerContext taskContext =
        new TaskController.TaskControllerContext();
    taskContext.env =
        new JvmEnv(null, null, null, null, -1, new File(localizedJobConf
            .get(TaskTracker.JOB_LOCAL_DIR)), null, localizedJobConf);
    taskContext.task = task;
    // /////////// The method being tested
    taskController.initializeTask(taskContext);

    // TODO: Let the task run and create files.

    // create files and set permissions 555. Verify if task controller sets
    // the permissions for TT to delete the task dir or work dir properly
    validateRemoveFiles(needCleanup, jvmReuse, tip);

    // Check that the empty $mapred.local.dir/taskTracker/$user dirs are still
    // there.
    for (String localDir : localDirs) {
      Path userDir =
          new Path(localDir, TaskTracker.getUserDir(task.getUser()));
      assertTrue("User directory " + userDir + " is not present!!",
          tracker.getLocalFileSystem().exists(userDir));
    }

    // Test userlogs cleanup.
    verifyUserLogsCleanup();
  }

  /**
   * Test userlogs cleanup.
   * 
   * @throws IOException
   */
  private void verifyUserLogsCleanup()
      throws IOException {
    Path logDir =
        new Path(HADOOP_LOG_DIR.toURI().getPath(), TaskLog.USERLOGS_DIR_NAME
            + Path.SEPARATOR + task.getTaskID().toString());

    // Logs should be there before cleanup.
    assertTrue("Userlogs dir " + logDir + " is not presen as expected!!",
        tracker.getLocalFileSystem().exists(logDir));

    // ////////// Another being tested
    TaskLog.cleanup(-1); // -1 so as to move purgeTimeStamp to future and file
    // modification time behind retainTimeStatmp
    // //////////

    // Logs should be gone after cleanup.
    assertFalse("Userlogs dir " + logDir + " is not deleted as expected!!",
        tracker.getLocalFileSystem().exists(logDir));
  }
}

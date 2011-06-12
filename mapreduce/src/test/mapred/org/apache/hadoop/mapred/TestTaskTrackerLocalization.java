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
import java.io.PrintWriter;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.server.tasktracker.Localizer;
import org.apache.hadoop.mapreduce.util.MRAsyncDiskService;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.mapred.JvmManager.JvmEnv;
import org.apache.hadoop.mapred.TaskController.JobInitializationContext;
import org.apache.hadoop.mapred.TaskController.TaskControllerContext;
import org.apache.hadoop.mapred.TaskTracker.RunningJob;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.mapred.UtilsForTests.InlineCleanupQueue;

import junit.framework.TestCase;

/**
 * Test to verify localization of a job and localization of a task on a
 * TaskTracker.
 * 
 */
public class TestTaskTrackerLocalization extends TestCase {

  private static File TEST_ROOT_DIR = 
    new File(System.getProperty("test.build.data", "/tmp"));
  private File ROOT_MAPRED_LOCAL_DIR;
  private File HADOOP_LOG_DIR;
  private static File PERMISSION_SCRIPT_DIR;
  private static File PERMISSION_SCRIPT_FILE;
  private static final String PERMISSION_SCRIPT_CONTENT = "ls -l -d $1 | " +
  		"awk '{print $1\":\"$3\":\"$4}'";

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
      new LocalDirAllocator(MRConfig.LOCAL_DIR);
  protected Path attemptWorkDir;
  protected File[] attemptLogFiles;
  protected JobConf localizedTaskConf;
  private TaskInProgress tip;
  private JobConf jobConf;
  private File jobConfFile;

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
    trackerFConf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    localDirs = new String[numLocalDirs];
    for (int i = 0; i < numLocalDirs; i++) {
      localDirs[i] = new File(ROOT_MAPRED_LOCAL_DIR, "0_" + i).getPath();
    }
    trackerFConf.setStrings(MRConfig.LOCAL_DIR, localDirs);

    // Create the job configuration file. Same as trackerConf in this test.
    jobConf = new JobConf(trackerFConf);
    // Set job view ACLs in conf sothat validation of contents of jobACLsFile
    // can be done against this value. Have both users and groups
    String jobViewACLs = "user1,user2, group1,group2";
    jobConf.set(MRJobConfig.JOB_ACL_VIEW_JOB, jobViewACLs);

    jobConf.setInt(MRJobConfig.USER_LOG_RETAIN_HOURS, 0);
    jobConf.setUser(getJobOwner().getShortUserName());

    Job job = new Job(jobConf);
    String jtIdentifier = "200907202331";
    jobId = new JobID(jtIdentifier, 1);

    // JobClient uploads the job jar to the file system and sets it in the
    // jobConf.
    uploadJobJar(job);

    // JobClient uploads the jobConf to the file system.
    jobConfFile = uploadJobConf(job.getConfiguration());
    
    // create jobTokens file
    uploadJobTokensFile(); 
    
    taskTrackerUGI = UserGroupInformation.getCurrentUser();
    startTracker();

    // Set up the task to be localized
    taskId =
        new TaskAttemptID(jtIdentifier, jobId.getId(), TaskType.MAP, 1, 0);
    createTask();

    // mimic register task
    // create the tip
    tip = tracker.new TaskInProgress(task, trackerFConf);
  }

  private void startTracker() throws IOException {
    // Set up the TaskTracker
    tracker = new TaskTracker();
    tracker.setConf(trackerFConf);
    tracker.setTaskLogCleanupThread(new UserLogCleaner(trackerFConf));
    initializeTracker();
  }

  private void initializeTracker() throws IOException {
    tracker.setIndexCache(new IndexCache(trackerFConf));
    tracker.setTaskMemoryManagerEnabledFlag();

    // for test case system FS is the local FS
    tracker.systemFS = FileSystem.getLocal(trackerFConf);
    tracker.setLocalFileSystem(tracker.systemFS);
    tracker.systemDirectory = new Path(TEST_ROOT_DIR.getAbsolutePath());

    tracker.runningTasks = new LinkedHashMap<TaskAttemptID, TaskInProgress>();
    tracker.runningJobs = new TreeMap<JobID, RunningJob>();
    tracker.setAsyncDiskService(new MRAsyncDiskService(trackerFConf));
    tracker.getAsyncDiskService().cleanupAllVolumes();

    // setup task controller
    taskController = createTaskController();
    taskController.setConf(trackerFConf);
    taskController.setup();
    tracker.setTaskController(taskController);
    tracker.setLocalizer(new Localizer(tracker.getLocalFileSystem(), localDirs,
        taskController));
  }

  protected TaskController createTaskController() {
    return new DefaultTaskController();
  }

  private void createTask()
      throws IOException {
    task = new MapTask(jobConfFile.toURI().toString(), taskId, 1, null, 1);
    task.setConf(jobConf); // Set conf. Set user name in particular.
    task.setUser(jobConf.getUser());
  }

  protected UserGroupInformation getJobOwner() throws IOException {
    return UserGroupInformation.getCurrentUser();
  }

  /**
   * static block setting up the permission script which would be used by the 
   * checkFilePermissions
   */
  static {
    PERMISSION_SCRIPT_DIR = new File(TEST_ROOT_DIR, "permission_script_dir");
    PERMISSION_SCRIPT_FILE = new File(PERMISSION_SCRIPT_DIR, "getperms.sh");
    
    if(PERMISSION_SCRIPT_FILE.exists()) {
      PERMISSION_SCRIPT_FILE.delete();
    }
    
    if(PERMISSION_SCRIPT_DIR.exists()) {
      PERMISSION_SCRIPT_DIR.delete();
    }
    
    PERMISSION_SCRIPT_DIR.mkdir();
    
    try {
      PrintWriter writer = new PrintWriter(PERMISSION_SCRIPT_FILE);
      writer.write(PERMISSION_SCRIPT_CONTENT);
      writer.close();
    } catch (FileNotFoundException fe) {
      fail();
    }
    PERMISSION_SCRIPT_FILE.setExecutable(true, true);
  }

  /**
   * @param job
   * @throws IOException
   * @throws FileNotFoundException
   */
  private void uploadJobJar(Job job)
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
    job.setJar(jobJarFile.toURI().toString());
  }

  /**
   * @param conf
   * @return
   * @throws FileNotFoundException
   * @throws IOException
   */
  protected File uploadJobConf(Configuration conf)
      throws FileNotFoundException,
      IOException {
    File jobConfFile = new File(TEST_ROOT_DIR, "jobconf-on-dfs.xml");
    FileOutputStream out = new FileOutputStream(jobConfFile);
    conf.writeXml(out);
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
    jt.write(out); // writing empty file, we don't the keys for this test 
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
    String[] command = {"bash",PERMISSION_SCRIPT_FILE.getAbsolutePath(), path};
    String output=Shell.execCommand(command);
    return output.split(":|\n");
  }


  /**
   * Utility method to check permission of a given path. Requires the permission
   * script directory to be setup in order to call.
   * 
   * 
   * @param path
   * @param expectedPermissions
   * @param expectedOwnerUser
   * @param expectedOwnerGroup
   * @throws IOException
   */
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
      if (!FileUtil.fullyDelete(userDir)) {
        throw new IOException("Uanble to delete " + userDir);
      }
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
      assertTrue(MRConfig.LOCAL_DIR + localDir + " isn'task created!",
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
    TaskTracker.RunningJob rjob = tracker.localizeJob(tip);
    localizedJobConf = rjob.getJobConf();

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

    // Check the setting of mapreduce.job.local.dir and job.jar which will eventually be
    // used by the user's task
    boolean jobLocalDirFlag = false, mapredJarFlag = false;
    String localizedJobLocalDir =
        localizedJobConf.get(TaskTracker.JOB_LOCAL_DIR);
    String localizedJobJar = localizedJobConf.getJar();
    for (String localDir : localizedJobConf.getStrings(MRConfig.LOCAL_DIR)) {
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
        "mapreduce.job.jar is not set properly to the target users directory : "
            + localizedJobJar, mapredJarFlag);

    // check job user-log directory permissions
    File jobLogDir = TaskLog.getJobDir(jobId);
    assertTrue("job log directory " + jobLogDir + " does not exist!", jobLogDir
        .exists());
    checkFilePermissions(jobLogDir.toString(), "drwx------", task.getUser(),
        taskTrackerUGI.getGroupNames()[0]);
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
    TaskTracker.RunningJob rjob = tracker.localizeJob(tip);
    localizedJobConf = rjob.getJobConf();
    initializeTask();

    checkTaskLocalization();
  }

  private void initializeTask() throws IOException {
    tip.setJobConf(localizedJobConf);

    // ////////// The central method being tested
    tip.localizeTask(task);
    // //////////

    // check the functionality of localizeTask
    for (String dir : trackerFConf.getStrings(MRConfig.LOCAL_DIR)) {
      File attemptDir =
          new File(dir, TaskTracker.getLocalTaskDir(task.getUser(), jobId
              .toString(), taskId.toString(), task.isTaskCleanupTask()));
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
    tip.setTaskRunner(runner);

    // /////// Few more methods being tested
    runner.setupChildTaskConfiguration(lDirAlloc);
    TaskRunner.createChildTmpDir(new File(attemptWorkDir.toUri().getPath()),
        localizedJobConf);
    attemptLogFiles = runner.prepareLogFiles(task.getTaskID(),
        task.isTaskCleanupTask());

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
  }

  protected void checkTaskLocalization()
      throws IOException {
    // Make sure that the mapreduce.cluster.local.dir is sandboxed
    for (String childMapredLocalDir : localizedTaskConf
        .getStrings(MRConfig.LOCAL_DIR)) {
      assertTrue("Local dir " + childMapredLocalDir + " is not sandboxed !!",
          childMapredLocalDir.endsWith(TaskTracker.getLocalTaskDir(task
              .getUser(), jobId.toString(), taskId.toString(),
              task.isTaskCleanupTask())));
    }

    // Make sure task task.getJobFile is changed and pointed correctly.
    assertTrue(task.getJobFile().endsWith(
        TaskTracker.getTaskConfFile(task.getUser(), jobId.toString(), taskId
            .toString(), task.isTaskCleanupTask())));

    // Make sure that the tmp directories are created
    assertTrue("tmp dir is not created in workDir "
        + attemptWorkDir.toUri().getPath(), new File(attemptWorkDir.toUri()
        .getPath(), "tmp").exists());

    // Make sure that the logs are setup properly
    File logDir = TaskLog.getAttemptDir(taskId, task.isTaskCleanupTask());
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

    // Make sure that the job ACLs file exists in the task log dir
    File jobACLsFile = new File(logDir, TaskRunner.jobACLsFile);
    assertTrue("JobACLsFile is missing in the task log dir " + logDir,
        jobACLsFile.exists());

    // With default task controller, the job-acls file is owned by TT and
    // permissions are 700
    checkFilePermissions(jobACLsFile.getAbsolutePath(), "-rwx------",
        taskTrackerUGI.getShortUserName(), taskTrackerUGI.getGroupNames()[0]);

    // Validate the contents of jobACLsFile(both user name and job-view-acls)
    Configuration jobACLsConf = TaskLogServlet.getConfFromJobACLsFile(task
        .getTaskID(), task.isTaskCleanupTask());
    assertTrue(jobACLsConf.get(MRJobConfig.USER_NAME).equals(
        localizedJobConf.getUser()));
    assertTrue(jobACLsConf.get(MRJobConfig.JOB_ACL_VIEW_JOB).
        equals(localizedJobConf.get(MRJobConfig.JOB_ACL_VIEW_JOB)));
  }

  /**
   * Create a file in the given dir and set permissions r_xr_xr_x sothat no one
   * can delete it directly(without doing chmod).
   * Creates dir/subDir and dir/subDir/file
   */
  static void createFileAndSetPermissions(JobConf jobConf, Path dir)
       throws IOException {
    Path subDir = new Path(dir, "subDir");
    FileSystem fs = FileSystem.getLocal(jobConf);
    fs.mkdirs(subDir);
    Path p = new Path(subDir, "file");
    java.io.DataOutputStream out = fs.create(p);
    out.writeBytes("dummy input");
    out.close();
    // no write permission for subDir and subDir/file
    try {
      int ret = 0;
      if((ret = FileUtil.chmod(subDir.toUri().getPath(), "a=rx", true)) != 0) {
        LOG.warn("chmod failed for " + subDir + ";retVal=" + ret);
      }
    } catch(InterruptedException e) {
      LOG.warn("Interrupted while doing chmod for " + subDir);
    }
  }

  /**
   * Validates the removal of $taskid and $tasid/work under mapred-local-dir
   * in cases where those directories cannot be deleted without adding
   * write permission to the newly created directories under $taskid and
   * $taskid/work
   * Also see createFileAndSetPermissions for details
   */
  void validateRemoveTaskFiles(boolean needCleanup, boolean jvmReuse,
                           TaskInProgress tip) throws IOException {
    // create files and set permissions 555. Verify if task controller sets
    // the permissions for TT to delete the taskDir or workDir
    String dir = (!needCleanup || jvmReuse) ?
        TaskTracker.getTaskWorkDir(task.getUser(), task.getJobID().toString(),
          taskId.toString(), task.isTaskCleanupTask())
      : TaskTracker.getLocalTaskDir(task.getUser(), task.getJobID().toString(),
          taskId.toString(), task.isTaskCleanupTask());

    Path[] paths = tracker.getLocalFiles(localizedJobConf, dir);
    assertTrue("No paths found", paths.length > 0);
    for (Path p : paths) {
      if (tracker.getLocalFileSystem().exists(p)) {
        createFileAndSetPermissions(localizedJobConf, p);
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

    assertTrue("Some task files are not deleted!! Number of stale paths is "
        + cleanupQueue.stalePaths.size(), cleanupQueue.stalePaths.size() == 0);
  }

  /**
   * Validates if task cleanup is done properly for a succeeded task
   * @throws IOException
   */
  public void testTaskFilesRemoval()
      throws Exception {
    if (!canRun()) {
      return;
    }
    testTaskFilesRemoval(false, false);// no needCleanup; no jvmReuse
  }

  /**
   * Validates if task cleanup is done properly for a task that is not succeeded
   * @throws IOException
   */
  public void testFailedTaskFilesRemoval()
  throws Exception {
    if (!canRun()) {
      return;
    }
    testTaskFilesRemoval(true, false);// needCleanup; no jvmReuse

    // initialize a cleanupAttempt for the task.
    task.setTaskCleanupTask();
    // localize task cleanup attempt
    initializeTask();
    checkTaskLocalization();

    // verify the cleanup of cleanup attempt.
    testTaskFilesRemoval(true, false);// needCleanup; no jvmReuse
  }

  /**
   * Validates if task cleanup is done properly for a succeeded task
   * @throws IOException
   */
  public void testTaskFilesRemovalWithJvmUse()
      throws Exception {
    if (!canRun()) {
      return;
    }
    testTaskFilesRemoval(false, true);// no needCleanup; jvmReuse
  }

  /**
   * Validates if task cleanup is done properly
   */
  private void testTaskFilesRemoval(boolean needCleanup, boolean jvmReuse)
      throws Exception {
    // Localize job and localize task.
    TaskTracker.RunningJob rjob = tracker.localizeJob(tip);
    localizedJobConf = rjob.getJobConf();
    if (jvmReuse) {
      localizedJobConf.setNumTasksToExecutePerJvm(2);
    }
    initializeTask();

    // TODO: Let the task run and create files.

    // create files and set permissions 555. Verify if task controller sets
    // the permissions for TT to delete the task dir or work dir properly
    validateRemoveTaskFiles(needCleanup, jvmReuse, tip);
  }

  /**
   * Test userlogs cleanup.
   * 
   * @throws IOException
   */
  private void verifyUserLogsRemoval()
      throws IOException {
    // verify user logs cleanup
    File jobUserLogDir = TaskLog.getJobDir(jobId);
    // Logs should be there before cleanup.
    assertTrue("Userlogs dir " + jobUserLogDir + " is not present as expected!!",
          jobUserLogDir.exists());
    tracker.purgeJob(new KillJobAction(jobId));
    tracker.getTaskLogCleanupThread().processCompletedJobs();
    
    // Logs should be gone after cleanup.
    assertFalse("Userlogs dir " + jobUserLogDir + " is not deleted as expected!!",
        jobUserLogDir.exists());
  }
  
  /**
   * Test job cleanup by doing the following
   *   - create files with no write permissions to TT under job-work-dir
   *   - create files with no write permissions to TT under task-work-dir
   */
  public void testJobFilesRemoval() throws IOException, InterruptedException {
    if (!canRun()) {
      return;
    }
    
    LOG.info("Running testJobCleanup()");
    // Localize job and localize task.
    TaskTracker.RunningJob rjob = tracker.localizeJob(tip);
    localizedJobConf = rjob.getJobConf();
    
    // Set an inline cleanup queue
    InlineCleanupQueue cleanupQueue = new InlineCleanupQueue();
    tracker.setCleanupThread(cleanupQueue);
    
    // Create a file in job's work-dir with 555
    String jobWorkDir = 
      TaskTracker.getJobWorkDir(task.getUser(), task.getJobID().toString());
    Path[] jPaths = tracker.getLocalFiles(localizedJobConf, jobWorkDir);
    assertTrue("No paths found for job", jPaths.length > 0);
    for (Path p : jPaths) {
      if (tracker.getLocalFileSystem().exists(p)) {
        createFileAndSetPermissions(localizedJobConf, p);
      }
    }
    
    // Initialize task dirs
    tip.setJobConf(localizedJobConf);
    tip.localizeTask(task);
    
    // Create a file in task local dir with 555
    // this is to simply test the case where the jvm reuse is enabled and some
    // files in task-attempt-local-dir are left behind to be cleaned up when the
    // job finishes.
    String taskLocalDir = 
      TaskTracker.getLocalTaskDir(task.getUser(), task.getJobID().toString(), 
                                  task.getTaskID().toString(), false);
    Path[] tPaths = tracker.getLocalFiles(localizedJobConf, taskLocalDir);
    assertTrue("No paths found for task", tPaths.length > 0);
    for (Path p : tPaths) {
      if (tracker.getLocalFileSystem().exists(p)) {
        createFileAndSetPermissions(localizedJobConf, p);
      }
    }
    
    // remove the job work dir
    tracker.removeJobFiles(task.getUser(), task.getJobID());

    // check the task-local-dir
    boolean tLocalDirExists = false;
    for (Path p : tPaths) {
      if (tracker.getLocalFileSystem().exists(p)) {
        tLocalDirExists = true;
      }
    }
    assertFalse("Task " + task.getTaskID() + " local dir exists after cleanup", 
                tLocalDirExists);
    
    // Verify that the TaskTracker (via the task-controller) cleans up the dirs.
    // check the job-work-dir
    boolean jWorkDirExists = false;
    for (Path p : jPaths) {
      if (tracker.getLocalFileSystem().exists(p)) {
        jWorkDirExists = true;
      }
    }
    assertFalse("Job " + task.getJobID() + " work dir exists after cleanup", 
                jWorkDirExists);
    // Test userlogs cleanup.
    verifyUserLogsRemoval();

    // Check that the empty $mapred.local.dir/taskTracker/$user dirs are still
    // there.
    for (String localDir : localDirs) {
      Path userDir =
          new Path(localDir, TaskTracker.getUserDir(task.getUser()));
      assertTrue("User directory " + userDir + " is not present!!",
          tracker.getLocalFileSystem().exists(userDir));
    }
  }
  
  /**
   * Tests TaskTracker restart after the localization.
   * 
   * This tests the following steps:
   * 
   * Localize Job, initialize a task.
   * Then restart the Tracker.
   * launch a cleanup attempt for the task.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  public void testTrackerRestart() throws IOException, InterruptedException {
    if (!canRun()) {
      return;
    }

    // Localize job and localize task.
    TaskTracker.RunningJob rjob = tracker.localizeJob(tip);
    localizedJobConf = rjob.getJobConf();
    initializeTask();
    
    // imitate tracker restart
    startTracker();
    
    // create a task cleanup attempt
    createTask();
    task.setTaskCleanupTask();
    // register task
    tip = tracker.new TaskInProgress(task, trackerFConf);

    // localize the job again.
    rjob = tracker.localizeJob(tip);
    localizedJobConf = rjob.getJobConf();
    checkJobLocalization();
    
    // localize task cleanup attempt
    initializeTask();
    checkTaskLocalization();
  }
  
  /**
   * Tests TaskTracker re-init after the localization.
   * 
   * This tests the following steps:
   * 
   * Localize Job, initialize a task.
   * Then reinit the Tracker.
   * launch a cleanup attempt for the task.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  public void testTrackerReinit() throws IOException, InterruptedException {
    if (!canRun()) {
      return;
    }

    // Localize job and localize task.
    TaskTracker.RunningJob rjob = tracker.localizeJob(tip);
    localizedJobConf = rjob.getJobConf();
    initializeTask();
    
    // imitate tracker reinit
    initializeTracker();
    
    // create a task cleanup attempt
    createTask();
    task.setTaskCleanupTask();
    // register task
    tip = tracker.new TaskInProgress(task, trackerFConf);

    // localize the job again.
    rjob = tracker.localizeJob(tip);
    localizedJobConf = rjob.getJobConf();
    checkJobLocalization();
    
    // localize task cleanup attempt
    initializeTask();
    checkTaskLocalization();
  }

  /**
   * Localizes a cleanup task and validates permissions.
   * 
   * @throws InterruptedException 
   * @throws IOException 
   */
  public void testCleanupTaskLocalization() throws IOException,
      InterruptedException {
    if (!canRun()) {
      return;
    }

    task.setTaskCleanupTask();
    // register task
    tip = tracker.new TaskInProgress(task, trackerFConf);

    // localize the job.
    RunningJob rjob = tracker.localizeJob(tip);
    localizedJobConf = rjob.getJobConf();
    checkJobLocalization();

    // localize task cleanup attempt
    initializeTask();
    checkTaskLocalization();

  }
}

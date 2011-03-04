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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TaskController.JobInitializationContext;
import org.apache.hadoop.mapred.TaskController.TaskControllerContext;
import org.apache.hadoop.mapred.TaskTracker.TaskInProgress;
import org.apache.hadoop.mapred.ClusterWithLinuxTaskController.MyLinuxTaskController;
import org.apache.hadoop.mapred.JvmManager.JvmEnv;

/**
 * Test to verify localization of a job and localization of a task on a
 * TaskTracker when {@link LinuxTaskController} is used.
 * 
 */
public class TestLocalizationWithLinuxTaskController extends
    TestTaskTrackerLocalization {

  private static final Log LOG =
      LogFactory.getLog(TestLocalizationWithLinuxTaskController.class);

  private File configFile;
  private MyLinuxTaskController taskController;

  private static String taskTrackerSpecialGroup;

  @Override
  protected void setUp()
      throws Exception {

    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }

    super.setUp();

    taskController = new MyLinuxTaskController();
    String path =
        System.getProperty(ClusterWithLinuxTaskController.TASKCONTROLLER_PATH);
    configFile =
        ClusterWithLinuxTaskController.createTaskControllerConf(path,
            localDirs);
    String execPath = path + "/task-controller";
    taskController.setTaskControllerExe(execPath);
    taskTrackerSpecialGroup = getFilePermissionAttrs(execPath)[2];
    taskController.setConf(trackerFConf);
    taskController.setup();
  }

  @Override
  protected void tearDown()
      throws Exception {
    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }
    super.tearDown();
    if (configFile != null) {
      configFile.delete();
    }
  }

  /** @InheritDoc */
  @Override
  public void testTaskControllerSetup() {
    // Do nothing.
  }

  /**
   * Test job localization with {@link LinuxTaskController}. Also check the
   * permissions and file ownership of the job related files.
   */
  @Override
  public void testJobLocalization()
      throws IOException,
      LoginException {

    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }

    // Do job localization
    JobConf localizedJobConf = tracker.localizeJobFiles(task);

    String ugi =
        System.getProperty(ClusterWithLinuxTaskController.TASKCONTROLLER_UGI);
    localizedJobConf.setUser(ugi.split(",")[0]);

    // Now initialize the job via task-controller so as to set
    // ownership/permissions of jars, job-work-dir
    JobInitializationContext context = new JobInitializationContext();
    context.jobid = jobId;
    context.user = localizedJobConf.getUser();
    context.workDir =
        new File(localizedJobConf.get(TaskTracker.JOB_LOCAL_DIR));

    // /////////// The method being tested
    taskController.initializeJob(context);
    // ///////////

    for (String localDir : trackerFConf.getStrings("mapred.local.dir")) {
      File jobDir =
          new File(localDir, TaskTracker.getLocalJobDir(jobId.toString()));
      // check the private permissions on the job directory
      checkFilePermissions(jobDir.getAbsolutePath(), "dr-xrws---",
          localizedJobConf.getUser(), taskTrackerSpecialGroup);
    }

    // check the private permissions of various directories
    List<Path> dirs = new ArrayList<Path>();
    Path jarsDir =
        lDirAlloc.getLocalPathToRead(TaskTracker.getJobJarsDir(jobId
            .toString()), trackerFConf);
    dirs.add(jarsDir);
    dirs.add(new Path(jarsDir, "lib"));
    for (Path dir : dirs) {
      checkFilePermissions(dir.toUri().getPath(), "dr-xrws---",
          localizedJobConf.getUser(), taskTrackerSpecialGroup);
    }

    // job-work dir needs user writable permissions
    Path jobWorkDir =
        lDirAlloc.getLocalPathToRead(TaskTracker.getJobWorkDir(jobId
            .toString()), trackerFConf);
    checkFilePermissions(jobWorkDir.toUri().getPath(), "drwxrws---",
        localizedJobConf.getUser(), taskTrackerSpecialGroup);

    // check the private permissions of various files
    List<Path> files = new ArrayList<Path>();
    files.add(lDirAlloc.getLocalPathToRead(TaskTracker
        .getLocalJobConfFile(jobId.toString()), trackerFConf));
    files.add(lDirAlloc.getLocalPathToRead(TaskTracker.getJobJarFile(jobId
        .toString()), trackerFConf));
    files.add(new Path(jarsDir, "lib" + Path.SEPARATOR + "lib1.jar"));
    files.add(new Path(jarsDir, "lib" + Path.SEPARATOR + "lib2.jar"));
    for (Path file : files) {
      checkFilePermissions(file.toUri().getPath(), "-r-xrwx---",
          localizedJobConf.getUser(), taskTrackerSpecialGroup);
    }
  }

  /**
   * Test task localization with {@link LinuxTaskController}. Also check the
   * permissions and file ownership of task related files.
   */
  @Override
  public void testTaskLocalization()
      throws IOException,
      LoginException {

    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }

    JobConf localizedJobConf = tracker.localizeJobFiles(task);
    String ugi =
        System.getProperty(ClusterWithLinuxTaskController.TASKCONTROLLER_UGI);
    localizedJobConf.setUser(ugi.split(",")[0]);

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

    // localize the task.
    tip.localizeTask(task);
    TaskRunner runner = task.createRunner(tracker, tip);
    runner.setupChildTaskConfiguration(lDirAlloc);
    Path workDir =
        lDirAlloc.getLocalPathToRead(TaskTracker.getTaskWorkDir(task
            .getJobID().toString(), task.getTaskID().toString(), task
            .isTaskCleanupTask()), trackerFConf);
    TaskRunner.createChildTmpDir(new File(workDir.toUri().getPath()),
        localizedJobConf);
    File[] logFiles = TaskRunner.prepareLogFiles(task.getTaskID());

    // Initialize task
    TaskControllerContext taskContext =
        new TaskController.TaskControllerContext();
    taskContext.env =
        new JvmEnv(null, null, null, null, -1, new File(localizedJobConf
            .get(TaskTracker.JOB_LOCAL_DIR)), null, localizedJobConf);
    taskContext.task = task;
    // /////////// The method being tested
    taskController.initializeTask(taskContext);
    // ///////////

    // check the private permissions of various directories
    List<Path> dirs = new ArrayList<Path>();
    dirs.add(lDirAlloc.getLocalPathToRead(TaskTracker.getLocalTaskDir(jobId
        .toString(), taskId.toString()), trackerFConf));
    dirs.add(workDir);
    dirs.add(new Path(workDir, "tmp"));
    dirs.add(new Path(logFiles[1].getParentFile().getAbsolutePath()));
    for (Path dir : dirs) {
      checkFilePermissions(dir.toUri().getPath(), "drwxrws---",
          localizedJobConf.getUser(), taskTrackerSpecialGroup);
    }

    // check the private permissions of various files
    List<Path> files = new ArrayList<Path>();
    files.add(lDirAlloc.getLocalPathToRead(TaskTracker.getTaskConfFile(task
        .getJobID().toString(), task.getTaskID().toString(), task
        .isTaskCleanupTask()), trackerFConf));
    for (Path file : files) {
      checkFilePermissions(file.toUri().getPath(), "-rwxrwx---",
          localizedJobConf.getUser(), taskTrackerSpecialGroup);
    }
  }
}

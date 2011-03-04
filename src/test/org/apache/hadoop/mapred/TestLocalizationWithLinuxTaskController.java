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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterWithLinuxTaskController.MyLinuxTaskController;
import org.apache.hadoop.mapreduce.server.tasktracker.Localizer;

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
    ((MyLinuxTaskController) taskController).setTaskControllerExe(execPath);
    taskTrackerSpecialGroup = getFilePermissionAttrs(execPath)[2];
    taskController.setConf(trackerFConf);
    taskController.setup();

    tracker.setLocalizer(new Localizer(tracker.localFs, localDirs,
        taskController));

    // Rewrite conf so as to reflect task's correct user name.
    String ugi =
        System.getProperty(ClusterWithLinuxTaskController.TASKCONTROLLER_UGI);
    JobConf jobConf = new JobConf(task.getConf());
    jobConf.setUser(ugi.split(",")[0]);
    File jobConfFile = uploadJobConf(jobConf);
    // Create the task again to change the job-user
    task =
      new MapTask(jobConfFile.toURI().toString(), taskId, 1, null, 1);
    task.setConf(jobConf);
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
   * Test the localization of a user on the TT when {@link LinuxTaskController}
   * is in use.
   */
  @Override
  public void testUserLocalization()
      throws IOException {

    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }

    super.testJobLocalization();
  }

  @Override
  protected void checkUserLocalization()
      throws IOException {
    // Check the directory structure and permissions
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
      checkFilePermissions(userDir.getAbsolutePath(), "dr-xrws---", task
          .getUser(), taskTrackerSpecialGroup);

      File jobCache = new File(userDir, TaskTracker.JOBCACHE);
      assertTrue("jobcache in the userDir " + userDir + " isn't created!",
          jobCache.exists());
      checkFilePermissions(jobCache.getAbsolutePath(), "dr-xrws---", task
          .getUser(), taskTrackerSpecialGroup);

      // Verify the distributed cache dir.
      File distributedCacheDir =
          new File(localDir, TaskTracker
              .getDistributedCacheDir(task.getUser()));
      assertTrue("distributed cache dir " + distributedCacheDir
          + " doesn't exists!", distributedCacheDir.exists());
      checkFilePermissions(distributedCacheDir.getAbsolutePath(),
          "dr-xrws---", task.getUser(), taskTrackerSpecialGroup);
    }
  }

  /**
   * Test job localization with {@link LinuxTaskController}. Also check the
   * permissions and file ownership of the job related files.
   */
  @Override
  public void testJobLocalization()
      throws IOException {

    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }

    super.testJobLocalization();
  }

  @Override
  protected void checkJobLocalization()
      throws IOException {
    for (String localDir : trackerFConf.getStrings("mapred.local.dir")) {
      File jobDir =
          new File(localDir, TaskTracker.getLocalJobDir(task.getUser(), jobId
              .toString()));
      // check the private permissions on the job directory
      checkFilePermissions(jobDir.getAbsolutePath(), "dr-xrws---", task
          .getUser(), taskTrackerSpecialGroup);
    }

    // check the private permissions of various directories
    List<Path> dirs = new ArrayList<Path>();
    Path jarsDir =
        lDirAlloc.getLocalPathToRead(TaskTracker.getJobJarsDir(task.getUser(),
            jobId.toString()), trackerFConf);
    dirs.add(jarsDir);
    dirs.add(new Path(jarsDir, "lib"));
    for (Path dir : dirs) {
      checkFilePermissions(dir.toUri().getPath(), "dr-xrws---",
          task.getUser(), taskTrackerSpecialGroup);
    }

    // job-work dir needs user writable permissions
    Path jobWorkDir =
        lDirAlloc.getLocalPathToRead(TaskTracker.getJobWorkDir(task.getUser(),
            jobId.toString()), trackerFConf);
    checkFilePermissions(jobWorkDir.toUri().getPath(), "drwxrws---", task
        .getUser(), taskTrackerSpecialGroup);

    // check the private permissions of various files
    List<Path> files = new ArrayList<Path>();
    files.add(lDirAlloc.getLocalPathToRead(TaskTracker.getLocalJobConfFile(
        task.getUser(), jobId.toString()), trackerFConf));
    files.add(lDirAlloc.getLocalPathToRead(TaskTracker.getJobJarFile(task
        .getUser(), jobId.toString()), trackerFConf));
    files.add(new Path(jarsDir, "lib" + Path.SEPARATOR + "lib1.jar"));
    files.add(new Path(jarsDir, "lib" + Path.SEPARATOR + "lib2.jar"));
    for (Path file : files) {
      checkFilePermissions(file.toUri().getPath(), "-r-xrwx---", task
          .getUser(), taskTrackerSpecialGroup);
    }
  }

  /**
   * Test task localization with {@link LinuxTaskController}. Also check the
   * permissions and file ownership of task related files.
   */
  @Override
  public void testTaskLocalization()
      throws IOException {

    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }

    super.testTaskLocalization();
  }

  @Override
  protected void checkTaskLocalization()
      throws IOException {
    // check the private permissions of various directories
    List<Path> dirs = new ArrayList<Path>();
    dirs.add(lDirAlloc.getLocalPathToRead(TaskTracker.getLocalTaskDir(task
        .getUser(), jobId.toString(), taskId.toString()), trackerFConf));
    dirs.add(attemptWorkDir);
    dirs.add(new Path(attemptWorkDir, "tmp"));
    dirs.add(new Path(attemptLogFiles[1].getParentFile().getAbsolutePath()));
    for (Path dir : dirs) {
      checkFilePermissions(dir.toUri().getPath(), "drwxrws---",
          task.getUser(), taskTrackerSpecialGroup);
    }

    // check the private permissions of various files
    List<Path> files = new ArrayList<Path>();
    files.add(lDirAlloc.getLocalPathToRead(TaskTracker.getTaskConfFile(task
        .getUser(), task.getJobID().toString(), task.getTaskID().toString(),
        task.isTaskCleanupTask()), trackerFConf));
    for (Path file : files) {
      checkFilePermissions(file.toUri().getPath(), "-rwxrwx---", task
          .getUser(), taskTrackerSpecialGroup);
    }
  }

  /**
   * Test cleanup of task files with {@link LinuxTaskController}.
   */
  @Override
  public void testTaskCleanup()
      throws IOException {
    if (!ClusterWithLinuxTaskController.shouldRun()) {
      return;
    }
    super.testTaskCleanup();
  }
}

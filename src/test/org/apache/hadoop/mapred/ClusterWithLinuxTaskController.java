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
import java.io.PrintWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UnixUserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation;

import junit.framework.TestCase;

/**
 * The base class which starts up a cluster with LinuxTaskController as the task
 * controller.
 * 
 * In order to run test cases utilizing LinuxTaskController please follow the
 * following steps:
 * <ol>
 * <li>Build LinuxTaskController by not passing any
 * <code>-Dhadoop.conf.dir</code></li>
 * <li>Make the built binary to setuid executable</li>
 * <li>Execute following targets:
 * <code>ant test -Dcompile.c++=true -Dtaskcontroller-path=<em>path to built binary</em> 
 * -Dtaskcontroller-user=<em>user,group</em></code></li>
 * </ol>
 * 
 */
public class ClusterWithLinuxTaskController extends TestCase {
  private static final Log LOG =
      LogFactory.getLog(ClusterWithLinuxTaskController.class);

  /**
   * The wrapper class around LinuxTaskController which allows modification of
   * the custom path to task-controller which we can use for task management.
   * 
   **/
  public static class MyLinuxTaskController extends LinuxTaskController {
    String taskControllerExePath;

    @Override
    protected String getTaskControllerExecutablePath() {
      return taskControllerExePath;
    }

    void setTaskControllerExe(String execPath) {
      this.taskControllerExePath = execPath;
    }
  }

  // cluster instances which sub classes can use
  protected MiniMRCluster mrCluster = null;
  protected MiniDFSCluster dfsCluster = null;

  private JobConf clusterConf = null;
  protected Path homeDirectory;

  private static final int NUMBER_OF_NODES = 1;

  private File configurationFile = null;

  private UserGroupInformation taskControllerUser;

  /*
   * Utility method which subclasses use to start and configure the MR Cluster
   * so they can directly submit a job.
   */
  protected void startCluster()
      throws IOException {
    JobConf conf = new JobConf();
    dfsCluster = new MiniDFSCluster(conf, NUMBER_OF_NODES, true, null);
    conf.set("mapred.task.tracker.task-controller",
        MyLinuxTaskController.class.getName());
    mrCluster =
        new MiniMRCluster(NUMBER_OF_NODES, dfsCluster.getFileSystem().getUri()
            .toString(), 1, null, null, conf);

    // Get the configured taskcontroller-path
    String path = System.getProperty("taskcontroller-path");
    createTaskControllerConf(path);
    String execPath = path + "/task-controller";
    TaskTracker tracker = mrCluster.getTaskTrackerRunner(0).tt;
    // TypeCasting the parent to our TaskController instance as we
    // know that that would be instance which should be present in TT.
    ((MyLinuxTaskController) tracker.getTaskController())
        .setTaskControllerExe(execPath);
    String ugi = System.getProperty("taskcontroller-user");
    clusterConf = mrCluster.createJobConf();
    String[] splits = ugi.split(",");
    taskControllerUser = new UnixUserGroupInformation(splits);
    clusterConf.set(UnixUserGroupInformation.UGI_PROPERTY_NAME, ugi);
    createHomeDirectory(clusterConf);
  }

  private void createHomeDirectory(JobConf conf)
      throws IOException {
    FileSystem fs = dfsCluster.getFileSystem();
    String path = "/user/" + taskControllerUser.getUserName();
    homeDirectory = new Path(path);
    LOG.info("Creating Home directory : " + homeDirectory);
    fs.mkdirs(homeDirectory);
    changePermission(conf, homeDirectory);
  }

  private void changePermission(JobConf conf, Path p)
      throws IOException {
    FileSystem fs = dfsCluster.getFileSystem();
    fs.setOwner(homeDirectory, taskControllerUser.getUserName(),
        taskControllerUser.getGroupNames()[0]);
  }

  private void createTaskControllerConf(String path)
      throws IOException {
    File confDirectory = new File(path, "../conf");
    if (!confDirectory.exists()) {
      confDirectory.mkdirs();
    }
    configurationFile = new File(confDirectory, "taskcontroller.cfg");
    PrintWriter writer =
        new PrintWriter(new FileOutputStream(configurationFile));

    writer.println(String.format("mapred.local.dir=%s", mrCluster
        .getTaskTrackerLocalDir(0)));

    writer.flush();
    writer.close();
  }

  /**
   * Can we run the tests with LinuxTaskController?
   * 
   * @return boolean
   */
  protected boolean shouldRun() {
    return isTaskExecPathPassed() && isUserPassed();
  }

  private boolean isTaskExecPathPassed() {
    String path = System.getProperty("taskcontroller-path");
    if (path == null || path.isEmpty()
        || path.equals("${taskcontroller-path}")) {
      return false;
    }
    return true;
  }

  private boolean isUserPassed() {
    String ugi = System.getProperty("taskcontroller-user");
    if (ugi != null && !(ugi.equals("${taskcontroller-user}"))
        && !ugi.isEmpty()) {
      if (ugi.indexOf(",") > 1) {
        return true;
      }
      return false;
    }
    return false;
  }

  protected JobConf getClusterConf() {
    return new JobConf(clusterConf);
  }

  @Override
  protected void tearDown()
      throws Exception {
    if (mrCluster != null) {
      mrCluster.shutdown();
    }

    if (dfsCluster != null) {
      dfsCluster.shutdown();
    }

    if (configurationFile != null) {
      configurationFile.delete();
    }

    super.tearDown();
  }

  /**
   * Assert that the job is actually run by the specified user by verifying the
   * permissions of the output part-files.
   * 
   * @param outDir
   * @throws IOException
   */
  protected void assertOwnerShip(Path outDir)
      throws IOException {
    FileSystem fs = outDir.getFileSystem(clusterConf);
    assertOwnerShip(outDir, fs);
  }

  /**
   * Assert that the job is actually run by the specified user by verifying the
   * permissions of the output part-files.
   * 
   * @param outDir
   * @param fs
   * @throws IOException
   */
  protected void assertOwnerShip(Path outDir, FileSystem fs)
      throws IOException {
    for (FileStatus status : fs.listStatus(outDir, new OutputLogFilter())) {
      String owner = status.getOwner();
      String group = status.getGroup();
      LOG.info("Ownership of the file is " + status.getPath() + " is " + owner
          + "," + group);
      assertTrue("Output part-file's owner is not correct. Expected : "
          + taskControllerUser.getUserName() + " Found : " + owner, owner
          .equals(taskControllerUser.getUserName()));
      assertTrue("Output part-file's group is not correct. Expected : "
          + taskControllerUser.getGroupNames()[0] + " Found : " + group, group
          .equals(taskControllerUser.getGroupNames()[0]));
    }
  }
}

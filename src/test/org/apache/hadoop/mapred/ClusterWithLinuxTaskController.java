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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
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
 * <li>Change ownership of the built binary to root:group1, where group1 is
 * a secondary group of the test runner.</li>
 * <li>Change permissions on the binary so that <em>others</em> component does
 * not have any permissions on binary</li> 
 * <li>Make the built binary to setuid and setgid executable</li>
 * <li>Execute following targets:
 * <code>ant test -Dcompile.c++=true -Dtaskcontroller-path=<em>path to built binary</em> 
 * -Dtaskcontroller-ugi=<em>user,group</em></code></li>
 * </ol>
 * 
 */
public class ClusterWithLinuxTaskController extends TestCase {
  private static final Log LOG =
      LogFactory.getLog(ClusterWithLinuxTaskController.class);
  static String TT_GROUP = "mapreduce.tasktracker.group";

  /**
   * The wrapper class around LinuxTaskController which allows modification of
   * the custom path to task-controller which we can use for task management.
   * 
   **/
  public static class MyLinuxTaskController extends LinuxTaskController {
    String taskControllerExePath = System.getProperty(TASKCONTROLLER_PATH)
        + "/task-controller";

    @Override
    public void setup() throws IOException {
      // get the current ugi and set the task controller group owner
      getConf().set(TT_GROUP, taskTrackerSpecialGroup);

      // write configuration file
      configurationFile = createTaskControllerConf(System
          .getProperty(TASKCONTROLLER_PATH), getConf());
      super.setup();
    }

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

  /** changing this to a larger number needs more work for creating
   *  taskcontroller.cfg.
   *  see {@link #startCluster()} and
   *  {@link #createTaskControllerConf(String, Configuration)}
   */
  private static final int NUMBER_OF_NODES = 1;

  static final String TASKCONTROLLER_PATH = "taskcontroller-path";
  static final String TASKCONTROLLER_UGI = "taskcontroller-ugi";

  private static File configurationFile = null;

  protected UserGroupInformation taskControllerUser;
  
  protected static String taskTrackerSpecialGroup = null;
  static {
    if (isTaskExecPathPassed()) {
      try {
        taskTrackerSpecialGroup = FileSystem.getLocal(new Configuration())
            .getFileStatus(
                new Path(System.getProperty(TASKCONTROLLER_PATH),
                    "task-controller")).getGroup();
      } catch (IOException e) {
        LOG.warn("Could not get group of the binary", e);
        fail("Could not get group of the binary");
      }
    }
  }

  /*
   * Utility method which subclasses use to start and configure the MR Cluster
   * so they can directly submit a job.
   */
  protected void startCluster()
      throws IOException, InterruptedException {
    JobConf conf = new JobConf();
    dfsCluster = new MiniDFSCluster(conf, NUMBER_OF_NODES, true, null);
    conf.set("mapred.task.tracker.task-controller",
        MyLinuxTaskController.class.getName());
    mrCluster =
        new MiniMRCluster(NUMBER_OF_NODES, dfsCluster.getFileSystem().getUri()
            .toString(), 4, null, null, conf);

    String ugi = System.getProperty(TASKCONTROLLER_UGI);
    clusterConf = mrCluster.createJobConf();
    String[] splits = ugi.split(",");
    taskControllerUser = UserGroupInformation.createUserForTesting(splits[0],
        new String[]{splits[1]});
    createHomeAndStagingDirectory(clusterConf);
  }

  private void createHomeAndStagingDirectory(JobConf conf)
      throws IOException {
    FileSystem fs = dfsCluster.getFileSystem();
    String path = "/user/" + taskControllerUser.getUserName();
    homeDirectory = new Path(path);
    LOG.info("Creating Home directory : " + homeDirectory);
    fs.mkdirs(homeDirectory);
    changePermission(fs);
    Path stagingArea = 
      new Path(conf.get("mapreduce.jobtracker.staging.root.dir",
          "/tmp/hadoop/mapred/staging"));
    LOG.info("Creating Staging root directory : " + stagingArea);
    fs.mkdirs(stagingArea);
    fs.setPermission(stagingArea, new FsPermission((short)0777));
  }

  private void changePermission(FileSystem fs)
      throws IOException {
    fs.setOwner(homeDirectory, taskControllerUser.getUserName(),
        taskControllerUser.getGroupNames()[0]);
  }

  static File getTaskControllerConfFile(String path) {
    File confDirectory = new File(path, "../conf");
    return new File(confDirectory, "taskcontroller.cfg");
  }

  /**
   * Create taskcontroller.cfg.
   * 
   * @param path Path to the taskcontroller binary.
   * @param conf TaskTracker's configuration
   * @return the created conf file
   * @throws IOException
   */
  static File createTaskControllerConf(String path,
      Configuration conf) throws IOException {
    File confDirectory = new File(path, "../conf");
    if (!confDirectory.exists()) {
      confDirectory.mkdirs();
    }
    File configurationFile = new File(confDirectory, "taskcontroller.cfg");
    PrintWriter writer =
        new PrintWriter(new FileOutputStream(configurationFile));

    writer.println(String.format("mapred.local.dir=%s", conf.
        get(JobConf.MAPRED_LOCAL_DIR_PROPERTY)));

    writer
        .println(String.format("hadoop.log.dir=%s", TaskLog.getBaseLogDir()));
    writer.println(String.format(TT_GROUP + "=%s", conf.get(TT_GROUP)));

    writer.flush();
    writer.close();
    return configurationFile;
  }

  /**
   * Can we run the tests with LinuxTaskController?
   * 
   * @return boolean
   */
  protected static boolean shouldRun() {
    if (!isTaskExecPathPassed() || !isUserPassed()) {
      LOG.info("Not running test.");
      return false;
    }
    return true;
  }

  static boolean isTaskExecPathPassed() {
    String path = System.getProperty(TASKCONTROLLER_PATH);
    if (path == null || path.isEmpty()
        || path.equals("${" + TASKCONTROLLER_PATH + "}")) {
      LOG.info("Invalid taskcontroller-path : " + path); 
      return false;
    }
    return true;
  }

  private static boolean isUserPassed() {
    String ugi = System.getProperty(TASKCONTROLLER_UGI);
    if (ugi != null && !(ugi.equals("${" + TASKCONTROLLER_UGI + "}"))
        && !ugi.isEmpty()) {
      if (ugi.indexOf(",") > 1) {
        return true;
      }
      LOG.info("Invalid taskcontroller-ugi : " + ugi); 
      return false;
    }
    LOG.info("Invalid taskcontroller-ugi : " + ugi);
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

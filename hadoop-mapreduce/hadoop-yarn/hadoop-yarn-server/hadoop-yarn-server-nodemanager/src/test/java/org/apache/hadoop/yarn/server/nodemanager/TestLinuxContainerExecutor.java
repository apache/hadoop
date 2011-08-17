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

package org.apache.hadoop.yarn.server.nodemanager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.AccessControlException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestLinuxContainerExecutor {
//
//  private static final Log LOG = LogFactory
//      .getLog(TestLinuxContainerExecutor.class);
//
//  // TODO: FIXME
//  private static File workSpace = new File("target",
//      TestLinuxContainerExecutor.class.getName() + "-workSpace");
//
//  @Before
//  public void setup() throws IOException {
//    FileContext.getLocalFSFileContext().mkdir(
//        new Path(workSpace.getAbsolutePath()), null, true);
//    workSpace.setReadable(true, false);
//    workSpace.setExecutable(true, false);
//    workSpace.setWritable(true, false);
//  }
//
//  @After
//  public void tearDown() throws AccessControlException, FileNotFoundException,
//      UnsupportedFileSystemException, IOException {
//    FileContext.getLocalFSFileContext().delete(
//        new Path(workSpace.getAbsolutePath()), true);
//  }
//
  @Test
  public void testCommandFilePreparation() throws IOException {
//    LinuxContainerExecutor executor = new LinuxContainerExecutor(new String[] {
//        "/bin/echo", "hello" }, null, null, "nobody"); // TODO: fix user name
//    executor.prepareCommandFile(workSpace.getAbsolutePath());
//
//    // Now verify the contents of the commandFile
//    File commandFile = new File(workSpace, LinuxContainerExecutor.COMMAND_FILE);
//    BufferedReader reader = new BufferedReader(new FileReader(commandFile));
//    Assert.assertEquals("/bin/echo hello", reader.readLine());
//    Assert.assertEquals(null, reader.readLine());
//    Assert.assertTrue(commandFile.canExecute());
  }
//
//  @Test
//  public void testContainerLaunch() throws IOException {
//    String containerExecutorPath = System
//        .getProperty("container-executor-path");
//    if (containerExecutorPath == null || containerExecutorPath.equals("")) {
//      LOG.info("Not Running test for lack of container-executor-path");
//      return;
//    }
//
//    String applicationSubmitter = "nobody";
//
//    File touchFile = new File(workSpace, "touch-file");
//    LinuxContainerExecutor executor = new LinuxContainerExecutor(new String[] {
//        "touch", touchFile.getAbsolutePath() }, workSpace, null,
//        applicationSubmitter);
//    executor.setCommandExecutorPath(containerExecutorPath);
//    executor.execute();
//
//    FileStatus fileStatus = FileContext.getLocalFSFileContext().getFileStatus(
//        new Path(touchFile.getAbsolutePath()));
//    Assert.assertEquals(applicationSubmitter, fileStatus.getOwner());
//  }
//
//  @Test
//  public void testContainerKill() throws IOException, InterruptedException,
//      IllegalArgumentException, SecurityException, IllegalAccessException,
//      NoSuchFieldException {
//    String containerExecutorPath = System
//        .getProperty("container-executor-path");
//    if (containerExecutorPath == null || containerExecutorPath.equals("")) {
//      LOG.info("Not Running test for lack of container-executor-path");
//      return;
//    }
//
//    String applicationSubmitter = "nobody";
//    final LinuxContainerExecutor executor = new LinuxContainerExecutor(
//        new String[] { "sleep", "100" }, workSpace, null, applicationSubmitter);
//    executor.setCommandExecutorPath(containerExecutorPath);
//    new Thread() {
//      public void run() {
//        try {
//          executor.execute();
//        } catch (IOException e) {
//          // TODO Auto-generated catch block
//          e.printStackTrace();
//        }
//      };
//    }.start();
//
//    String pid;
//    while ((pid = executor.getPid()) == null) {
//      LOG.info("Sleeping for 5 seconds before checking if "
//          + "the process is alive.");
//      Thread.sleep(5000);
//    }
//    LOG.info("Going to check the liveliness of the process with pid " + pid);
//
//    LinuxContainerExecutor checkLiveliness = new LinuxContainerExecutor(
//        new String[] { "kill", "-0", "-" + pid }, workSpace, null,
//        applicationSubmitter);
//    checkLiveliness.setCommandExecutorPath(containerExecutorPath);
//    checkLiveliness.execute();
//
//    LOG.info("Process is alive. "
//        + "Sleeping for 5 seconds before killing the process.");
//    Thread.sleep(5000);
//    LOG.info("Going to killing the process.");
//
//    executor.kill();
//
//    LOG.info("Sleeping for 5 seconds before checking if "
//        + "the process is alive.");
//    Thread.sleep(5000);
//    LOG.info("Going to check the liveliness of the process.");
//
//    // TODO: fix
//    checkLiveliness = new LinuxContainerExecutor(new String[] { "kill", "-0",
//        "-" + pid }, workSpace, null, applicationSubmitter);
//    checkLiveliness.setCommandExecutorPath(containerExecutorPath);
//    boolean success = false;
//    try {
//      checkLiveliness.execute();
//      success = true;
//    } catch (IOException e) {
//      success = false;
//    }
//
//    Assert.assertFalse(success);
//  }
}
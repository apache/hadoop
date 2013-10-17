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

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;

import junit.framework.Assert;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor.Signal;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This is intended to test the LinuxContainerExecutor code, but because of
 * some security restrictions this can only be done with some special setup
 * first.
 * <br><ol>
 * <li>Compile the code with container-executor.conf.dir set to the location you
 * want for testing.
 * <br><pre><code>
 * > mvn clean install -Pnative -Dcontainer-executor.conf.dir=/etc/hadoop
 *                          -DskipTests
 * </code></pre>
 * 
 * <li>Set up <code>${container-executor.conf.dir}/container-executor.cfg</code>
 * container-executor.cfg needs to be owned by root and have in it the proper
 * config values.
 * <br><pre><code>
 * > cat /etc/hadoop/container-executor.cfg
 * yarn.nodemanager.linux-container-executor.group=mapred
 * #depending on the user id of the application.submitter option
 * min.user.id=1
 * > sudo chown root:root /etc/hadoop/container-executor.cfg
 * > sudo chmod 444 /etc/hadoop/container-executor.cfg
 * </code></pre>
 * 
 * <li>Move the binary and set proper permissions on it. It needs to be owned 
 * by root, the group needs to be the group configured in container-executor.cfg, 
 * and it needs the setuid bit set. (The build will also overwrite it so you
 * need to move it to a place that you can support it. 
 * <br><pre><code>
 * > cp ./hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-nodemanager/src/main/c/container-executor/container-executor /tmp/
 * > sudo chown root:mapred /tmp/container-executor
 * > sudo chmod 4550 /tmp/container-executor
 * </code></pre>
 * 
 * <li>Run the tests with the execution enabled (The user you run the tests as
 * needs to be part of the group from the config.
 * <br><pre><code>
 * mvn test -Dtest=TestLinuxContainerExecutor -Dapplication.submitter=nobody -Dcontainer-executor.path=/tmp/container-executor
 * </code></pre>
 * </ol>
 */
public class TestLinuxContainerExecutor {
  private static final Log LOG = LogFactory
      .getLog(TestLinuxContainerExecutor.class);
  
  private static File workSpace = new File("target",
      TestLinuxContainerExecutor.class.getName() + "-workSpace");
  
  private LinuxContainerExecutor exec = null;
  private String appSubmitter = null;
  private LocalDirsHandlerService dirsHandler;

  @Before
  public void setup() throws Exception {
    FileContext files = FileContext.getLocalFSFileContext();
    Path workSpacePath = new Path(workSpace.getAbsolutePath());
    files.mkdir(workSpacePath, null, true);
    FileUtil.chmod(workSpace.getAbsolutePath(), "777");
    File localDir = new File(workSpace.getAbsoluteFile(), "localDir");
    files.mkdir(new Path(localDir.getAbsolutePath()),
        new FsPermission("777"), false);
    File logDir = new File(workSpace.getAbsoluteFile(), "logDir");
    files.mkdir(new Path(logDir.getAbsolutePath()),
        new FsPermission("777"), false);
    String exec_path = System.getProperty("container-executor.path");
    if(exec_path != null && !exec_path.isEmpty()) {
      Configuration conf = new Configuration(false);
      LOG.info("Setting "+YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH
          +"="+exec_path);
      conf.set(YarnConfiguration.NM_LINUX_CONTAINER_EXECUTOR_PATH, exec_path);
      exec = new LinuxContainerExecutor();
      exec.setConf(conf);
      conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir.getAbsolutePath());
      conf.set(YarnConfiguration.NM_LOG_DIRS, logDir.getAbsolutePath());
      dirsHandler = new LocalDirsHandlerService();
      dirsHandler.init(conf);
    }
    appSubmitter = System.getProperty("application.submitter");
    if(appSubmitter == null || appSubmitter.isEmpty()) {
      appSubmitter = "nobody";
    }
  }

  @After
  public void tearDown() throws Exception {
    FileContext.getLocalFSFileContext().delete(
        new Path(workSpace.getAbsolutePath()), true);
  }

  private boolean shouldRun() {
    if(exec == null) {
      LOG.warn("Not running test because container-executor.path is not set");
      return false;
    }
    return true;
  }
  
  private String writeScriptFile(String ... cmd) throws IOException {
    File f = File.createTempFile("TestLinuxContainerExecutor", ".sh");
    f.deleteOnExit();
    PrintWriter p = new PrintWriter(new FileOutputStream(f));
    p.println("#!/bin/sh");
    p.print("exec");
    for(String part: cmd) {
      p.print(" '");
      p.print(part.replace("\\", "\\\\").replace("'", "\\'"));
      p.print("'");
    }
    p.println();
    p.close();
    return f.getAbsolutePath();
  }
  
  private int id = 0;
  private synchronized int getNextId() {
    id += 1;
    return id;
  }
  
  private ContainerId getNextContainerId() {
    ContainerId cId = mock(ContainerId.class);
    String id = "CONTAINER_"+getNextId();
    when(cId.toString()).thenReturn(id);
    return cId;
  }
  

  private int runAndBlock(String ... cmd) throws IOException {
    return runAndBlock(getNextContainerId(), cmd);
  }
  
  private int runAndBlock(ContainerId cId, String ... cmd) throws IOException {
    String appId = "APP_"+getNextId();
    Container container = mock(Container.class);
    ContainerLaunchContext context = mock(ContainerLaunchContext.class);
    HashMap<String, String> env = new HashMap<String,String>();

    when(container.getContainerId()).thenReturn(cId);
    when(container.getLaunchContext()).thenReturn(context);

    when(context.getEnvironment()).thenReturn(env);
    
    String script = writeScriptFile(cmd);

    Path scriptPath = new Path(script);
    Path tokensPath = new Path("/dev/null");
    Path workDir = new Path(workSpace.getAbsolutePath());
    Path pidFile = new Path(workDir, "pid.txt");

    exec.activateContainer(cId, pidFile);
    return exec.launchContainer(container, scriptPath, tokensPath,
        appSubmitter, appId, workDir, dirsHandler.getLocalDirs(),
        dirsHandler.getLogDirs());
  }
  
  
  @Test
  public void testContainerLaunch() throws IOException {
    if (!shouldRun()) {
      return;
    }

    File touchFile = new File(workSpace, "touch-file");
    int ret = runAndBlock("touch", touchFile.getAbsolutePath());
    
    assertEquals(0, ret);
    FileStatus fileStatus = FileContext.getLocalFSFileContext().getFileStatus(
          new Path(touchFile.getAbsolutePath()));
    assertEquals(appSubmitter, fileStatus.getOwner());
  }

  @Test
  public void testContainerKill() throws Exception {
    if (!shouldRun()) {
      return;
    }
    
    final ContainerId sleepId = getNextContainerId();   
    Thread t = new Thread() {
      public void run() {
        try {
          runAndBlock(sleepId, "sleep", "100");
        } catch (IOException e) {
          LOG.warn("Caught exception while running sleep",e);
        }
      };
    };
    t.setDaemon(true); //If it does not exit we shouldn't block the test.
    t.start();

    assertTrue(t.isAlive());
   
    String pid = null;
    int count = 10;
    while ((pid = exec.getProcessId(sleepId)) == null && count > 0) {
      LOG.info("Sleeping for 200 ms before checking for pid ");
      Thread.sleep(200);
      count--;
    }
    assertNotNull(pid);

    LOG.info("Going to killing the process.");
    exec.signalContainer(appSubmitter, pid, Signal.TERM);
    LOG.info("sleeping for 100ms to let the sleep be killed");
    Thread.sleep(100);
    
    assertFalse(t.isAlive());
  }

  @Test
  public void testLocalUser() throws Exception {
    try {
      //nonsecure default
      Configuration conf = new YarnConfiguration();
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          "simple");
      UserGroupInformation.setConfiguration(conf);
      LinuxContainerExecutor lce = new LinuxContainerExecutor();
      lce.setConf(conf);
      Assert.assertEquals(YarnConfiguration.DEFAULT_NM_NONSECURE_MODE_LOCAL_USER,
          lce.getRunAsUser("foo"));

      //nonsecure custom setting
      conf.set(YarnConfiguration.NM_NONSECURE_MODE_LOCAL_USER_KEY, "bar");
      lce = new LinuxContainerExecutor();
      lce.setConf(conf);
      Assert.assertEquals("bar", lce.getRunAsUser("foo"));

      //secure
      conf = new YarnConfiguration();
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          "kerberos");
      UserGroupInformation.setConfiguration(conf);
      lce = new LinuxContainerExecutor();
      lce.setConf(conf);
      Assert.assertEquals("foo", lce.getRunAsUser("foo"));
    } finally {
      Configuration conf = new YarnConfiguration();
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          "simple");
      UserGroupInformation.setConfiguration(conf);
    }
  }

  @Test
  public void testNonsecureUsernamePattern() throws Exception {
    try {
      //nonsecure default
      Configuration conf = new YarnConfiguration();
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          "simple");
      UserGroupInformation.setConfiguration(conf);
      LinuxContainerExecutor lce = new LinuxContainerExecutor();
      lce.setConf(conf);
      lce.verifyUsernamePattern("foo");
      try {
        lce.verifyUsernamePattern("foo/x");
        Assert.fail();
      } catch (IllegalArgumentException ex) {
        //NOP        
      } catch (Throwable ex) {
        Assert.fail(ex.toString());
      }
      
      //nonsecure custom setting
      conf.set(YarnConfiguration.NM_NONSECURE_MODE_USER_PATTERN_KEY, "foo");
      lce = new LinuxContainerExecutor();
      lce.setConf(conf);
      lce.verifyUsernamePattern("foo");
      try {
        lce.verifyUsernamePattern("bar");
        Assert.fail();
      } catch (IllegalArgumentException ex) {
        //NOP        
      } catch (Throwable ex) {
        Assert.fail(ex.toString());
      }

      //secure, pattern matching does not kick in.
      conf = new YarnConfiguration();
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          "kerberos");
      UserGroupInformation.setConfiguration(conf);
      lce = new LinuxContainerExecutor();
      lce.setConf(conf);
      lce.verifyUsernamePattern("foo");
      lce.verifyUsernamePattern("foo/w");
    } finally {
      Configuration conf = new YarnConfiguration();
      conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
          "simple");
      UserGroupInformation.setConfiguration(conf);
    }
  }

}

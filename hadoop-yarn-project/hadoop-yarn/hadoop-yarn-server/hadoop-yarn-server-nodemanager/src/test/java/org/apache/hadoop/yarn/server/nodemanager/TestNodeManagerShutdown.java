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
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.api.records.MasterKey;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.TestContainerManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNodeManagerShutdown {
  static final File basedir =
      new File("target", TestNodeManagerShutdown.class.getName());
  static final File tmpDir = new File(basedir, "tmpDir");
  static final File logsDir = new File(basedir, "logs");
  static final File remoteLogsDir = new File(basedir, "remotelogs");
  static final File nmLocalDir = new File(basedir, "nm0");
  static final File processStartFile = new File(tmpDir, "start_file.txt")
    .getAbsoluteFile();

  static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  static final String user = "nobody";
  private FileContext localFS;
  private ContainerId cId;
  private NodeManager nm;

  @Before
  public void setup() throws UnsupportedFileSystemException {
    localFS = FileContext.getLocalFSFileContext();
    tmpDir.mkdirs();
    logsDir.mkdirs();
    remoteLogsDir.mkdirs();
    nmLocalDir.mkdirs();

    // Construct the Container-id
    cId = createContainerId();
  }
  
  @After
  public void tearDown() throws IOException, InterruptedException {
    if (nm != null) {
      nm.stop();
    }
    localFS.delete(new Path(basedir.getPath()), true);
  }
  
  @Test
  public void testStateStoreRemovalOnDecommission() throws IOException {
    final File recoveryDir = new File(basedir, "nm-recovery");
    nm = new TestNodeManager();
    YarnConfiguration conf = createNMConfig();
    conf.setBoolean(YarnConfiguration.NM_RECOVERY_ENABLED, true);
    conf.set(YarnConfiguration.NM_RECOVERY_DIR, recoveryDir.getAbsolutePath());

    // verify state store is not removed on normal shutdown
    nm.init(conf);
    nm.start();
    Assert.assertTrue(recoveryDir.exists());
    Assert.assertTrue(recoveryDir.isDirectory());
    nm.stop();
    nm = null;
    Assert.assertTrue(recoveryDir.exists());
    Assert.assertTrue(recoveryDir.isDirectory());

    // verify state store is removed on decommissioned shutdown
    nm = new TestNodeManager();
    nm.init(conf);
    nm.start();
    Assert.assertTrue(recoveryDir.exists());
    Assert.assertTrue(recoveryDir.isDirectory());
    nm.getNMContext().setDecommissioned(true);
    nm.stop();
    nm = null;
    Assert.assertFalse(recoveryDir.exists());
  }

  @Test
  public void testKillContainersOnShutdown() throws IOException,
      YarnException {
    nm = new TestNodeManager();
    int port = ServerSocketUtil.getPort(49157, 10);
    nm.init(createNMConfig(port));
    nm.start();
    startContainer(nm, cId, localFS, tmpDir, processStartFile, port);
    
    final int MAX_TRIES=20;
    int numTries = 0;
    while (!processStartFile.exists() && numTries < MAX_TRIES) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException ex) {ex.printStackTrace();}
      numTries++;
    }
    
    nm.stop();
    
    // Now verify the contents of the file.  Script generates a message when it
    // receives a sigterm so we look for that.  We cannot perform this check on
    // Windows, because the process is not notified when killed by winutils.
    // There is no way for the process to trap and respond.  Instead, we can
    // verify that the job object with ID matching container ID no longer exists.
    if (Shell.WINDOWS) {
      Assert.assertFalse("Process is still alive!",
        DefaultContainerExecutor.containerIsAlive(cId.toString()));
    } else {
      BufferedReader reader =
          new BufferedReader(new FileReader(processStartFile));

      boolean foundSigTermMessage = false;
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        if (line.contains("SIGTERM")) {
          foundSigTermMessage = true;
          break;
        }
      }
      Assert.assertTrue("Did not find sigterm message", foundSigTermMessage);
      reader.close();
    }
  }

  public static void startContainer(NodeManager nm, ContainerId cId,
      FileContext localFS, File scriptFileDir, File processStartFile,
      final int port)
          throws IOException, YarnException {
    File scriptFile =
        createUnhaltingScriptFile(cId, scriptFileDir, processStartFile);
    
    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);

    NodeId nodeId = BuilderUtils.newNodeId(InetAddress.getByName("localhost")
        .getCanonicalHostName(), port);
    
    URL localResourceUri =
        URL.fromPath(localFS
            .makeQualified(new Path(scriptFile.getAbsolutePath())));
    LocalResource localResource =
        recordFactory.newRecordInstance(LocalResource.class);
    localResource.setResource(localResourceUri);
    localResource.setSize(-1);
    localResource.setVisibility(LocalResourceVisibility.APPLICATION);
    localResource.setType(LocalResourceType.FILE);
    localResource.setTimestamp(scriptFile.lastModified());
    String destinationFile = "dest_file";
    Map<String, LocalResource> localResources = 
        new HashMap<String, LocalResource>();
    localResources.put(destinationFile, localResource);
    containerLaunchContext.setLocalResources(localResources);
    List<String> commands = Arrays.asList(Shell.getRunScriptCommand(scriptFile));
    containerLaunchContext.setCommands(commands);
    final InetSocketAddress containerManagerBindAddress =
        NetUtils.createSocketAddrForHost("127.0.0.1", port);
    UserGroupInformation currentUser = UserGroupInformation
        .createRemoteUser(cId.toString());
    org.apache.hadoop.security.token.Token<NMTokenIdentifier> nmToken =
        ConverterUtils.convertFromYarn(
          nm.getNMContext().getNMTokenSecretManager()
            .createNMToken(cId.getApplicationAttemptId(), nodeId, user),
          containerManagerBindAddress);
    currentUser.addToken(nmToken);

    ContainerManagementProtocol containerManager =
        currentUser.doAs(new PrivilegedAction<ContainerManagementProtocol>() {
          @Override
          public ContainerManagementProtocol run() {
            Configuration conf = new Configuration();
            YarnRPC rpc = YarnRPC.create(conf);
            InetSocketAddress containerManagerBindAddress =
                NetUtils.createSocketAddrForHost("127.0.0.1", port);
            return (ContainerManagementProtocol) rpc.getProxy(ContainerManagementProtocol.class,
              containerManagerBindAddress, conf);
          }
        });
    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(containerLaunchContext,
          TestContainerManager.createContainerToken(cId, 0,
            nodeId, user, nm.getNMContext().getContainerTokenSecretManager()));
    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    list.add(scRequest);
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);
    containerManager.startContainers(allRequests);
    
    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(cId);
    GetContainerStatusesRequest request =
        GetContainerStatusesRequest.newInstance(containerIds);
    ContainerStatus containerStatus =
        containerManager.getContainerStatuses(request).getContainerStatuses().get(0);
    Assert.assertEquals(ContainerState.RUNNING, containerStatus.getState());
  }
  
  public static ContainerId createContainerId() {
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 0);
    return containerId;
  }
  
  private YarnConfiguration createNMConfig(int port) throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    conf.setInt(YarnConfiguration.NM_PMEM_MB, 5*1024); // 5GB
    conf.set(YarnConfiguration.NM_ADDRESS, "127.0.0.1:" + port);
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, "127.0.0.1:"
        + ServerSocketUtil.getPort(49158, 10));
    conf.set(YarnConfiguration.NM_WEBAPP_ADDRESS,
        "127.0.0.1:" + ServerSocketUtil
            .getPort(YarnConfiguration.DEFAULT_NM_WEBAPP_PORT, 10));
    conf.set(YarnConfiguration.NM_LOG_DIRS, logsDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteLogsDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, nmLocalDir.getAbsolutePath());
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    return conf;
  }
  
  private YarnConfiguration createNMConfig() throws IOException {
    return createNMConfig(ServerSocketUtil.getPort(49157, 10));
  }

  /**
   * Creates a script to run a container that will run forever unless
   * stopped by external means.
   */
  private static File createUnhaltingScriptFile(ContainerId cId,
      File scriptFileDir, File processStartFile) throws IOException {
    File scriptFile = Shell.appendScriptExtension(scriptFileDir, "scriptFile");
    PrintWriter fileWriter = new PrintWriter(scriptFile);
    if (Shell.WINDOWS) {
      fileWriter.println("@echo \"Running testscript for delayed kill\"");
      fileWriter.println("@echo \"Writing pid to start file\"");
      fileWriter.println("@echo " + cId + ">> " + processStartFile);
      fileWriter.println("@pause");
    } else {
      fileWriter.write("#!/bin/bash\n\n");
      fileWriter.write("echo \"Running testscript for delayed kill\"\n");
      fileWriter.write("hello=\"Got SIGTERM\"\n");
      fileWriter.write("umask 0\n");
      fileWriter.write("trap \"echo $hello >> " + processStartFile +
        "\" SIGTERM\n");
      fileWriter.write("echo \"Writing pid to start file\"\n");
      fileWriter.write("echo $$ >> " + processStartFile + "\n");
      fileWriter.write("while true; do\ndate >> /dev/null;\n done\n");
    }

    fileWriter.close();
    return scriptFile;
  }

  class TestNodeManager extends NodeManager {

    @Override
    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
        Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
      MockNodeStatusUpdater myNodeStatusUpdater =
          new MockNodeStatusUpdater(context, dispatcher, healthChecker, metrics);
      return myNodeStatusUpdater;
    }
    
    public void setMasterKey(MasterKey masterKey) {
      getNMContext().getContainerTokenSecretManager().setMasterKey(masterKey);
    }
  }
}

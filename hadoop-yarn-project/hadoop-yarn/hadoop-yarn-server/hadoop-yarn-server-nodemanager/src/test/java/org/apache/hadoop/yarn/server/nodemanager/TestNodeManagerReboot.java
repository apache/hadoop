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

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.TestContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.deletion.task.FileDeletionMatcher;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ContainerLocalizer;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNodeManagerReboot {

  static final File basedir = new File("target",
    TestNodeManagerReboot.class.getName());
  static final File logsDir = new File(basedir, "logs");
  static final File nmLocalDir = new File(basedir, "nm0");
  static final File localResourceDir = new File(basedir, "resource");

  static final String user = System.getProperty("user.name");
  private FileContext localFS;
  private MyNodeManager nm;
  private DeletionService delService;
  static final Logger LOG =
       LoggerFactory.getLogger(TestNodeManagerReboot.class);

  @Before
  public void setup() throws UnsupportedFileSystemException {
    localFS = FileContext.getLocalFSFileContext();
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    localFS.delete(new Path(basedir.getPath()), true);
    if (nm != null) {
      nm.stop();
    }
  }

  @Test(timeout = 2000000)
  public void testClearLocalDirWhenNodeReboot() throws IOException,
      YarnException, InterruptedException {
    nm = new MyNodeManager();
    nm.start();

    final ContainerManagementProtocol containerManager =
        nm.getContainerManager();

    // create files under fileCache
    createFiles(nmLocalDir.getAbsolutePath(), ContainerLocalizer.FILECACHE, 100);
    localResourceDir.mkdirs();

    ContainerLaunchContext containerLaunchContext =
        Records.newRecord(ContainerLaunchContext.class);
    // Construct the Container-id
    ContainerId cId = createContainerId();

    URL localResourceUri =
        URL.fromPath(localFS.makeQualified(new Path(
          localResourceDir.getAbsolutePath())));

    LocalResource localResource =
        LocalResource.newInstance(localResourceUri, LocalResourceType.FILE,
          LocalResourceVisibility.APPLICATION, -1,
          localResourceDir.lastModified());
    String destinationFile = "dest_file";
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();
    localResources.put(destinationFile, localResource);
    containerLaunchContext.setLocalResources(localResources);
    List<String> commands = new ArrayList<String>();
    containerLaunchContext.setCommands(commands);

    NodeId nodeId = nm.getNMContext().getNodeId();
    StartContainerRequest scRequest =
        StartContainerRequest.newInstance(containerLaunchContext,
          TestContainerManager.createContainerToken(
            cId, 0, nodeId, destinationFile, nm.getNMContext()
              .getContainerTokenSecretManager()));
    List<StartContainerRequest> list = new ArrayList<StartContainerRequest>();
    list.add(scRequest);
    final StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);

    final UserGroupInformation currentUser =
        UserGroupInformation.createRemoteUser(cId.getApplicationAttemptId()
          .toString());
    NMTokenIdentifier nmIdentifier =
        new NMTokenIdentifier(cId.getApplicationAttemptId(), nodeId, user, 123);
    currentUser.addTokenIdentifier(nmIdentifier);
    currentUser.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws YarnException, IOException {
        nm.getContainerManager().startContainers(allRequests);
        return null;
      }
    });

    List<ContainerId> containerIds = new ArrayList<ContainerId>();
    containerIds.add(cId);
    GetContainerStatusesRequest request =
        GetContainerStatusesRequest.newInstance(containerIds);
    Container container =
        nm.getNMContext().getContainers().get(request.getContainerIds().get(0));

    final int MAX_TRIES = 20;
    int numTries = 0;
    while (!container.getContainerState().equals(ContainerState.DONE)
        && numTries <= MAX_TRIES) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException ex) {
        // Do nothing
      }
      numTries++;
    }

    Assert.assertEquals(ContainerState.DONE, container.getContainerState());

    Assert
      .assertTrue(
        "The container should create a subDir named currentUser: " + user
            + "under localDir/usercache",
        numOfLocalDirs(nmLocalDir.getAbsolutePath(),
          ContainerLocalizer.USERCACHE) > 0);

    Assert.assertTrue(
      "There should be files or Dirs under nm_private when "
          + "container is launched",
      numOfLocalDirs(nmLocalDir.getAbsolutePath(),
        ResourceLocalizationService.NM_PRIVATE_DIR) > 0);

    // restart the NodeManager
    restartNM(MAX_TRIES);
    checkNumOfLocalDirs();

    verify(delService, times(1)).delete(argThat(new FileDeletionMatcher(
        delService, null,
        new Path(ResourceLocalizationService.NM_PRIVATE_DIR + "_DEL_"), null)));
    verify(delService, times(1)).delete(argThat(new FileDeletionMatcher(
        delService, null, new Path(ContainerLocalizer.FILECACHE + "_DEL_"),
        null)));
    verify(delService, times(1)).delete(argThat(new FileDeletionMatcher(
        delService, user, null, Arrays.asList(new Path(destinationFile)))));
    verify(delService, times(1)).delete(argThat(new FileDeletionMatcher(
        delService, null, new Path(ContainerLocalizer.USERCACHE + "_DEL_"),
        new ArrayList<Path>())));
    
    // restart the NodeManager again
    // this time usercache directory should be empty
    restartNM(MAX_TRIES);
    checkNumOfLocalDirs();
    
  }

  private void restartNM(int maxTries) throws IOException {
    nm.stop();
    nm = new MyNodeManager();
    nm.start();

    int numTries = 0;
    while ((numOfLocalDirs(nmLocalDir.getAbsolutePath(),
      ContainerLocalizer.USERCACHE) > 0
        || numOfLocalDirs(nmLocalDir.getAbsolutePath(),
          ContainerLocalizer.FILECACHE) > 0 || numOfLocalDirs(
      nmLocalDir.getAbsolutePath(), ResourceLocalizationService.NM_PRIVATE_DIR) > 0)
        && numTries < maxTries) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException ex) {
        // Do nothing
      }
      numTries++;
    }
  }
  
  private void checkNumOfLocalDirs() throws IOException {
    Assert
      .assertTrue(
        "After NM reboots, all local files should be deleted",
        numOfLocalDirs(nmLocalDir.getAbsolutePath(),
          ContainerLocalizer.USERCACHE) == 0
            && numOfLocalDirs(nmLocalDir.getAbsolutePath(),
              ContainerLocalizer.FILECACHE) == 0
            && numOfLocalDirs(nmLocalDir.getAbsolutePath(),
              ResourceLocalizationService.NM_PRIVATE_DIR) == 0);
    
    Assert
    .assertTrue(
      "After NM reboots, usercache_DEL_* directory should be deleted",
      numOfUsercacheDELDirs(nmLocalDir.getAbsolutePath()) == 0);
  }
  
  private int numOfLocalDirs(String localDir, String localSubDir) {
    File[] listOfFiles = new File(localDir, localSubDir).listFiles();
    if (listOfFiles == null) {
      return 0;
    } else {
      return listOfFiles.length;
    }
  }
  
  private int numOfUsercacheDELDirs(String localDir) throws IOException {
    int count = 0;
    RemoteIterator<FileStatus> fileStatus = localFS.listStatus(new Path(localDir));
    while (fileStatus.hasNext()) {
      FileStatus status = fileStatus.next();
      if (status.getPath().getName().matches(".*" +
          ContainerLocalizer.USERCACHE + "_DEL_.*")) {
        count++;
      }
    }
    return count;
  }

  private void createFiles(String dir, String subDir, int numOfFiles) {
    for (int i = 0; i < numOfFiles; i++) {
      File newFile = new File(dir + "/" + subDir, "file_" + (i + 1));
      try {
        newFile.createNewFile();
      } catch (IOException e) {
        // Do nothing
      }
    }
  }

  private ContainerId createContainerId() {
    ApplicationId appId = ApplicationId.newInstance(0, 0);
    ApplicationAttemptId appAttemptId =
        ApplicationAttemptId.newInstance(appId, 1);
    ContainerId containerId = ContainerId.newContainerId(appAttemptId, 0);
    return containerId;
  }

  private class MyNodeManager extends NodeManager {

    public MyNodeManager() throws IOException {
      super();
      this.init(createNMConfig());
    }

    @Override
    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
        Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
      MockNodeStatusUpdater myNodeStatusUpdater =
          new MockNodeStatusUpdater(context, dispatcher, healthChecker, metrics);
      return myNodeStatusUpdater;
    }

    @Override
    protected DeletionService createDeletionService(ContainerExecutor exec) {
      delService = spy(new DeletionService(exec));
      return delService;
    }

    private YarnConfiguration createNMConfig() throws IOException {
      YarnConfiguration conf = new YarnConfiguration();
      conf.setInt(YarnConfiguration.NM_PMEM_MB, 5 * 1024); // 5GB
      conf.set(YarnConfiguration.NM_ADDRESS,
          "127.0.0.1:" + ServerSocketUtil.getPort(49152, 10));
      conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, "127.0.0.1:"
          + ServerSocketUtil.getPort(49153, 10));
      conf.set(YarnConfiguration.NM_LOG_DIRS, logsDir.getAbsolutePath());
      conf.set(YarnConfiguration.NM_LOCAL_DIRS, nmLocalDir.getAbsolutePath());
      conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
      return conf;
    }
  }
}

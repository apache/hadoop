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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.LocalRMInterface;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager.NMContext;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.ApplicationState;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.service.Service.STATE;
import org.junit.After;
import org.junit.Before;

public abstract class BaseContainerManagerTest {

  protected static RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  protected static FileContext localFS;
  protected static File localDir;
  protected static File localLogDir;
  protected static File remoteLogDir;
  protected static File tmpDir;
  protected ContainerTokenSecretManager containerTokenSecretManager = new ContainerTokenSecretManager();

  protected final NodeManagerMetrics metrics = NodeManagerMetrics.create();

  public BaseContainerManagerTest() throws UnsupportedFileSystemException {
    localFS = FileContext.getLocalFSFileContext();
    localDir =
        new File("target", this.getClass().getName() + "-localDir")
            .getAbsoluteFile();
    localLogDir =
        new File("target", this.getClass().getName() + "-localLogDir")
            .getAbsoluteFile();
    remoteLogDir =
      new File("target", this.getClass().getName() + "-remoteLogDir")
          .getAbsoluteFile();
    tmpDir = new File("target", this.getClass().getName() + "-tmpDir");
  }

  protected static Log LOG = LogFactory
      .getLog(BaseContainerManagerTest.class);

  protected Configuration conf = new YarnConfiguration();
  protected Context context = new NMContext();
  protected ContainerExecutor exec;
  protected DeletionService delSrvc;
  protected String user = "nobody";

  protected NodeStatusUpdater nodeStatusUpdater = new NodeStatusUpdaterImpl(
      context, new AsyncDispatcher(), null, metrics, this.containerTokenSecretManager) {
    @Override
    protected ResourceTracker getRMClient() {
      return new LocalRMInterface();
    };

    @Override
    protected void startStatusUpdater() {
      return; // Don't start any updating thread.
    }
  };

  protected ContainerManagerImpl containerManager = null;

  protected ContainerExecutor createContainerExecutor() {
    DefaultContainerExecutor exec = new DefaultContainerExecutor();
    exec.setConf(conf);
    return exec;
  }

  @Before
  public void setup() throws IOException {
    localFS.delete(new Path(localDir.getAbsolutePath()), true);
    localFS.delete(new Path(tmpDir.getAbsolutePath()), true);
    localFS.delete(new Path(localLogDir.getAbsolutePath()), true);
    localFS.delete(new Path(remoteLogDir.getAbsolutePath()), true);
    localDir.mkdir();
    tmpDir.mkdir();
    localLogDir.mkdir();
    remoteLogDir.mkdir();
    LOG.info("Created localDir in " + localDir.getAbsolutePath());
    LOG.info("Created tmpDir in " + tmpDir.getAbsolutePath());

    String bindAddress = "0.0.0.0:5555";
    conf.set(YarnConfiguration.NM_ADDRESS, bindAddress);
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, localDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_LOG_DIRS, localLogDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR, remoteLogDir.getAbsolutePath());

    // Default delSrvc
    delSrvc = new DeletionService(exec) {
      @Override
      public void delete(String user, Path subDir, Path[] baseDirs) {
        // Don't do any deletions.
        LOG.info("Psuedo delete: user - " + user + ", subDir - " + subDir
            + ", baseDirs - " + baseDirs); 
      };
    };
    delSrvc.init(conf);

    exec = createContainerExecutor();
    containerManager =
        new ContainerManagerImpl(context, exec, delSrvc, nodeStatusUpdater,
                                 metrics, this.containerTokenSecretManager);
    containerManager.init(conf);
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    if (containerManager != null
        && containerManager.getServiceState() == STATE.STARTED) {
      containerManager.stop();
    }
    createContainerExecutor().deleteAsUser(user,
        new Path(localDir.getAbsolutePath()), new Path[] {});
  }

  public static void waitForContainerState(ContainerManager containerManager,
      ContainerId containerID, ContainerState finalState)
      throws InterruptedException, YarnRemoteException {
    waitForContainerState(containerManager, containerID, finalState, 20);
  }

  public static void waitForContainerState(ContainerManager containerManager,
          ContainerId containerID, ContainerState finalState, int timeOutMax)
          throws InterruptedException, YarnRemoteException {
    GetContainerStatusRequest request =
        recordFactory.newRecordInstance(GetContainerStatusRequest.class);
        request.setContainerId(containerID);
        ContainerStatus containerStatus =
            containerManager.getContainerStatus(request).getStatus();
        int timeoutSecs = 0;
      while (!containerStatus.getState().equals(finalState)
          && timeoutSecs++ < timeOutMax) {
          Thread.sleep(1000);
          LOG.info("Waiting for container to get into state " + finalState
              + ". Current state is " + containerStatus.getState());
          containerStatus = containerManager.getContainerStatus(request).getStatus();
        }
        LOG.info("Container state is " + containerStatus.getState());
        Assert.assertEquals("ContainerState is not correct (timedout)",
            finalState, containerStatus.getState());
      }

  static void waitForApplicationState(ContainerManagerImpl containerManager,
      ApplicationId appID, ApplicationState finalState)
      throws InterruptedException {
    // Wait for app-finish
    Application app =
        containerManager.context.getApplications().get(appID);
    int timeout = 0;
    while (!(app.getApplicationState().equals(finalState))
        && timeout++ < 15) {
      LOG.info("Waiting for app to reach " + finalState
          + ".. Current state is "
          + app.getApplicationState());
      Thread.sleep(1000);
    }
  
    Assert.assertTrue("App is not in " + finalState + " yet!! Timedout!!",
        app.getApplicationState().equals(finalState));
  }

}

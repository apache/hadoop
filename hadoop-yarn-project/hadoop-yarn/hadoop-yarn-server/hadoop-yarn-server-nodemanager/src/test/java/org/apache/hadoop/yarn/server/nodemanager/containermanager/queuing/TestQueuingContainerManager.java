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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.queuing;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusesRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.BaseContainerManagerTest;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.TestContainerManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;

import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor
    .ContainersMonitorImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.MockResourceCalculatorPlugin;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.MockResourceCalculatorProcessTree;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.Assert;
import org.junit.Test;


import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestQueuingContainerManager extends TestContainerManager {

  interface HasResources {
    boolean decide(Context context, ContainerId cId);
  }

  public TestQueuingContainerManager() throws UnsupportedFileSystemException {
    super();
  }

  static {
    LOG = LogFactory.getLog(TestQueuingContainerManager.class);
  }

  HasResources hasResources = null;
  boolean shouldDeleteWait = false;

  @Override
  protected ContainerManagerImpl
  createContainerManager(DeletionService delSrvc) {
    return new QueuingContainerManagerImpl(context, exec, delSrvc,
        nodeStatusUpdater, metrics, dirsHandler) {

      @Override
      public void serviceInit(Configuration conf) throws Exception {
        conf.set(
            YarnConfiguration.NM_CONTAINER_MON_RESOURCE_CALCULATOR,
            MockResourceCalculatorPlugin.class.getCanonicalName());
        conf.set(
            YarnConfiguration.NM_CONTAINER_MON_PROCESS_TREE,
            MockResourceCalculatorProcessTree.class.getCanonicalName());
        super.serviceInit(conf);
      }

      @Override
      public void
      setBlockNewContainerRequests(boolean blockNewContainerRequests) {
        // do nothing
      }

      @Override
      protected UserGroupInformation getRemoteUgi() throws YarnException {
        ApplicationId appId = ApplicationId.newInstance(0, 0);
        ApplicationAttemptId appAttemptId =
            ApplicationAttemptId.newInstance(appId, 1);
        UserGroupInformation ugi =
            UserGroupInformation.createRemoteUser(appAttemptId.toString());
        ugi.addTokenIdentifier(new NMTokenIdentifier(appAttemptId, context
            .getNodeId(), user, context.getNMTokenSecretManager().getCurrentKey()
            .getKeyId()));
        return ugi;
      }

      @Override
      protected void authorizeGetAndStopContainerRequest(ContainerId containerId,
          Container container, boolean stopRequest, NMTokenIdentifier identifier) throws YarnException {
        if(container == null || container.getUser().equals("Fail")){
          throw new YarnException("Reject this container");
        }
      }

      @Override
      protected ContainersMonitor createContainersMonitor(ContainerExecutor
          exec) {
        return new ContainersMonitorImpl(exec, dispatcher, this.context) {
          @Override
          public boolean hasResourcesAvailable(
              ContainersMonitorImpl.ProcessTreeInfo pti) {
            return hasResources.decide(this.context, pti.getContainerId());
          }
        };
      }
    };
  }

  @Override
  protected DeletionService createDeletionService() {
    return new DeletionService(exec) {
      @Override
      public void delete(String user, Path subDir, Path... baseDirs) {
        // Don't do any deletions.
        if (shouldDeleteWait) {
          try {
            Thread.sleep(10000);
            LOG.info("\n\nSleeping Pseudo delete : user - " + user + ", " +
                "subDir - " + subDir + ", " +
                "baseDirs - " + Arrays.asList(baseDirs));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        } else {
          LOG.info("\n\nPseudo delete : user - " + user + ", " +
              "subDir - " + subDir + ", " +
              "baseDirs - " + Arrays.asList(baseDirs));
        }
      }
    };
  }

  @Override
  public void setup() throws IOException {
    super.setup();
    shouldDeleteWait = false;
    hasResources = new HasResources() {
      @Override
      public boolean decide(Context context, ContainerId cId) {
        return true;
      }
    };
  }

  /**
   * Test to verify that an OPPORTUNISTIC container is killed when
   * a GUARANTEED container arrives and all the Node Resources are used up
   *
   * For this specific test case, 4 containers are requested (last one being
   * guaranteed). Assumptions :
   * 1) The first OPPORTUNISTIC Container will start running
   * 2) The second and third OPP containers will be queued
   * 3) When the GUARANTEED container comes in, the running OPP container
   *    will be killed to make room
   * 4) After the GUARANTEED container finishes, the remaining 2 OPP
   *    containers will be dequeued and run.
   * 5) Only the first OPP container will be killed.
   *
   * @throws Exception
   */
  @Test
  public void testSimpleOpportunisticContainer() throws Exception {
    shouldDeleteWait = true;
    containerManager.start();

    // ////// Create the resources for the container
    File dir = new File(tmpDir, "dir");
    dir.mkdirs();
    File file = new File(dir, "file");
    PrintWriter fileWriter = new PrintWriter(file);
    fileWriter.write("Hello World!");
    fileWriter.close();

    // ////// Construct the container-spec.
    ContainerLaunchContext containerLaunchContext =
        recordFactory.newRecordInstance(ContainerLaunchContext.class);
    URL resource_alpha =
        ConverterUtils.getYarnUrlFromPath(localFS
            .makeQualified(new Path(file.getAbsolutePath())));
    LocalResource rsrc_alpha =
        recordFactory.newRecordInstance(LocalResource.class);
    rsrc_alpha.setResource(resource_alpha);
    rsrc_alpha.setSize(-1);
    rsrc_alpha.setVisibility(LocalResourceVisibility.APPLICATION);
    rsrc_alpha.setType(LocalResourceType.FILE);
    rsrc_alpha.setTimestamp(file.lastModified());
    String destinationFile = "dest_file";
    Map<String, LocalResource> localResources =
        new HashMap<String, LocalResource>();
    localResources.put(destinationFile, rsrc_alpha);
    containerLaunchContext.setLocalResources(localResources);

    // Start 3 OPPORTUNISTIC containers and 1 GUARANTEED container
    List<StartContainerRequest> list = new ArrayList<>();
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(0), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(1024, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(1), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(1024, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(2), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, BuilderUtils.newResource(1024, 1),
            context.getContainerTokenSecretManager(), null,
            ExecutionType.OPPORTUNISTIC)));
    // GUARANTEED
    list.add(StartContainerRequest.newInstance(
        containerLaunchContext,
        createContainerToken(createContainerId(3), DUMMY_RM_IDENTIFIER,
            context.getNodeId(),
            user, context.getContainerTokenSecretManager())));
    StartContainersRequest allRequests =
        StartContainersRequest.newInstance(list);

    // Plugin to simulate that the Node is full
    // It only allows 1 container to run at a time.
    hasResources = new HasResources() {
      @Override
      public boolean decide(Context context, ContainerId cId) {
        int nOpp = ((QueuingContainerManagerImpl) containerManager)
            .getNumAllocatedOpportunisticContainers();
        int nGuar = ((QueuingContainerManagerImpl) containerManager)
            .getNumAllocatedGuaranteedContainers();
        boolean val = (nOpp + nGuar < 1);
        System.out.println("\nHasResources : [" + cId + "]," +
            "Opp[" + nOpp + "], Guar[" + nGuar + "], [" + val + "]\n");
        return val;
      }
    };

    containerManager.startContainers(allRequests);

    BaseContainerManagerTest.waitForContainerState(containerManager,
        createContainerId(3),
        ContainerState.COMPLETE, 40);
    List<ContainerId> statList = new ArrayList<ContainerId>();
    for (int i = 0; i < 4; i++) {
      statList.add(createContainerId(i));
    }
    GetContainerStatusesRequest statRequest =
        GetContainerStatusesRequest.newInstance(statList);
    List<ContainerStatus> containerStatuses = containerManager
        .getContainerStatuses(statRequest).getContainerStatuses();
    for (ContainerStatus status : containerStatuses) {
      // Ensure that the first opportunistic container is killed
      if (status.getContainerId().equals(createContainerId(0))) {
        Assert.assertTrue(status.getDiagnostics()
            .contains("Container killed by the ApplicationMaster"));
      }
      System.out.println("\nStatus : [" + status + "]\n");
    }
  }
}

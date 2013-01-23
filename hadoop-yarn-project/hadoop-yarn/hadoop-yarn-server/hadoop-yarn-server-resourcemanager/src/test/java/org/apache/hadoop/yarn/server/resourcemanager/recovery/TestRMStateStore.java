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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.impl.pb.ApplicationSubmissionContextPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptStoredEvent;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class TestRMStateStore {

  public static final Log LOG = LogFactory.getLog(TestRMStateStore.class);

  class TestDispatcher implements Dispatcher, EventHandler<RMAppAttemptStoredEvent> {

    ApplicationAttemptId attemptId;
    Exception storedException;

    boolean notified = false;

    @SuppressWarnings("rawtypes")
    @Override
    public void register(Class<? extends Enum> eventType, EventHandler handler) {
    }

    @Override
    public void handle(RMAppAttemptStoredEvent event) {
      assertEquals(attemptId, event.getApplicationAttemptId());
      assertEquals(storedException, event.getStoredException());
      notified = true;
      synchronized (this) {
        notifyAll();
      }
    }

    @SuppressWarnings("rawtypes")
    @Override
    public EventHandler getEventHandler() {
      return this;
    }

  }

  interface RMStateStoreHelper {
    RMStateStore getRMStateStore() throws Exception;
    void addOrphanAttemptIfNeeded(RMStateStore testStore,
                                  TestDispatcher dispatcher) throws Exception;
    boolean isFinalStateValid() throws Exception;
  }

  @Test
  public void testFSRMStateStore() throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    try {
      TestFSRMStateStoreTester fsTester = new TestFSRMStateStoreTester(cluster);
      testRMStateStore(fsTester);
    } finally {
      cluster.shutdown();
    }
  }

  class TestFSRMStateStoreTester implements RMStateStoreHelper {
    Path workingDirPathURI;
    FileSystemRMStateStore store;
    MiniDFSCluster cluster;

    class TestFileSystemRMStore extends FileSystemRMStateStore {
      TestFileSystemRMStore(Configuration conf) throws Exception {
        init(conf);
        assertTrue(workingDirPathURI.equals(fsWorkingPath));
      }
    }

    public TestFSRMStateStoreTester(MiniDFSCluster cluster) throws Exception {
      Path workingDirPath = new Path("/Test");
      this.cluster = cluster;
      FileSystem fs = cluster.getFileSystem();
      fs.mkdirs(workingDirPath);
      Path clusterURI = new Path(cluster.getURI());
      workingDirPathURI = new Path(clusterURI, workingDirPath);
      fs.close();
    }

    @Override
    public RMStateStore getRMStateStore() throws Exception {
      YarnConfiguration conf = new YarnConfiguration();
      conf.set(YarnConfiguration.FS_RM_STATE_STORE_URI, workingDirPathURI.toString());
      this.store = new TestFileSystemRMStore(conf);
      return store;
    }

    @Override
    public void addOrphanAttemptIfNeeded(RMStateStore testStore,
                                 TestDispatcher dispatcher) throws Exception {
      ApplicationAttemptId attemptId = ConverterUtils.toApplicationAttemptId(
                                      "appattempt_1352994193343_0003_000001");
      storeAttempt(testStore, attemptId,
          "container_1352994193343_0003_01_000001", dispatcher);
    }

    @Override
    public boolean isFinalStateValid() throws Exception {
      FileSystem fs = cluster.getFileSystem();
      FileStatus[] files = fs.listStatus(workingDirPathURI);
      if(files.length == 1) {
        // only store root directory should exist
        return true;
      }
      return false;
    }
  }

  void waitNotify(TestDispatcher dispatcher) {
    long startTime = System.currentTimeMillis();
    while(!dispatcher.notified) {
      synchronized (dispatcher) {
        try {
          dispatcher.wait(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
      if(System.currentTimeMillis() - startTime > 1000*60) {
        fail("Timed out attempt store notification");
      }
    }
    dispatcher.notified = false;
  }

  void storeApp(RMStateStore store, ApplicationId appId, long time)
                                                              throws Exception {
    ApplicationSubmissionContext context = new ApplicationSubmissionContextPBImpl();
    context.setApplicationId(appId);

    RMApp mockApp = mock(RMApp.class);
    when(mockApp.getApplicationId()).thenReturn(appId);
    when(mockApp.getSubmitTime()).thenReturn(time);
    when(mockApp.getApplicationSubmissionContext()).thenReturn(context);
    store.storeApplication(mockApp);
  }

  ContainerId storeAttempt(RMStateStore store, ApplicationAttemptId attemptId,
                           String containerIdStr, TestDispatcher dispatcher)
                                                             throws Exception {

    Container container = new ContainerPBImpl();
    container.setId(ConverterUtils.toContainerId(containerIdStr));
    RMAppAttempt mockAttempt = mock(RMAppAttempt.class);
    when(mockAttempt.getAppAttemptId()).thenReturn(attemptId);
    when(mockAttempt.getMasterContainer()).thenReturn(container);
    dispatcher.attemptId = attemptId;
    dispatcher.storedException = null;
    store.storeApplicationAttempt(mockAttempt);
    waitNotify(dispatcher);
    return container.getId();
  }

  void testRMStateStore(RMStateStoreHelper stateStoreHelper) throws Exception {
    long submitTime = System.currentTimeMillis();
    RMStateStore store = stateStoreHelper.getRMStateStore();
    TestDispatcher dispatcher = new TestDispatcher();
    store.setDispatcher(dispatcher);

    ApplicationAttemptId attemptId1 = ConverterUtils
        .toApplicationAttemptId("appattempt_1352994193343_0001_000001");
    ApplicationId appId1 = attemptId1.getApplicationId();
    storeApp(store, appId1, submitTime);
    ContainerId containerId1 = storeAttempt(store, attemptId1,
                 "container_1352994193343_0001_01_000001", dispatcher);
    String appAttemptIdStr2 = "appattempt_1352994193343_0001_000002";
    ApplicationAttemptId attemptId2 =
                ConverterUtils.toApplicationAttemptId(appAttemptIdStr2);
    ContainerId containerId2 = storeAttempt(store, attemptId2,
                 "container_1352994193343_0001_02_000001", dispatcher);

    ApplicationAttemptId attemptIdRemoved = ConverterUtils
        .toApplicationAttemptId("appattempt_1352994193343_0002_000001");
    ApplicationId appIdRemoved = attemptIdRemoved.getApplicationId();
    storeApp(store, appIdRemoved, submitTime);
    storeAttempt(store, attemptIdRemoved,
        "container_1352994193343_0002_01_000001", dispatcher);

    RMApp mockRemovedApp = mock(RMApp.class);
    HashMap<ApplicationAttemptId, RMAppAttempt> attempts =
                              new HashMap<ApplicationAttemptId, RMAppAttempt>();
    ApplicationSubmissionContext context = new ApplicationSubmissionContextPBImpl();
    context.setApplicationId(appIdRemoved);
    when(mockRemovedApp.getSubmitTime()).thenReturn(submitTime);
    when(mockRemovedApp.getApplicationSubmissionContext()).thenReturn(context);
    when(mockRemovedApp.getAppAttempts()).thenReturn(attempts);
    RMAppAttempt mockRemovedAttempt = mock(RMAppAttempt.class);
    when(mockRemovedAttempt.getAppAttemptId()).thenReturn(attemptIdRemoved);
    attempts.put(attemptIdRemoved, mockRemovedAttempt);
    store.removeApplication(mockRemovedApp);

    // add orphan attempt file to simulate incomplete removal of app state
    stateStoreHelper.addOrphanAttemptIfNeeded(store, dispatcher);

    // let things settle down
    Thread.sleep(1000);
    store.close();

    // load state
    store = stateStoreHelper.getRMStateStore();
    RMState state = store.loadState();
    Map<ApplicationId, ApplicationState> rmAppState = state.getApplicationState();

    // removed app or orphan attempt is not loaded
    assertEquals(1, rmAppState.size());

    ApplicationState appState = rmAppState.get(appId1);
    // app is loaded
    assertNotNull(appState);
    // app is loaded correctly
    assertEquals(submitTime, appState.getSubmitTime());
    // submission context is loaded correctly
    assertEquals(appId1,
                 appState.getApplicationSubmissionContext().getApplicationId());
    ApplicationAttemptState attemptState = appState.getAttempt(attemptId1);
    // attempt1 is loaded correctly
    assertNotNull(attemptState);
    assertEquals(attemptId1, attemptState.getAttemptId());
    // attempt1 container is loaded correctly
    assertEquals(containerId1, attemptState.getMasterContainer().getId());
    attemptState = appState.getAttempt(attemptId2);
    // attempt2 is loaded correctly
    assertNotNull(attemptState);
    assertEquals(attemptId2, attemptState.getAttemptId());
    // attempt2 container is loaded correctly
    assertEquals(containerId2, attemptState.getMasterContainer().getId());

    // assert store is in expected state after everything is cleaned
    assertTrue(stateStoreHelper.isFinalStateValid());

    store.close();
  }

}

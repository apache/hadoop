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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.crypto.SecretKey;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.security.AMRMTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestZKRMStateStorePerf extends RMStateStoreTestBase
    implements Tool {
  public static final Logger LOG =
      LoggerFactory.getLogger(TestZKRMStateStore.class);

  final String version = "0.1";

  // Configurable variables for performance test
  private int ZK_PERF_NUM_APP_DEFAULT = 1000;
  private int ZK_PERF_NUM_APPATTEMPT_PER_APP = 10;

  private final long clusterTimeStamp =  1352994193343L;

  private static final String USAGE =
      "Usage: " + TestZKRMStateStorePerf.class.getSimpleName() +
          " -appSize numberOfApplications" +
          " -appAttemptSize numberOfApplicationAttempts" +
          " [-hostPort Host:Port]" +
          " [-workingZnode rootZnodeForTesting]\n";

  private YarnConfiguration conf = null;
  private String workingZnode = "/Test";
  private ZKRMStateStore store;
  private AMRMTokenSecretManager appTokenMgr;
  private ClientToAMTokenSecretManagerInRM clientToAMTokenMgr;
  private TestingServer curatorTestingServer;

  @Before
  public void setUpZKServer() throws Exception {
    curatorTestingServer = new TestingServer();
  }

  @After
  public void tearDown() throws Exception {
    if (store != null) {
      store.stop();
    }
    if (appTokenMgr != null) {
      appTokenMgr.stop();
    }
    if (curatorTestingServer != null) {
      curatorTestingServer.stop();
    }
  }

  private void initStore(String hostPort) {
    Optional<String> optHostPort = Optional.ofNullable(hostPort);
    RMContext rmContext = mock(RMContext.class);

    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_ZK_ADDRESS, optHostPort
        .orElse((curatorTestingServer == null) ? "" : curatorTestingServer
            .getConnectString()));
    conf.set(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH, workingZnode);

    store = new ZKRMStateStore();
    store.setResourceManager(new ResourceManager());
    store.init(conf);
    store.start();
    when(rmContext.getStateStore()).thenReturn(store);
    appTokenMgr = new AMRMTokenSecretManager(conf, rmContext);
    appTokenMgr.start();
    clientToAMTokenMgr = new ClientToAMTokenSecretManagerInRM();
  }

  @SuppressWarnings("unchecked")
  @Override
  public int run(String[] args) {
    LOG.info("Starting ZKRMStateStorePerf ver." + version);

    int numApp = ZK_PERF_NUM_APP_DEFAULT;
    int numAppAttemptPerApp = ZK_PERF_NUM_APPATTEMPT_PER_APP;
    String hostPort = null;
    boolean launchLocalZK= true;

    if (args.length == 0) {
      System.err.println("Missing arguments.");
      return -1;
    }

    for (int i = 0; i < args.length; i++) { // parse command line
      if (args[i].equalsIgnoreCase("-appsize")) {
        numApp = Integer.parseInt(args[++i]);
      } else if (args[i].equalsIgnoreCase("-appattemptsize")) {
        numAppAttemptPerApp = Integer.parseInt(args[++i]);
      } else if (args[i].equalsIgnoreCase("-hostPort")) {
        hostPort = args[++i];
        launchLocalZK = false;
      } else if (args[i].equalsIgnoreCase("-workingZnode"))  {
        workingZnode = args[++i];
      } else {
        System.err.println("Illegal argument: " + args[i]);
        return -1;
      }
    }

    if (launchLocalZK) {
      try {
        setUpZKServer();
      } catch (Exception e) {
        System.err.println("failed to setup. : " + e.getMessage());
        return -1;
      }
    }

    initStore(hostPort);

    long submitTime = System.currentTimeMillis();
    long startTime = System.currentTimeMillis() + 1234;

    ArrayList<ApplicationId> applicationIds = new ArrayList<>();
    ArrayList<RMApp> rmApps = new ArrayList<>();
    ArrayList<ApplicationAttemptId> attemptIds = new ArrayList<>();
    HashMap<ApplicationId, Set<ApplicationAttemptId>> appIdsToAttemptId =
        new HashMap<>();
    TestDispatcher dispatcher = new TestDispatcher();
    store.setRMDispatcher(dispatcher);

    for (int i = 0; i < numApp; i++) {
      ApplicationId appId = ApplicationId.newInstance(clusterTimeStamp, i);
      applicationIds.add(appId);
      ArrayList<ApplicationAttemptId> attemptIdsForThisApp =
          new ArrayList<>();
      for (int j = 0; j < numAppAttemptPerApp; j++) {
        ApplicationAttemptId attemptId =
            ApplicationAttemptId.newInstance(appId, j);
        attemptIdsForThisApp.add(attemptId);
      }
      appIdsToAttemptId.put(appId, new LinkedHashSet(attemptIdsForThisApp));
      attemptIds.addAll(attemptIdsForThisApp);
    }

    for (ApplicationId appId : applicationIds) {
      RMApp app = null;
      try {
        app = storeApp(store, appId, submitTime, startTime);
      } catch (Exception e) {
        System.err.println("failed to create Application Znode. : "
            + e.getMessage());
        return -1;
      }
      waitNotify(dispatcher);
      rmApps.add(app);
    }

    for (ApplicationAttemptId attemptId : attemptIds) {
      Token<AMRMTokenIdentifier> tokenId =
          generateAMRMToken(attemptId, appTokenMgr);
      SecretKey clientTokenKey =
          clientToAMTokenMgr.createMasterKey(attemptId);
      try {
        storeAttempt(store, attemptId,
            ContainerId.newContainerId(attemptId, 0L).toString(),
            tokenId, clientTokenKey, dispatcher);
      } catch (Exception e) {
        System.err.println("failed to create AppAttempt Znode. : "
            + e.getMessage());
        return -1;
      }
    }

    long storeStart = System.currentTimeMillis();
    try {
      store.loadState();
    } catch (Exception e) {
      System.err.println("failed to locaState from ZKRMStateStore. : "
          + e.getMessage());
      return -1;
    }
    long storeEnd = System.currentTimeMillis();

    long loadTime = storeEnd - storeStart;

    String resultMsg =  "ZKRMStateStore takes " + loadTime + " msec to loadState.";
    LOG.info(resultMsg);
    System.out.println(resultMsg);

    // cleanup
    try {
      for (RMApp app : rmApps) {
        ApplicationStateData appState =
            ApplicationStateData.newInstance(app.getSubmitTime(),
                app.getStartTime(), app.getApplicationSubmissionContext(),
                app.getUser());
        ApplicationId appId = app.getApplicationId();
        Map m = mock(Map.class);
        when(m.keySet()).thenReturn(appIdsToAttemptId.get(appId));
        appState.attempts = m;
        store.removeApplicationStateInternal(appState);
      }
    } catch (Exception e) {
      System.err.println("failed to cleanup. : " + e.getMessage());
      return -1;
    }

    return 0;
  }

  @Override
  public void setConf(Configuration conf) {
    // currently this function is just ignored
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Test
  public void perfZKRMStateStore() throws Exception {
    String[] args = {
        "-appSize", String.valueOf(ZK_PERF_NUM_APP_DEFAULT),
        "-appAttemptSize", String.valueOf(ZK_PERF_NUM_APPATTEMPT_PER_APP)
    };
    run(args);
  }

  static public void main(String[] args) throws Exception {
    TestZKRMStateStorePerf perf = new TestZKRMStateStorePerf();

    int res = -1;
    try {
      res = ToolRunner.run(perf, args);
    } catch(Exception e) {
      System.err.print(StringUtils.stringifyException(e));
      res = -2;
    }
    if(res == -1) {
      System.err.print(USAGE);
    }
    System.exit(res);
  }
}

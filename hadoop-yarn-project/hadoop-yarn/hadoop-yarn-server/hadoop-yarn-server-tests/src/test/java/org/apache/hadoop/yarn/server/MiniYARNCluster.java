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

package org.apache.hadoop.yarn.server;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.yarn.api.protocolrecords.GetClusterMetricsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryServer;
import org.apache.hadoop.yarn.server.applicationhistoryservice.ApplicationHistoryStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.MemoryApplicationHistoryStore;
import org.apache.hadoop.yarn.server.applicationhistoryservice.webapp.AHSWebApp;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeHealthCheckerService;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdater;
import org.apache.hadoop.yarn.server.nodemanager.NodeStatusUpdaterImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceTrackerService;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptRegistrationEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.event.RMAppAttemptUnregistrationEvent;
import org.apache.hadoop.yarn.server.timeline.MemoryTimelineStore;
import org.apache.hadoop.yarn.server.timeline.TimelineStore;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * Embedded Yarn minicluster for testcases that need to interact with a cluster.
 * <p/>
 * In a real cluster, resource request matching is done using the hostname, and
 * by default Yarn minicluster works in the exact same way as a real cluster.
 * <p/>
 * If a testcase needs to use multiple nodes and exercise resource request 
 * matching to a specific node, then the property 
 * {@YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME} should be set
 * <code>true</code> in the configuration used to initialize the minicluster.
 * <p/>
 * With this property set to <code>true</code>, the matching will be done using
 * the <code>hostname:port</code> of the namenodes. In such case, the AM must
 * do resource request using <code>hostname:port</code> as the location.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class MiniYARNCluster extends CompositeService {

  private static final Log LOG = LogFactory.getLog(MiniYARNCluster.class);

  // temp fix until metrics system can auto-detect itself running in unit test:
  static {
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  private NodeManager[] nodeManagers;
  private ResourceManager[] resourceManagers;
  private String[] rmIds;

  private ApplicationHistoryServer appHistoryServer;

  private boolean useFixedPorts;
  private boolean useRpc = false;
  private int failoverTimeout;

  private ConcurrentMap<ApplicationAttemptId, Long> appMasters =
      new ConcurrentHashMap<ApplicationAttemptId, Long>(16, 0.75f, 2);
  
  private File testWorkDir;

  // Number of nm-local-dirs per nodemanager
  private int numLocalDirs;
  // Number of nm-log-dirs per nodemanager
  private int numLogDirs;
  private boolean enableAHS;

  /**
   * @param testName name of the test
   * @param numResourceManagers the number of resource managers in the cluster
   * @param numNodeManagers the number of node managers in the cluster
   * @param numLocalDirs the number of nm-local-dirs per nodemanager
   * @param numLogDirs the number of nm-log-dirs per nodemanager
   * @param enableAHS enable ApplicationHistoryServer or not
   */
  public MiniYARNCluster(
      String testName, int numResourceManagers, int numNodeManagers,
      int numLocalDirs, int numLogDirs, boolean enableAHS) {
    super(testName.replace("$", ""));
    this.numLocalDirs = numLocalDirs;
    this.numLogDirs = numLogDirs;
    this.enableAHS = enableAHS;
    String testSubDir = testName.replace("$", "");
    File targetWorkDir = new File("target", testSubDir);
    try {
      FileContext.getLocalFSFileContext().delete(
          new Path(targetWorkDir.getAbsolutePath()), true);
    } catch (Exception e) {
      LOG.warn("COULD NOT CLEANUP", e);
      throw new YarnRuntimeException("could not cleanup test dir: "+ e, e);
    } 

    if (Shell.WINDOWS) {
      // The test working directory can exceed the maximum path length supported
      // by some Windows APIs and cmd.exe (260 characters).  To work around this,
      // create a symlink in temporary storage with a much shorter path,
      // targeting the full path to the test working directory.  Then, use the
      // symlink as the test working directory.
      String targetPath = targetWorkDir.getAbsolutePath();
      File link = new File(System.getProperty("java.io.tmpdir"),
        String.valueOf(System.currentTimeMillis()));
      String linkPath = link.getAbsolutePath();

      try {
        FileContext.getLocalFSFileContext().delete(new Path(linkPath), true);
      } catch (IOException e) {
        throw new YarnRuntimeException("could not cleanup symlink: " + linkPath, e);
      }

      // Guarantee target exists before creating symlink.
      targetWorkDir.mkdirs();

      ShellCommandExecutor shexec = new ShellCommandExecutor(
        Shell.getSymlinkCommand(targetPath, linkPath));
      try {
        shexec.execute();
      } catch (IOException e) {
        throw new YarnRuntimeException(String.format(
          "failed to create symlink from %s to %s, shell output: %s", linkPath,
          targetPath, shexec.getOutput()), e);
      }

      this.testWorkDir = link;
    } else {
      this.testWorkDir = targetWorkDir;
    }

    resourceManagers = new ResourceManager[numResourceManagers];
    nodeManagers = new NodeManager[numNodeManagers];
  }

  /**
   * @param testName name of the test
   * @param numResourceManagers the number of resource managers in the cluster
   * @param numNodeManagers the number of node managers in the cluster
   * @param numLocalDirs the number of nm-local-dirs per nodemanager
   * @param numLogDirs the number of nm-log-dirs per nodemanager
   */
  public MiniYARNCluster(
      String testName, int numResourceManagers, int numNodeManagers,
      int numLocalDirs, int numLogDirs) {
    this(testName, numResourceManagers, numNodeManagers, numLocalDirs,
        numLogDirs, false);
  }

  /**
   * @param testName name of the test
   * @param numNodeManagers the number of node managers in the cluster
   * @param numLocalDirs the number of nm-local-dirs per nodemanager
   * @param numLogDirs the number of nm-log-dirs per nodemanager
   */
  public MiniYARNCluster(String testName, int numNodeManagers,
                         int numLocalDirs, int numLogDirs) {
    this(testName, 1, numNodeManagers, numLocalDirs, numLogDirs);
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    useFixedPorts = conf.getBoolean(
        YarnConfiguration.YARN_MINICLUSTER_FIXED_PORTS,
        YarnConfiguration.DEFAULT_YARN_MINICLUSTER_FIXED_PORTS);
    useRpc = conf.getBoolean(YarnConfiguration.YARN_MINICLUSTER_USE_RPC,
        YarnConfiguration.DEFAULT_YARN_MINICLUSTER_USE_RPC);
    failoverTimeout = conf.getInt(YarnConfiguration.RM_ZK_TIMEOUT_MS,
        YarnConfiguration.DEFAULT_RM_ZK_TIMEOUT_MS);

    if (useRpc && !useFixedPorts) {
      throw new YarnRuntimeException("Invalid configuration!" +
          " Minicluster can use rpc only when configured to use fixed ports");
    }

    conf.setBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, true);
    if (resourceManagers.length > 1) {
      conf.setBoolean(YarnConfiguration.RM_HA_ENABLED, true);
      if (conf.get(YarnConfiguration.RM_HA_IDS) == null) {
        StringBuilder rmIds = new StringBuilder();
        for (int i = 0; i < resourceManagers.length; i++) {
          if (i != 0) {
            rmIds.append(",");
          }
          rmIds.append("rm" + i);
        }
        conf.set(YarnConfiguration.RM_HA_IDS, rmIds.toString());
      }
      Collection<String> rmIdsCollection = HAUtil.getRMHAIds(conf);
      rmIds = rmIdsCollection.toArray(new String[rmIdsCollection.size()]);
    }

    for (int i = 0; i < resourceManagers.length; i++) {
      resourceManagers[i] = createResourceManager();
      if (!useFixedPorts) {
        if (HAUtil.isHAEnabled(conf)) {
          setHARMConfiguration(i, conf);
        } else {
          setNonHARMConfiguration(conf);
        }
      }
      addService(new ResourceManagerWrapper(i));
    }
    for(int index = 0; index < nodeManagers.length; index++) {
      nodeManagers[index] =
          useRpc ? new CustomNodeManager() : new ShortCircuitedNodeManager();
      addService(new NodeManagerWrapper(index));
    }

    if (enableAHS) {
      addService(new ApplicationHistoryServerWrapper());
    }
    
    super.serviceInit(
        conf instanceof YarnConfiguration ? conf : new YarnConfiguration(conf));
  }

  private void setNonHARMConfiguration(Configuration conf) {
    String hostname = MiniYARNCluster.getHostname();
    conf.set(YarnConfiguration.RM_ADDRESS, hostname + ":0");
    conf.set(YarnConfiguration.RM_ADMIN_ADDRESS, hostname + ":0");
    conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, hostname + ":0");
    conf.set(YarnConfiguration.RM_RESOURCE_TRACKER_ADDRESS, hostname + ":0");
    WebAppUtils.setRMWebAppHostnameAndPort(conf, hostname, 0);
  }

  private void setHARMConfiguration(final int index, Configuration conf) {
    String hostname = MiniYARNCluster.getHostname();
    for (String confKey : YarnConfiguration.getServiceAddressConfKeys(conf)) {
      conf.set(HAUtil.addSuffix(confKey, rmIds[index]), hostname + ":0");
    }
  }

  private synchronized void initResourceManager(int index, Configuration conf) {
    if (HAUtil.isHAEnabled(conf)) {
      conf.set(YarnConfiguration.RM_HA_ID, rmIds[index]);
    }
    resourceManagers[index].init(conf);
    resourceManagers[index].getRMContext().getDispatcher().register(
        RMAppAttemptEventType.class,
        new EventHandler<RMAppAttemptEvent>() {
          public void handle(RMAppAttemptEvent event) {
            if (event instanceof RMAppAttemptRegistrationEvent) {
              appMasters.put(event.getApplicationAttemptId(),
                  event.getTimestamp());
            } else if (event instanceof RMAppAttemptUnregistrationEvent) {
              appMasters.remove(event.getApplicationAttemptId());
            }
          }
        });
  }

  private synchronized void startResourceManager(final int index) {
    try {
      Thread rmThread = new Thread() {
        public void run() {
          resourceManagers[index].start();
        }
      };
      rmThread.setName("RM-" + index);
      rmThread.start();
      int waitCount = 0;
      while (resourceManagers[index].getServiceState() == STATE.INITED
          && waitCount++ < 60) {
        LOG.info("Waiting for RM to start...");
        Thread.sleep(1500);
      }
      if (resourceManagers[index].getServiceState() != STATE.STARTED) {
        // RM could have failed.
        throw new IOException(
            "ResourceManager failed to start. Final state is "
                + resourceManagers[index].getServiceState());
      }
    } catch (Throwable t) {
      throw new YarnRuntimeException(t);
    }
    LOG.info("MiniYARN ResourceManager address: " +
        getConfig().get(YarnConfiguration.RM_ADDRESS));
    LOG.info("MiniYARN ResourceManager web address: " +
        WebAppUtils.getRMWebAppURLWithoutScheme(getConfig()));
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  public synchronized void stopResourceManager(int index) {
    if (resourceManagers[index] != null) {
      resourceManagers[index].stop();
      resourceManagers[index] = null;
    }
  }

  @InterfaceAudience.Private
  @VisibleForTesting
  public synchronized void restartResourceManager(int index)
      throws InterruptedException {
    if (resourceManagers[index] != null) {
      resourceManagers[index].stop();
      resourceManagers[index] = null;
    }
    Configuration conf = getConfig();
    resourceManagers[index] = new ResourceManager();
    initResourceManager(index, getConfig());
    startResourceManager(index);
  }

  public File getTestWorkDir() {
    return testWorkDir;
  }

  /**
   * In a HA cluster, go through all the RMs and find the Active RM. In a
   * non-HA cluster, return the index of the only RM.
   *
   * @return index of the active RM or -1 if none of them turn active
   */
  @InterfaceAudience.Private
  @VisibleForTesting
  public int getActiveRMIndex() {
    if (resourceManagers.length == 1) {
      return 0;
    }

    int numRetriesForRMBecomingActive = failoverTimeout / 100;
    while (numRetriesForRMBecomingActive-- > 0) {
      for (int i = 0; i < resourceManagers.length; i++) {
        if (resourceManagers[i] == null) {
          continue;
        }
        try {
          if (HAServiceProtocol.HAServiceState.ACTIVE ==
              resourceManagers[i].getRMContext().getRMAdminService()
                  .getServiceStatus().getState()) {
            return i;
          }
        } catch (IOException e) {
          throw new YarnRuntimeException("Couldn't read the status of " +
              "a ResourceManger in the HA ensemble.", e);
        }
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new YarnRuntimeException("Interrupted while waiting for one " +
            "of the ResourceManagers to become active");
      }
    }
    return -1;
  }

  /**
   * @return the active {@link ResourceManager} of the cluster,
   * null if none of them are active.
   */
  public ResourceManager getResourceManager() {
    int activeRMIndex = getActiveRMIndex();
    return activeRMIndex == -1
        ? null
        : this.resourceManagers[activeRMIndex];
  }

  public ResourceManager getResourceManager(int i) {
    return this.resourceManagers[i];
  }

  public NodeManager getNodeManager(int i) {
    return this.nodeManagers[i];
  }

  public static String getHostname() {
    try {
      return InetAddress.getLocalHost().getHostName();
    }
    catch (UnknownHostException ex) {
      throw new RuntimeException(ex);
    }
  }

  private class ResourceManagerWrapper extends AbstractService {
    private int index;

    public ResourceManagerWrapper(int i) {
      super(ResourceManagerWrapper.class.getName() + "_" + i);
      index = i;
    }

    @Override
    protected synchronized void serviceInit(Configuration conf)
        throws Exception {
      initResourceManager(index, conf);
      super.serviceInit(conf);
    }

    @Override
    protected synchronized void serviceStart() throws Exception {
      startResourceManager(index);
      LOG.info("MiniYARN ResourceManager address: " +
               getConfig().get(YarnConfiguration.RM_ADDRESS));
      LOG.info("MiniYARN ResourceManager web address: " +
               WebAppUtils.getRMWebAppURLWithoutScheme(getConfig()));
      super.serviceStart();
    }

    private void waitForAppMastersToFinish(long timeoutMillis) throws InterruptedException {
      long started = System.currentTimeMillis();
      synchronized (appMasters) {
        while (!appMasters.isEmpty() && System.currentTimeMillis() - started < timeoutMillis) {
          appMasters.wait(1000);
        }
      }
      if (!appMasters.isEmpty()) {
        LOG.warn("Stopping RM while some app masters are still alive");
      }
    }
    
    @Override
    protected synchronized void serviceStop() throws Exception {
      if (resourceManagers[index] != null) {
        waitForAppMastersToFinish(5000);
        resourceManagers[index].stop();
      }

      if (Shell.WINDOWS) {
        // On Windows, clean up the short temporary symlink that was created to
        // work around path length limitation.
        String testWorkDirPath = testWorkDir.getAbsolutePath();
        try {
          FileContext.getLocalFSFileContext().delete(new Path(testWorkDirPath),
            true);
        } catch (IOException e) {
          LOG.warn("could not cleanup symlink: " +
            testWorkDir.getAbsolutePath());
        }
      }
      super.serviceStop();
    }
  }

  private class NodeManagerWrapper extends AbstractService {
    int index = 0;

    public NodeManagerWrapper(int i) {
      super(NodeManagerWrapper.class.getName() + "_" + i);
      index = i;
    }

    protected synchronized void serviceInit(Configuration conf)
        throws Exception {
      Configuration config = new YarnConfiguration(conf);
      // create nm-local-dirs and configure them for the nodemanager
      String localDirsString = prepareDirs("local", numLocalDirs);
      config.set(YarnConfiguration.NM_LOCAL_DIRS, localDirsString);
      // create nm-log-dirs and configure them for the nodemanager
      String logDirsString = prepareDirs("log", numLogDirs);
      config.set(YarnConfiguration.NM_LOG_DIRS, logDirsString);

      // By default AM + 2 containers
      config.setInt(YarnConfiguration.NM_PMEM_MB, 4*1024);
      config.set(YarnConfiguration.NM_ADDRESS,
          MiniYARNCluster.getHostname() + ":0");
      config.set(YarnConfiguration.NM_LOCALIZER_ADDRESS,
          MiniYARNCluster.getHostname() + ":0");
      WebAppUtils
          .setNMWebAppHostNameAndPort(config,
              MiniYARNCluster.getHostname(), 0);

      // Disable resource checks by default
      if (!config.getBoolean(
          YarnConfiguration.YARN_MINICLUSTER_CONTROL_RESOURCE_MONITORING,
          YarnConfiguration.
              DEFAULT_YARN_MINICLUSTER_CONTROL_RESOURCE_MONITORING)) {
        config.setBoolean(YarnConfiguration.NM_PMEM_CHECK_ENABLED, false);
        config.setBoolean(YarnConfiguration.NM_VMEM_CHECK_ENABLED, false);
      }

      LOG.info("Starting NM: " + index);
      nodeManagers[index].init(config);
      super.serviceInit(config);
    }

    /**
     * Create local/log directories
     * @param dirType type of directories i.e. local dirs or log dirs 
     * @param numDirs number of directories
     * @return the created directories as a comma delimited String
     */
    private String prepareDirs(String dirType, int numDirs) {
      File []dirs = new File[numDirs];
      String dirsString = "";
      for (int i = 0; i < numDirs; i++) {
        dirs[i]= new File(testWorkDir, MiniYARNCluster.this.getName()
            + "-" + dirType + "Dir-nm-" + index + "_" + i);
        dirs[i].mkdirs();
        LOG.info("Created " + dirType + "Dir in " + dirs[i].getAbsolutePath());
        String delimiter = (i > 0) ? "," : "";
        dirsString = dirsString.concat(delimiter + dirs[i].getAbsolutePath());
      }
      return dirsString;
    }

    protected synchronized void serviceStart() throws Exception {
      try {
        new Thread() {
          public void run() {
            nodeManagers[index].start();
          }
        }.start();
        int waitCount = 0;
        while (nodeManagers[index].getServiceState() == STATE.INITED
            && waitCount++ < 60) {
          LOG.info("Waiting for NM " + index + " to start...");
          Thread.sleep(1000);
        }
        if (nodeManagers[index].getServiceState() != STATE.STARTED) {
          // RM could have failed.
          throw new IOException("NodeManager " + index + " failed to start");
        }
        super.serviceStart();
      } catch (Throwable t) {
        throw new YarnRuntimeException(t);
      }
    }

    @Override
    protected synchronized void serviceStop() throws Exception {
      if (nodeManagers[index] != null) {
        nodeManagers[index].stop();
      }
      super.serviceStop();
    }
  }

  private class CustomNodeManager extends NodeManager {
    @Override
    protected void doSecureLogin() throws IOException {
      // Don't try to login using keytab in the testcase.
    }
  }

  private class ShortCircuitedNodeManager extends CustomNodeManager {
    @Override
    protected NodeStatusUpdater createNodeStatusUpdater(Context context,
        Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
      return new NodeStatusUpdaterImpl(context, dispatcher,
          healthChecker, metrics) {
        @Override
        protected ResourceTracker getRMClient() {
          final ResourceTrackerService rt =
              getResourceManager().getResourceTrackerService();
          final RecordFactory recordFactory =
            RecordFactoryProvider.getRecordFactory(null);

          // For in-process communication without RPC
          return new ResourceTracker() {

            @Override
            public NodeHeartbeatResponse nodeHeartbeat(
                NodeHeartbeatRequest request) throws YarnException,
                IOException {
              NodeHeartbeatResponse response;
              try {
                response = rt.nodeHeartbeat(request);
              } catch (YarnException e) {
                LOG.info("Exception in heartbeat from node " + 
                    request.getNodeStatus().getNodeId(), e);
                throw e;
              }
              return response;
            }

            @Override
            public RegisterNodeManagerResponse registerNodeManager(
                RegisterNodeManagerRequest request)
                throws YarnException, IOException {
              RegisterNodeManagerResponse response;
              try {
                response = rt.registerNodeManager(request);
              } catch (YarnException e) {
                LOG.info("Exception in node registration from "
                    + request.getNodeId().toString(), e);
                throw e;
              }
              return response;
            }
          };
        }

        @Override
        protected void stopRMProxy() { }
      };
    }
  }

  /**
   * Wait for all the NodeManagers to connect to the ResourceManager.
   *
   * @param timeout Time to wait (sleeps in 100 ms intervals) in milliseconds.
   * @return true if all NodeManagers connect to the (Active)
   * ResourceManager, false otherwise.
   * @throws YarnException
   * @throws InterruptedException
   */
  public boolean waitForNodeManagersToConnect(long timeout)
      throws YarnException, InterruptedException {
    GetClusterMetricsRequest req = GetClusterMetricsRequest.newInstance();
    for (int i = 0; i < timeout / 100; i++) {
      ResourceManager rm = getResourceManager();
      if (rm == null) {
        throw new YarnException("Can not find the active RM.");
      }
      else if (nodeManagers.length == rm.getClientRMService()
            .getClusterMetrics(req).getClusterMetrics().getNumNodeManagers()) {
        return true;
      }
      Thread.sleep(100);
    }
    return false;
  }

  private class ApplicationHistoryServerWrapper extends AbstractService {
    public ApplicationHistoryServerWrapper() {
      super(ApplicationHistoryServerWrapper.class.getName());
    }

    @Override
    protected synchronized void serviceInit(Configuration conf)
        throws Exception {
      appHistoryServer = new ApplicationHistoryServer();
      conf.setClass(YarnConfiguration.APPLICATION_HISTORY_STORE,
          MemoryApplicationHistoryStore.class, ApplicationHistoryStore.class);
      conf.setClass(YarnConfiguration.TIMELINE_SERVICE_STORE,
          MemoryTimelineStore.class, TimelineStore.class);
      appHistoryServer.init(conf);
      super.serviceInit(conf);
    }

    @Override
    protected synchronized void serviceStart() throws Exception {
      try {
        new Thread() {
          public void run() {
            appHistoryServer.start();
          };
        }.start();
        int waitCount = 0;
        while (appHistoryServer.getServiceState() == STATE.INITED
            && waitCount++ < 60) {
          LOG.info("Waiting for Timeline Server to start...");
          Thread.sleep(1500);
        }
        if (appHistoryServer.getServiceState() != STATE.STARTED) {
          // AHS could have failed.
          throw new IOException(
              "ApplicationHistoryServer failed to start. Final state is "
                  + appHistoryServer.getServiceState());
        }
        super.serviceStart();
      } catch (Throwable t) {
        throw new YarnRuntimeException(t);
      }
      LOG.info("MiniYARN ApplicationHistoryServer address: "
          + getConfig().get(YarnConfiguration.TIMELINE_SERVICE_ADDRESS));
      LOG.info("MiniYARN ApplicationHistoryServer web address: "
          + getConfig().get(YarnConfiguration.TIMELINE_SERVICE_WEBAPP_ADDRESS));
    }

    @Override
    protected synchronized void serviceStop() throws Exception {
      if (appHistoryServer != null) {
        appHistoryServer.stop();
      }
      AHSWebApp.resetInstance();
      super.serviceStop();
    }
  }

  public ApplicationHistoryServer getApplicationHistoryServer() {
    return this.appHistoryServer;
  }

  protected ResourceManager createResourceManager() {
    return new ResourceManager(){
      @Override
      protected void doSecureLogin() throws IOException {
        // Don't try to login using keytab in the testcases.
      }
    };
  }

  public int getNumOfResourceManager() {
    return this.resourceManagers.length;
  }
}

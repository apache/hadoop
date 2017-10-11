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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.api.ResourceTracker;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.NodeHeartbeatResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.RegisterNodeManagerResponsePBImpl;
import org.apache.hadoop.yarn.server.api.protocolrecords.impl.pb.UnRegisterNodeManagerResponsePBImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class NodeManagerTestBase {
  // temp fix until metrics system can auto-detect itself running in unit test:
  static {
    DefaultMetricsSystem.setMiniClusterMode(true);
  }

  protected static final Logger LOG =
      LoggerFactory.getLogger(TestNodeStatusUpdater.class);
  protected static final File basedir =
      new File("target", TestNodeStatusUpdater.class.getName());
  protected static final File nmLocalDir = new File(basedir, "nm0");
  protected static final File tmpDir = new File(basedir, "tmpDir");
  protected static final File remoteLogsDir = new File(basedir, "remotelogs");
  protected static final File logsDir = new File(basedir, "logs");
  protected static final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);
  protected Configuration conf;

  protected YarnConfiguration createNMConfig() throws IOException {
    return createNMConfig(ServerSocketUtil.getPort(49170, 10));
  }

  protected YarnConfiguration createNMConfig(int port) throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    String localhostAddress = null;
    try {
      localhostAddress = InetAddress.getByName("localhost")
          .getCanonicalHostName();
    } catch (UnknownHostException e) {
      Assert.fail("Unable to get localhost address: " + e.getMessage());
    }
    conf.setInt(YarnConfiguration.NM_PMEM_MB, 5 * 1024); // 5GB
    conf.set(YarnConfiguration.NM_ADDRESS, localhostAddress + ":" + port);
    conf.set(YarnConfiguration.NM_LOCALIZER_ADDRESS, localhostAddress + ":"
        + ServerSocketUtil.getPort(49160, 10));
    conf.set(YarnConfiguration.NM_LOG_DIRS, logsDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_REMOTE_APP_LOG_DIR,
        remoteLogsDir.getAbsolutePath());
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, nmLocalDir.getAbsolutePath());
    conf.setLong(YarnConfiguration.NM_LOG_RETAIN_SECONDS, 1);
    return conf;
  }

  public static class BaseResourceTrackerForTest implements ResourceTracker {
    @Override
    public RegisterNodeManagerResponse registerNodeManager(
        RegisterNodeManagerRequest request) throws YarnException, IOException {
      return new RegisterNodeManagerResponsePBImpl();
    }

    @Override
    public NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
        throws YarnException, IOException {
      return new NodeHeartbeatResponsePBImpl();
    }

    @Override
    public UnRegisterNodeManagerResponse unRegisterNodeManager(
        UnRegisterNodeManagerRequest request)
        throws YarnException, IOException {
      return new UnRegisterNodeManagerResponsePBImpl();
    }
  }

  protected static class BaseNodeStatusUpdaterForTest extends NodeStatusUpdaterImpl {
    public ResourceTracker resourceTracker;
    protected Context context;

    public BaseNodeStatusUpdaterForTest(Context context, Dispatcher dispatcher,
        NodeHealthCheckerService healthChecker, NodeManagerMetrics metrics,
        ResourceTracker resourceTracker) {
      super(context, dispatcher, healthChecker, metrics);
      this.context = context;
      this.resourceTracker = resourceTracker;
    }
    @Override
    protected ResourceTracker getRMClient() {
      return resourceTracker;
    }

    @Override
    protected void stopRMProxy() {
      return;
    }
  }

  public class MyContainerManager extends ContainerManagerImpl {
    public boolean signaled = false;

    public MyContainerManager(Context context, ContainerExecutor exec,
        DeletionService deletionContext, NodeStatusUpdater nodeStatusUpdater,
        NodeManagerMetrics metrics,
        LocalDirsHandlerService dirsHandler) {
      super(context, exec, deletionContext, nodeStatusUpdater,
          metrics, dirsHandler);
    }

    @Override
    public void handle(ContainerManagerEvent event) {
      if (event.getType() == ContainerManagerEventType.SIGNAL_CONTAINERS) {
        signaled = true;
      }
    }
  }

  @Before
  public void setUp() throws IOException {
    nmLocalDir.mkdirs();
    tmpDir.mkdirs();
    logsDir.mkdirs();
    remoteLogsDir.mkdirs();
    conf = createNMConfig();
  }
}

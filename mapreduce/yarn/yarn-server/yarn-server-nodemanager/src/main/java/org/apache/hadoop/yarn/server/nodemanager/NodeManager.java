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

import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.NM_CONTAINER_EXECUTOR_CLASS;
import static org.apache.hadoop.yarn.server.nodemanager.NMConfig.NM_KEYTAB;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.NodeHealthCheckerService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.yarn.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.YarnServerConfig;
import org.apache.hadoop.yarn.server.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.ContainerManagerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.application.Application;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.metrics.NodeManagerMetrics;
import org.apache.hadoop.yarn.server.nodemanager.webapp.WebServer;
import org.apache.hadoop.yarn.service.CompositeService;
import org.apache.hadoop.yarn.service.Service;

public class NodeManager extends CompositeService {
  protected final NodeManagerMetrics metrics = NodeManagerMetrics.create();

  public NodeManager() {
    super(NodeManager.class.getName());
  }

  protected NodeStatusUpdater createNodeStatusUpdater(Context context,
      Dispatcher dispatcher, NodeHealthCheckerService healthChecker) {
    return new NodeStatusUpdaterImpl(context, dispatcher, healthChecker,
                                     metrics);
  }

  protected NodeResourceMonitor createNodeResourceMonitor() {
    return new NodeResourceMonitorImpl();
  }

  protected ContainerManagerImpl createContainerManager(Context context,
      ContainerExecutor exec, DeletionService del,
      NodeStatusUpdater nodeStatusUpdater) {
    return new ContainerManagerImpl(context, exec, del, nodeStatusUpdater,
                                    metrics);
  }

  protected WebServer createWebServer(Context nmContext,
      ResourceView resourceView) {
    return new WebServer(nmContext, resourceView);
  }

  protected void doSecureLogin() throws IOException {
    SecurityUtil.login(getConfig(), NM_KEYTAB,
        YarnServerConfig.NM_SERVER_PRINCIPAL_KEY);
  }

  @Override
  public void init(Configuration conf) {

    Context context = new NMContext();

    ContainerExecutor exec = ReflectionUtils.newInstance(
        conf.getClass(NM_CONTAINER_EXECUTOR_CLASS,
          DefaultContainerExecutor.class, ContainerExecutor.class), conf);
    DeletionService del = new DeletionService(exec);
    addService(del);

    // NodeManager level dispatcher
    AsyncDispatcher dispatcher = new AsyncDispatcher();

    NodeHealthCheckerService healthChecker = null;
    if (NodeHealthCheckerService.shouldRun(conf)) {
      healthChecker = new NodeHealthCheckerService();
      addService(healthChecker);
    }

    // StatusUpdater should be added first so that it can start first. Once it
    // contacts RM, does registration and gets tokens, then only
    // ContainerManager can start.
    NodeStatusUpdater nodeStatusUpdater =
        createNodeStatusUpdater(context, dispatcher, healthChecker);
    addService(nodeStatusUpdater);

    NodeResourceMonitor nodeResourceMonitor = createNodeResourceMonitor();
    addService(nodeResourceMonitor);

    ContainerManagerImpl containerManager =
        createContainerManager(context, exec, del, nodeStatusUpdater);
    addService(containerManager);

    Service webServer =
        createWebServer(context, containerManager.getContainersMonitor());
    addService(webServer);

    dispatcher.register(ContainerManagerEventType.class, containerManager);
    addService(dispatcher);

    Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            NodeManager.this.stop();
          }
        });

    DefaultMetricsSystem.initialize("NodeManager");

    super.init(conf);
    // TODO add local dirs to del
  }

  @Override
  public void start() {
    try {
      doSecureLogin();
    } catch (IOException e) {
      throw new YarnException("Failed NodeManager login", e);
    }
    super.start();
  }

  @Override
  public void stop() {
    super.stop();
    DefaultMetricsSystem.shutdown();
  }

  public static class NMContext implements Context {

    private final ConcurrentMap<ApplicationId, Application> applications =
        new ConcurrentHashMap<ApplicationId, Application>();
    private final ConcurrentMap<ContainerId, Container> containers =
      new ConcurrentSkipListMap<ContainerId,Container>(
          new Comparator<ContainerId>() {
            @Override
            public int compare(ContainerId a, ContainerId b) {
              if (a.getAppId().getId() == b.getAppId().getId()) {
                return a.getId() - b.getId();
              }
              return a.getAppId().getId() - b.getAppId().getId();
            }
            @Override
            public boolean equals(Object other) {
              return getClass().equals(other.getClass());
            }
          });

    private final NodeHealthStatus nodeHealthStatus = RecordFactoryProvider
        .getRecordFactory(null).newRecordInstance(NodeHealthStatus.class);

    public NMContext() {
      this.nodeHealthStatus.setIsNodeHealthy(true);
      this.nodeHealthStatus.setHealthReport("Healthy");
      this.nodeHealthStatus.setLastHealthReportTime(System.currentTimeMillis());
    }

    @Override
    public ConcurrentMap<ApplicationId, Application> getApplications() {
      return this.applications;
    }

    @Override
    public ConcurrentMap<ContainerId, Container> getContainers() {
      return this.containers;
    }

    @Override
    public NodeHealthStatus getNodeHealthStatus() {
      return this.nodeHealthStatus;
    }
  }

  public static void main(String[] args) {
    NodeManager nodeManager = new NodeManager();
    YarnConfiguration conf = new YarnConfiguration();
    nodeManager.init(conf);
    nodeManager.start();
  }

}

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

import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ConfigurationException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.ContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.ContainerStateTransitionListener;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.DefaultContainerExecutor;
import org.apache.hadoop.yarn.server.nodemanager.DeletionService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerEventType;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.ContainerState;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor.ContainersMonitorImpl;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.ContainerScheduler;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler.TestContainerSchedulerQueuing;
import org.apache.hadoop.yarn.server.nodemanager.executor.ContainerStartContext;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.mockito.Mockito.spy;

/**
 * Base test class that overrides the behavior of
 * {@link ContainerStateTransitionListener} for testing
 * the {@link ContainerScheduler}.
 */
public class BaseContainerSchedulerTest extends BaseContainerManagerTest {
  private static final long TWO_GB = 2048 * 1024 * 1024L;

  public BaseContainerSchedulerTest() throws UnsupportedFileSystemException {
    super();
  }

  static {
    LOG = LoggerFactory.getLogger(TestContainerSchedulerQueuing.class);
  }

  public static class Listener implements ContainerStateTransitionListener {

    private final Map<ContainerId, List<ContainerState>> states =
        new HashMap<>();
    private final Map<ContainerId, List<ContainerEventType>> events =
        new HashMap<>();

    public Map<ContainerId, List<ContainerEventType>> getEvents() {
      return events;
    }

    public Map<ContainerId, List<ContainerState>> getStates() {
      return states;
    }

    @Override
    public void init(Context context) {}

    @Override
    public void preTransition(ContainerImpl op,
        ContainerState beforeState,
        ContainerEvent eventToBeProcessed) {
      if (!states.containsKey(op.getContainerId())) {
        states.put(op.getContainerId(), new ArrayList<>());
        states.get(op.getContainerId()).add(beforeState);
        events.put(op.getContainerId(), new ArrayList<>());
      }
    }

    @Override
    public void postTransition(ContainerImpl op, ContainerState beforeState,
        ContainerState afterState, ContainerEvent processedEvent) {
      states.get(op.getContainerId()).add(afterState);
      events.get(op.getContainerId()).add(processedEvent.getType());
    }
  }

  private boolean delayContainers = true;

  protected void setDelayContainers(final boolean delayContainersParam) {
    this.delayContainers = delayContainersParam;
  }

  @Override
  protected ContainerManagerImpl createContainerManager(
      DeletionService delSrvc) {
    return new ContainerManagerImpl(context, exec, delSrvc,
        getNodeStatusUpdater(), metrics, dirsHandler) {

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
      protected ContainersMonitor createContainersMonitor(
          ContainerExecutor exec) {
        return new ContainersMonitorImpl(exec, getDispatcher(), this.context) {
          // Define resources available for containers to be executed.
          @Override
          public long getPmemAllocatedForContainers() {
            return TWO_GB;
          }

          @Override
          public long getVmemAllocatedForContainers() {
            float pmemRatio = getConfig().getFloat(
                YarnConfiguration.NM_VMEM_PMEM_RATIO,
                YarnConfiguration.DEFAULT_NM_VMEM_PMEM_RATIO);
            return (long) (pmemRatio * getPmemAllocatedForContainers());
          }

          @Override
          public long getVCoresAllocatedForContainers() {
            return 4;
          }
        };
      }
    };
  }

  @Override
  protected ContainerExecutor createContainerExecutor() {
    DefaultContainerExecutor exec = new DefaultContainerExecutor() {
      ConcurrentMap<String, Boolean> oversleepMap =
          new ConcurrentHashMap<String, Boolean>();

      /**
       * Launches the container.
       * If delayContainers is turned on, then we sleep a while before
       * starting the container.
       */
      @Override
      public int launchContainer(ContainerStartContext ctx)
          throws IOException, ConfigurationException {
        final String containerId =
            ctx.getContainer().getContainerId().toString();
        oversleepMap.put(containerId, false);
        if (delayContainers) {
          try {
            Thread.sleep(10000);
            if (oversleepMap.get(containerId)) {
              Thread.sleep(10000);
            }
          } catch (InterruptedException e) {
            // Nothing..
          }
        }
        return super.launchContainer(ctx);
      }

      @Override
      public void pauseContainer(Container container) {
        // To mimic pausing we force the container to be in the PAUSED state
        // a little longer by oversleeping.
        oversleepMap.put(container.getContainerId().toString(), true);
        LOG.info("Container was paused");
      }

      @Override
      public void resumeContainer(Container container) {
        LOG.info("Container was resumed");
      }
    };
    exec.setConf(conf);
    return spy(exec);
  }
}

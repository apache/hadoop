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

package org.apache.hadoop.yarn.service.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.service.ServiceContext;
import org.apache.hadoop.yarn.service.component.Component;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.conf.YarnServiceConf;
import org.apache.hadoop.yarn.service.component.ComponentEvent;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEvent;
import org.apache.hadoop.yarn.service.component.ComponentState;
import org.apache.hadoop.yarn.service.monitor.probe.ProbeStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.yarn.service.component.instance.ComponentInstanceState.STARTED;
import static org.apache.hadoop.yarn.service.component.ComponentEventType.FLEX;
import static org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType.BECOME_NOT_READY;
import static org.apache.hadoop.yarn.service.component.instance.ComponentInstanceEventType.BECOME_READY;
import static org.apache.hadoop.yarn.service.component.instance.ComponentInstanceState.READY;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.CONTAINER_FAILURE_WINDOW;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.DEFAULT_CONTAINER_FAILURE_WINDOW;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.DEFAULT_READINESS_CHECK_INTERVAL;
import static org.apache.hadoop.yarn.service.conf.YarnServiceConf.READINESS_CHECK_INTERVAL;

public class ServiceMonitor extends AbstractService {

  private static final Logger LOG =
      LoggerFactory.getLogger(ServiceMonitor.class);

  public ScheduledExecutorService executorService;
  private  Map<ContainerId, ComponentInstance> liveInstances = null;
  private ServiceContext context;
  private Configuration conf;

  public ServiceMonitor(String name, ServiceContext context) {
    super(name);
    liveInstances = context.scheduler.getLiveInstances();
    this.context = context;
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    executorService = Executors.newScheduledThreadPool(1);
    this.conf = conf;
    super.serviceInit(conf);
  }

  @Override
  public void serviceStart() throws Exception {
    long readinessCheckInterval = YarnServiceConf
        .getLong(READINESS_CHECK_INTERVAL, DEFAULT_READINESS_CHECK_INTERVAL,
            context.service.getConfiguration(), conf);

    executorService
        .scheduleAtFixedRate(new ReadinessChecker(), readinessCheckInterval,
            readinessCheckInterval, TimeUnit.SECONDS);

    // Default 6 hours.
    long failureResetInterval = YarnServiceConf
        .getLong(CONTAINER_FAILURE_WINDOW, DEFAULT_CONTAINER_FAILURE_WINDOW,
            context.service.getConfiguration(), conf);

    executorService
        .scheduleAtFixedRate(new ContainerFailureReset(), failureResetInterval,
            failureResetInterval, TimeUnit.SECONDS);
  }

  @Override
  public void serviceStop() throws Exception {
    if (executorService != null) {
      executorService.shutdownNow();
    }
  }

  private class ReadinessChecker implements Runnable {

    @Override
    public void run() {

      // check if the comp instance are ready
      for (Map.Entry<ContainerId, ComponentInstance> entry : liveInstances
          .entrySet()) {
        ComponentInstance instance = entry.getValue();

        ProbeStatus status = instance.ping();
        if (status.isSuccess()) {
          if (instance.getState() == STARTED) {
            LOG.info("Readiness check succeeded for {}: {}", instance
                .getCompInstanceName(), status);
            // synchronously update the state.
            instance.handle(
                new ComponentInstanceEvent(entry.getKey(), BECOME_READY));
          }
        } else {
          LOG.info("Readiness check failed for {}: {}", instance
              .getCompInstanceName(), status);
          if (instance.getState() == READY) {
            instance.handle(
                new ComponentInstanceEvent(entry.getKey(), BECOME_NOT_READY));
          }
        }
      }

      for (Component component : context.scheduler.getAllComponents()
          .values()) {
        // If comp hasn't started yet and its dependencies are satisfied
        if (component.getState() == ComponentState.INIT && component
            .areDependenciesReady()) {
          LOG.info("[COMPONENT {}]: Dependencies satisfied, ramping up.",
              component.getName());
          ComponentEvent event = new ComponentEvent(component.getName(), FLEX)
              .setDesired(component.getComponentSpec().getNumberOfContainers());
          component.handle(event);
        }
      }
    }
  }

  private class ContainerFailureReset implements Runnable {
    @Override
    public void run() {
      for (Component component : context.scheduler.getAllComponents().values()) {
        component.resetCompFailureCount();
      }
    }
  }
}

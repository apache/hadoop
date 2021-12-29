/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.maintenance;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MaintenanceModeProvider provides common functionality across all the
 * maintenance mode provider classes. When the derived class calls
 * startMaintenanceMode, a background heartbeat thread is started that fires
 * events so that all the consumers have the maintenance mode set.
 */
public abstract class MaintenanceModeProvider extends AbstractService {

  private static final Logger LOG =
      LoggerFactory.getLogger(MaintenanceModeProvider.class);

  private final NodeManager nodeManager;
  private final Dispatcher dispatcher;
  private final ScheduledExecutorService taskExecutor;
  // Future of the heartbeat task. This should be used to
  // cancel the heartbeat task.
  private ScheduledFuture<?> heartBeatTaskFuture;
  private int heartBeatIntervalSeconds;

  /**
   * Instantiates a new maintenance mode provider.
   *
   * @param serviceName the service name
   * @param nodeManager the node manager
   * @param dispatcher  the dispatcher
   */
  public MaintenanceModeProvider(String serviceName, NodeManager nodeManager,
      Dispatcher dispatcher) {
    super(serviceName);
    this.nodeManager = nodeManager;
    this.dispatcher = dispatcher;
    this.taskExecutor = Executors.newSingleThreadScheduledExecutor();
  }

  /**
   * Gets the context.
   *
   * @return the context
   */
  public Context getContext() {
    return nodeManager.getNMContext();
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    heartBeatIntervalSeconds = conf.getInt(
        MaintenanceModeConfiguration.NM_MAINTENANCE_MODE_PROVIDER_HEARTBEAT_INTERVAL_SECONDS,
        MaintenanceModeConfiguration.NM_MAINTENANCE_MODE_PROVIDER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS);
    super.serviceInit(conf);
  }

  /**
   * Start maintenance mode.
   */
  synchronized public void startMaintenanceMode() {
    // If there is heart beat already going on then just return.
    if (heartBeatTaskFuture == null) {
      LOG.info("Maintenance mode started by provider service {}", getName());
      heartBeatTaskFuture = taskExecutor.scheduleWithFixedDelay(new Runnable() {
        @Override public void run() {
          dispatchMaintenanceEvent(
              MaintenanceModeEventType.MAINTENANCE_MODE_ON_HEARTBEAT);
        }
      }, 0, heartBeatIntervalSeconds, TimeUnit.SECONDS);
    }
  }

  /**
   * Stop maintenance mode.
   */
  synchronized public void stopMaintenanceMode() {
    if (heartBeatTaskFuture != null) {
      LOG.info("Maintenance mode ended by provider service {}", getName());
      heartBeatTaskFuture.cancel(false);
      heartBeatTaskFuture = null;
    }
  }

  /**
   * Dispatch maintenance event.
   *
   * @param type the type
   */
  @SuppressWarnings("unchecked")
  protected void dispatchMaintenanceEvent(
      MaintenanceModeEventType type) {
    @SuppressWarnings("rawtypes") EventHandler eventHandler =
        dispatcher.getEventHandler();
    eventHandler.handle(new MaintenanceModeEvent(type, getName()));
  }
}
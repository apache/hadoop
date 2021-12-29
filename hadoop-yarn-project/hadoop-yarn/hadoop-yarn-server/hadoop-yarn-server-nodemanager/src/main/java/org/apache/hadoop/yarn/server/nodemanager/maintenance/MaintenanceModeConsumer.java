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
import org.apache.hadoop.util.Preconditions;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.Context;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintenance mode consumer class that has the common functionality for
 * all the maintenance mode consumers. This class maintains a background
 * task what is revived on each heartbeat from provider. If the provider
 * does not provide heart beat for a configured timeout then the
 * maintenance mode will be turned off on this consumer.
 */
public abstract class MaintenanceModeConsumer extends AbstractService implements
    EventHandler<MaintenanceModeEvent> {
  private static final Logger LOG = LoggerFactory
      .getLogger(MaintenanceModeConsumer.class);

  private final NodeManager nodeManager;
  private int heartBeatExpirySeconds;
  private boolean isMaintenanceModeOn;
  private ScheduledFuture<?> heartBeatTaskFuture;
  private final ScheduledExecutorService heartBeatExecutor;

  /**
   * Instantiates a new maintenance mode consumer.
   *
   * @param serviceName the service name
   * @param nodeManager the node manager
   */
  public MaintenanceModeConsumer(String serviceName, NodeManager nodeManager) {
    super(serviceName);
    this.isMaintenanceModeOn = false;
    this.nodeManager = nodeManager;
    this.heartBeatExecutor = Executors.newSingleThreadScheduledExecutor();
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
    heartBeatExpirySeconds =
        conf.getInt(
            MaintenanceModeConfiguration.NM_MAINTENANCE_MODE_CONSUMER_HEARTBEAT_EXPIRY_SECONDS,
            MaintenanceModeConfiguration.NM_MAINTENANCE_MODE_CONSUMER_DEFAULT_HEARTBEAT_EXPIRY_SECONDS);
    super.serviceInit(conf);
  }

  @Override
  synchronized public void handle(MaintenanceModeEvent event) {
    switch (event.getType()) {
    case MAINTENANCE_MODE_ON_HEARTBEAT: {
      if (heartBeatTaskFuture != null) {
        // remove the previously scheduled event, and schedule another.
        // (More comments above on the class level)
        heartBeatTaskFuture.cancel(false);
      }
      LOG.debug("Receiving heartbeat for maintenance mode");
      heartBeatTaskFuture = heartBeatExecutor.schedule(new Runnable() {
        @Override
        public void run() {
          maintenanceModeEndedInternal();
        }
      }, heartBeatExpirySeconds, TimeUnit.SECONDS);
      maintenanceModeStartedInternal();
    }
    break;
    default: {
      Preconditions
          .checkArgument(false, "Unknown maintenance mode event type: {}",
              event.getType().toString());
    }
    }
  }

  /**
   * Calls maintenance mode started with try catch as maintenanceModeStarted
   * should be implemented by derived class.
   */
  private void maintenanceModeStartedInternal() {
    try {
      if (!isMaintenanceModeOn) {
        LOG.info("Maintenance mode started on consumer service {}", getName());
        maintenanceModeStarted();
        isMaintenanceModeOn = true;
      }
    } catch (Throwable t) {
      LOG.error("Exception calling maintenance mode started on service {}. "
          + "MaintenanceMode is {}", getName(), isMaintenanceModeOn, t);
    }
  }

  /**
   * Calls maintenance mode ended with try catch as maintenanceModeEnded should
   * be implemented by derived class.
   */
  private void maintenanceModeEndedInternal() {
    try {
      if (isMaintenanceModeOn) {
        LOG.info("Maintenance mode ended on consumer service {}", getName());
        maintenanceModeEnded();
        isMaintenanceModeOn = false;
      }
    } catch (Throwable t) {
      LOG.error("Exception calling maintenance mode ended on service {}. "
          + "MaintenanceMode is {}", getName(), isMaintenanceModeOn, t);
    }

  }

  protected abstract void maintenanceModeStarted();

  protected abstract void maintenanceModeEnded();
}
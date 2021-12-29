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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main class that instantiates providers and consumers, and connects them using
 * an event dispatcher. Providers provide maintenance on/off feature and
 * consumers act on this. On maintenance mode on, providers periodically heart
 * beat into consumers using events and consumers extents the maintenance time
 * to a configurable amount on each heat beat. Once the heartbeat event stops
 * and the heartbeat expiry time expires on the consumer side the consumer turns
 * off maintenance mode.
 */
public class MaintenanceModeService extends AbstractService {
  private AsyncDispatcher dispatcher;
  private NodeManager nodeManager;

  private static final Logger LOG =
      LoggerFactory.getLogger(MaintenanceModeService.class);

  private List<MaintenanceModeProvider> maintenanceModeProviderList;
  private List<MaintenanceModeConsumer> maintenanceModeConsumerList;

  /**
   * Instantiates a new maintenance mode service.
   *
   * @param nodeManager the context
   */
  public MaintenanceModeService(NodeManager nodeManager) {
    super(MaintenanceModeService.class.getName());
    this.dispatcher = new AsyncDispatcher();
    this.nodeManager = nodeManager;
    maintenanceModeProviderList = new ArrayList<MaintenanceModeProvider>();
    maintenanceModeConsumerList = new ArrayList<MaintenanceModeConsumer>();
  }

  /**
   * Creates and populates provider and consumer list from the config
   *
   * @param conf the conf
   * @throws Exception the exception
   */
  void populateProviderConsumerList(Configuration conf) throws Exception {
    Class<?>[] providerClasses = conf.getClasses(
        YarnConfiguration.NM_MAINTENANCE_MODE_PROVIDER_CLASSNAMES);

    if (providerClasses == null || providerClasses.length == 0) {
      LOG.warn("Maintenance mode disabled as provider list is empty.");
      return;
    }

    // Create instances of the provider list
    for (int i = 0; i < providerClasses.length; i++) {
      Constructor<?> cons = providerClasses[i]
          .getConstructor(NodeManager.class, Dispatcher.class);
      this.maintenanceModeProviderList.add(
          (MaintenanceModeProvider) cons.newInstance(nodeManager, dispatcher));
    }

    Class<?>[] consumerClasses = conf.getClasses(
        YarnConfiguration.NM_MAINTENANCE_MODE_CONSUMER_CLASSNAMES);
    if (consumerClasses != null && consumerClasses.length > 0) {
      // Create instances of the consumer list
      for (int i = 0; i < consumerClasses.length; i++) {
        Constructor<?> cons =
            consumerClasses[i].getConstructor(NodeManager.class);
        this.maintenanceModeConsumerList
            .add((MaintenanceModeConsumer) cons.newInstance(nodeManager));
      }
    } else {
      // No consumers specified for given providers
      String temp = String.format(
          "No maintenance mode consumers present for given providers.");
      throw new YarnRuntimeException(temp);
    }
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    populateProviderConsumerList(conf);
    dispatcher.setDrainEventsOnStop();
    dispatcher.init(conf);
    for (MaintenanceModeProvider provider : maintenanceModeProviderList) {
      provider.init(conf);
    }
    for (MaintenanceModeConsumer consumer : maintenanceModeConsumerList) {
      consumer.init(conf);
    }
    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    dispatcher.start();
    for (MaintenanceModeConsumer consumer : maintenanceModeConsumerList) {
      consumer.start();
      dispatcher.register(MaintenanceModeEventType.class, consumer);
    }

    for (MaintenanceModeProvider provider : maintenanceModeProviderList) {
      provider.start();
    }

    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    for (MaintenanceModeProvider provider : maintenanceModeProviderList) {
      provider.stop();
    }
    for (MaintenanceModeConsumer consumer : maintenanceModeConsumerList) {
      consumer.stop();
    }
    super.serviceStop();
    dispatcher.stop();
  }
}

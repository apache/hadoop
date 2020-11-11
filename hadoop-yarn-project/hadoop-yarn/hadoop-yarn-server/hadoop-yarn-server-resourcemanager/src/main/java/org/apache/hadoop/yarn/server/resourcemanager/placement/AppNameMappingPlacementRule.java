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

package org.apache.hadoop.yarn.server.resourcemanager.placement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.QueuePlacementRuleUtils.getPlacementContext;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.QueuePlacementRuleUtils.isStaticQueueMapping;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.QueuePlacementRuleUtils.validateAndGetAutoCreatedQueueMapping;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.QueuePlacementRuleUtils.validateAndGetQueueMapping;

public class AppNameMappingPlacementRule extends PlacementRule {
  private static final Logger LOG = LoggerFactory
      .getLogger(AppNameMappingPlacementRule.class);

  public static final String CURRENT_APP_MAPPING = "%application";

  private static final String QUEUE_MAPPING_NAME = "app-name";

  private boolean overrideWithQueueMappings = false;
  private List<QueueMapping> mappings = null;
  protected CapacitySchedulerQueueManager queueManager;

  public AppNameMappingPlacementRule() {
    this(false, null);
  }

  public AppNameMappingPlacementRule(boolean overrideWithQueueMappings,
      List<QueueMapping> newMappings) {
    this.overrideWithQueueMappings = overrideWithQueueMappings;
    this.mappings = newMappings;
  }

  @Override
  public boolean initialize(ResourceScheduler scheduler)
      throws IOException {
    if (!(scheduler instanceof CapacityScheduler)) {
      throw new IOException(
          "AppNameMappingPlacementRule can be configured only for "
              + "CapacityScheduler");
    }
    CapacitySchedulerContext schedulerContext =
        (CapacitySchedulerContext) scheduler;
    CapacitySchedulerConfiguration conf = schedulerContext.getConfiguration();
    boolean overrideWithQueueMappings = conf.getOverrideWithQueueMappings();
    LOG.info(
        "Initialized App Name queue mappings, override: " + overrideWithQueueMappings);

    List<QueueMapping> queueMappings =
        conf.getQueueMappingEntity(QUEUE_MAPPING_NAME);

    // Get new user mappings
    List<QueueMapping> newMappings = new ArrayList<>();

    queueManager = schedulerContext.getCapacitySchedulerQueueManager();

    // check if mappings refer to valid queues
    for (QueueMapping mapping : queueMappings) {
      if (isStaticQueueMapping(mapping)) {
        //at this point mapping.getQueueName() return only the queue name, since
        //the config parsing have been changed making QueueMapping more
        //consistent

        CSQueue queue = queueManager.getQueue(mapping.getFullPath());
        if (ifQueueDoesNotExist(queue)) {
          //Try getting queue by its full path name, if it exists it is a static
          //leaf queue indeed, without any auto creation magic

          if (queueManager.isAmbiguous(mapping.getFullPath())) {
            throw new IOException(
              "mapping contains ambiguous leaf queue reference " + mapping
                .getFullPath());
          }

          //if leaf queue does not exist,
          // this could be a potential auto created leaf queue
          //validate if parent queue is specified,
          // then it should exist and
          // be an instance of AutoCreateEnabledParentQueue
          QueueMapping newMapping =
              validateAndGetAutoCreatedQueueMapping(queueManager, mapping);
          if (newMapping == null) {
            throw new IOException(
                "mapping contains invalid or non-leaf queue " + mapping
                    .getQueue());
          }
          newMappings.add(newMapping);
        } else {
          // if queue exists, validate
          //   if its an instance of leaf queue
          //   if its an instance of auto created leaf queue,
          // then extract parent queue name and update queue mapping
          QueueMapping newMapping = validateAndGetQueueMapping(
              queueManager, queue, mapping);
          newMappings.add(newMapping);
        }
      } else {
        //If it is a dynamic queue mapping,
        // we can safely assume leaf queue name does not have '.' in it
        // validate
        // if parent queue is specified, then
        //  parent queue exists and an instance of AutoCreateEnabledParentQueue
        //
        QueueMapping newMapping = validateAndGetAutoCreatedQueueMapping(
            queueManager, mapping);
        if (newMapping != null) {
          newMappings.add(newMapping);
        } else{
          newMappings.add(mapping);
        }
      }
    }

    if (newMappings.size() > 0) {
      this.mappings = newMappings;
      this.overrideWithQueueMappings = overrideWithQueueMappings;
      LOG.info("get valid queue mapping from app name config: " +
          newMappings.toString() + ", override: " + overrideWithQueueMappings);
      return true;
    }
    return false;
  }

  private static boolean ifQueueDoesNotExist(CSQueue queue) {
    return queue == null;
  }

  private ApplicationPlacementContext getAppPlacementContext(String user,
      String applicationName) throws IOException {
    for (QueueMapping mapping : mappings) {
      if (mapping.getSource().equals(CURRENT_APP_MAPPING)) {
        if (mapping.getQueue().equals(CURRENT_APP_MAPPING)) {
          return getPlacementContext(mapping, applicationName, queueManager);
        } else {
          return getPlacementContext(mapping, queueManager);
        }
      }
      if (mapping.getSource().equals(applicationName)) {
        return getPlacementContext(mapping, queueManager);
      }
    }
    return null;
  }

  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException {
    String queueName = asc.getQueue();
    String applicationName = asc.getApplicationName();
    if (mappings != null && mappings.size() > 0) {
      try {
        ApplicationPlacementContext mappedQueue = getAppPlacementContext(user,
            applicationName);
        if (mappedQueue != null) {
          // We have a mapping, should we use it?
          if (queueName.equals(YarnConfiguration.DEFAULT_QUEUE_NAME)
              //queueName will be same as mapped queue name in case of recovery
              || queueName.equals(mappedQueue.getQueue())
              || overrideWithQueueMappings) {
            LOG.info("Application {} mapping [{}] to [{}] override {}",
                applicationName, queueName, mappedQueue.getQueue(),
                overrideWithQueueMappings);
            return mappedQueue;
          }
        }
      } catch (IOException ioex) {
        String message = "Failed to submit application " + applicationName +
            " reason: " + ioex.getMessage();
        throw new YarnException(message);
      }
    }
    return null;
  }
}

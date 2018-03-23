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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.yarn.server.resourcemanager.placement.QueuePlacementRuleUtils.extractQueuePath;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.QueuePlacementRuleUtils.getPlacementContext;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.QueuePlacementRuleUtils.isStaticQueueMapping;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.QueuePlacementRuleUtils.validateAndGetAutoCreatedQueueMapping;
import static org.apache.hadoop.yarn.server.resourcemanager.placement.QueuePlacementRuleUtils.validateAndGetQueueMapping;

public class AppNameMappingPlacementRule extends PlacementRule {
  private static final Log LOG = LogFactory
      .getLog(AppNameMappingPlacementRule.class);

  public static final String CURRENT_APP_MAPPING = "%application";

  private static final String QUEUE_MAPPING_NAME = "app-name";

  private boolean overrideWithQueueMappings = false;
  private List<QueueMappingEntity> mappings = null;

  public AppNameMappingPlacementRule() {
    this(false, null);
  }

  public AppNameMappingPlacementRule(boolean overrideWithQueueMappings,
      List<QueueMappingEntity> newMappings) {
    this.overrideWithQueueMappings = overrideWithQueueMappings;
    this.mappings = newMappings;
  }

  @Override
  public boolean initialize(CapacitySchedulerContext schedulerContext)
      throws IOException {
    CapacitySchedulerConfiguration conf = schedulerContext.getConfiguration();
    boolean overrideWithQueueMappings = conf.getOverrideWithQueueMappings();
    LOG.info(
        "Initialized queue mappings, override: " + overrideWithQueueMappings);

    List<QueueMappingEntity> queueMappings =
        conf.getQueueMappingEntity(QUEUE_MAPPING_NAME);

    // Get new user mappings
    List<QueueMappingEntity> newMappings = new ArrayList<>();

    CapacitySchedulerQueueManager queueManager =
        schedulerContext.getCapacitySchedulerQueueManager();

    // check if mappings refer to valid queues
    for (QueueMappingEntity mapping : queueMappings) {

      QueuePath queuePath = extractQueuePath(mapping.getQueue());
      if (isStaticQueueMapping(mapping)) {
        //Try getting queue by its leaf queue name
        // without splitting into parent/leaf queues
        CSQueue queue = queueManager.getQueue(mapping.getQueue());
        if (ifQueueDoesNotExist(queue)) {
          //Try getting the queue by extracting leaf and parent queue names
          //Assuming its a potential auto created leaf queue
          queue = queueManager.getQueue(queuePath.getLeafQueue());

          if (ifQueueDoesNotExist(queue)) {
            //if leaf queue does not exist,
            // this could be a potential auto created leaf queue
            //validate if parent queue is specified,
            // then it should exist and
            // be an instance of AutoCreateEnabledParentQueue
            QueueMappingEntity newMapping =
                validateAndGetAutoCreatedQueueMapping(queueManager, mapping,
                    queuePath);
            if (newMapping == null) {
              throw new IOException(
                  "mapping contains invalid or non-leaf queue " + mapping
                      .getQueue());
            }
            newMappings.add(newMapping);
          } else{
            QueueMappingEntity newMapping = validateAndGetQueueMapping(
                queueManager, queue, mapping, queuePath);
            newMappings.add(newMapping);
          }
        } else{
          // if queue exists, validate
          //   if its an instance of leaf queue
          //   if its an instance of auto created leaf queue,
          // then extract parent queue name and update queue mapping
          QueueMappingEntity newMapping = validateAndGetQueueMapping(
              queueManager, queue, mapping, queuePath);
          newMappings.add(newMapping);
        }
      } else{
        //If it is a dynamic queue mapping,
        // we can safely assume leaf queue name does not have '.' in it
        // validate
        // if parent queue is specified, then
        //  parent queue exists and an instance of AutoCreateEnabledParentQueue
        //
        QueueMappingEntity newMapping = validateAndGetAutoCreatedQueueMapping(
            queueManager, mapping, queuePath);
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
      return true;
    }
    return false;
  }

  private static boolean ifQueueDoesNotExist(CSQueue queue) {
    return queue == null;
  }

  private ApplicationPlacementContext getAppPlacementContext(String user,
      ApplicationId applicationId) throws IOException {
    for (QueueMappingEntity mapping : mappings) {
      if (mapping.getSource().equals(CURRENT_APP_MAPPING)) {
        if (mapping.getQueue().equals(CURRENT_APP_MAPPING)) {
          return getPlacementContext(mapping, String.valueOf(applicationId));
        } else {
          return getPlacementContext(mapping);
        }
      }
      if (mapping.getSource().equals(applicationId.toString())) {
        return getPlacementContext(mapping);
      }
    }
    return null;
  }

  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user) throws YarnException {
    String queueName = asc.getQueue();
    ApplicationId applicationId = asc.getApplicationId();
    if (mappings != null && mappings.size() > 0) {
      try {
        ApplicationPlacementContext mappedQueue = getAppPlacementContext(user,
            applicationId);
        if (mappedQueue != null) {
          // We have a mapping, should we use it?
          if (queueName.equals(YarnConfiguration.DEFAULT_QUEUE_NAME)
              //queueName will be same as mapped queue name in case of recovery
              || queueName.equals(mappedQueue.getQueue())
              || overrideWithQueueMappings) {
            LOG.info("Application " + applicationId
                + " mapping [" + queueName + "] to [" + mappedQueue
                + "] override " + overrideWithQueueMappings);
            return mappedQueue;
          }
        }
      } catch (IOException ioex) {
        String message = "Failed to submit application " + applicationId +
            " reason: " + ioex.getMessage();
        throw new YarnException(message);
      }
    }
    return null;
  }
}

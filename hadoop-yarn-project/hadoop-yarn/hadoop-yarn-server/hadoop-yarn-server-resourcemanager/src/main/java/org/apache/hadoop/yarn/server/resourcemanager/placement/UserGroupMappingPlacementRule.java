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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.UserGroupMappingPlacementRule.QueueMapping.MappingType;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.DOT;

public class UserGroupMappingPlacementRule extends PlacementRule {
  private static final Log LOG = LogFactory
      .getLog(UserGroupMappingPlacementRule.class);

  public static final String CURRENT_USER_MAPPING = "%user";

  public static final String PRIMARY_GROUP_MAPPING = "%primary_group";

  private boolean overrideWithQueueMappings = false;
  private List<QueueMapping> mappings = null;
  private Groups groups;

  @Private
  public static class QueueMapping {

    public enum MappingType {

      USER("u"), GROUP("g");
      private final String type;

      private MappingType(String type) {
        this.type = type;
      }

      public String toString() {
        return type;
      }

    };

    MappingType type;
    String source;
    String queue;
    String parentQueue;

    public final static String DELIMITER = ":";

    public QueueMapping(MappingType type, String source, String queue) {
      this.type = type;
      this.source = source;
      this.queue = queue;
      this.parentQueue = null;
    }

    public QueueMapping(MappingType type, String source,
        String queue, String parentQueue) {
      this.type = type;
      this.source = source;
      this.queue = queue;
      this.parentQueue = parentQueue;
    }

    public String getQueue() {
      return queue;
    }

    public String getParentQueue() {
      return parentQueue;
    }

    public boolean hasParentQueue() {
      return parentQueue != null;
    }

    public MappingType getType() {
      return type;
    }

    public String getSource() {
      return source;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
      if (obj instanceof QueueMapping) {
        QueueMapping other = (QueueMapping) obj;
        return (other.type.equals(type) && 
            other.source.equals(source) && 
            other.queue.equals(queue));
      } else {
        return false;
      }
    }

    public String toString() {
      return type.toString() + DELIMITER + source + DELIMITER +
        (parentQueue != null ?
        parentQueue + "." + queue :
        queue);
    }
  }

  public UserGroupMappingPlacementRule(boolean overrideWithQueueMappings,
      List<QueueMapping> newMappings, Groups groups) {
    this.mappings = newMappings;
    this.overrideWithQueueMappings = overrideWithQueueMappings;
    this.groups = groups;
  }

  private ApplicationPlacementContext getPlacementForUser(String user)
      throws IOException {
    for (QueueMapping mapping : mappings) {
      if (mapping.type == MappingType.USER) {
        if (mapping.source.equals(CURRENT_USER_MAPPING)) {
          if (mapping.queue.equals(CURRENT_USER_MAPPING)) {
            return getPlacementContext(mapping, user);
          } else if (mapping.queue.equals(PRIMARY_GROUP_MAPPING)) {
            return getPlacementContext(mapping, groups.getGroups(user).get(0));
          } else {
            return getPlacementContext(mapping);
          }
        }
        if (user.equals(mapping.source)) {
          return getPlacementContext(mapping);
        }
      }
      if (mapping.type == MappingType.GROUP) {
        for (String userGroups : groups.getGroups(user)) {
          if (userGroups.equals(mapping.source)) {
            if (mapping.queue.equals(CURRENT_USER_MAPPING)) {
              return getPlacementContext(mapping, user);
            }
            return getPlacementContext(mapping);
          }
        }
      }
    }
    return null;
  }

  @Override
  public ApplicationPlacementContext getPlacementForApp(
      ApplicationSubmissionContext asc, String user)
      throws YarnException {
    String queueName = asc.getQueue();
    ApplicationId applicationId = asc.getApplicationId();
    if (mappings != null && mappings.size() > 0) {
      try {
        ApplicationPlacementContext mappedQueue = getPlacementForUser(user);
        if (mappedQueue != null) {
          // We have a mapping, should we use it?
          if (queueName.equals(YarnConfiguration.DEFAULT_QUEUE_NAME)
              //queueName will be same as mapped queue name in case of recovery
              || queueName.equals(mappedQueue.getQueue())
              || overrideWithQueueMappings) {
            LOG.info("Application " + applicationId + " user " + user
                + " mapping [" + queueName + "] to [" + mappedQueue
                + "] override " + overrideWithQueueMappings);
            return mappedQueue;
          }
        }
      } catch (IOException ioex) {
        String message = "Failed to submit application " + applicationId +
            " submitted by user " + user + " reason: " + ioex.getMessage();
        throw new YarnException(message);
      }
    }
    return null;
  }

  private ApplicationPlacementContext getPlacementContext(
      QueueMapping mapping) {
    return getPlacementContext(mapping, mapping.getQueue());
  }

  private ApplicationPlacementContext getPlacementContext(QueueMapping mapping,
      String leafQueueName) {
    if (!StringUtils.isEmpty(mapping.parentQueue)) {
      return new ApplicationPlacementContext(leafQueueName,
          mapping.getParentQueue());
    } else{
      return new ApplicationPlacementContext(leafQueueName);
    }
  }

  @VisibleForTesting
  public static UserGroupMappingPlacementRule get(
      CapacitySchedulerContext schedulerContext) throws IOException {
    CapacitySchedulerConfiguration conf = schedulerContext.getConfiguration();
    boolean overrideWithQueueMappings = conf.getOverrideWithQueueMappings();
    LOG.info(
        "Initialized queue mappings, override: " + overrideWithQueueMappings);

    List<QueueMapping> queueMappings = conf.getQueueMappings();

    // Get new user/group mappings
    List<QueueMapping> newMappings = new ArrayList<>();

    CapacitySchedulerQueueManager queueManager =
        schedulerContext.getCapacitySchedulerQueueManager();

    // check if mappings refer to valid queues
    for (QueueMapping mapping : queueMappings) {

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
            QueueMapping newMapping = validateAndGetAutoCreatedQueueMapping(
                queueManager, mapping, queuePath);
            if (newMapping == null) {
              throw new IOException(
                  "mapping contains invalid or non-leaf queue " + mapping
                      .getQueue());
            }
            newMappings.add(newMapping);
          } else{
            QueueMapping newMapping = validateAndGetQueueMapping(queueManager,
                queue, mapping, queuePath);
            newMappings.add(newMapping);
          }
        } else{
          // if queue exists, validate
          //   if its an instance of leaf queue
          //   if its an instance of auto created leaf queue,
          // then extract parent queue name and update queue mapping
          QueueMapping newMapping = validateAndGetQueueMapping(queueManager,
              queue, mapping, queuePath);
          newMappings.add(newMapping);
        }
      } else{
        //If it is a dynamic queue mapping,
        // we can safely assume leaf queue name does not have '.' in it
        // validate
        // if parent queue is specified, then
        //  parent queue exists and an instance of AutoCreateEnabledParentQueue
        //
        QueueMapping newMapping = validateAndGetAutoCreatedQueueMapping(
            queueManager, mapping, queuePath);
        if (newMapping != null) {
          newMappings.add(newMapping);
        } else{
          newMappings.add(mapping);
        }
      }
    }

    // initialize groups if mappings are present
    if (newMappings.size() > 0) {
      Groups groups = new Groups(conf);
      return new UserGroupMappingPlacementRule(overrideWithQueueMappings,
          newMappings, groups);
    }

    return null;
  }

  private static QueueMapping validateAndGetQueueMapping(
      CapacitySchedulerQueueManager queueManager, CSQueue queue,
      QueueMapping mapping, QueuePath queuePath) throws IOException {
    if (!(queue instanceof LeafQueue)) {
      throw new IOException(
          "mapping contains invalid or non-leaf queue : " + mapping.getQueue());
    }

    if (queue instanceof AutoCreatedLeafQueue && queue
        .getParent() instanceof ManagedParentQueue) {

      QueueMapping newMapping = validateAndGetAutoCreatedQueueMapping(
          queueManager, mapping, queuePath);
      if (newMapping == null) {
        throw new IOException(
            "mapping contains invalid or non-leaf queue " + mapping.getQueue());
      }
      return newMapping;
    }
    return mapping;
  }

  private static boolean ifQueueDoesNotExist(CSQueue queue) {
    return queue == null;
  }

  private static QueueMapping validateAndGetAutoCreatedQueueMapping(
      CapacitySchedulerQueueManager queueManager, QueueMapping mapping,
      QueuePath queuePath) throws IOException {
    if (queuePath.hasParentQueue()) {
      //if parent queue is specified,
      // then it should exist and be an instance of ManagedParentQueue
      validateParentQueue(queueManager.getQueue(queuePath.getParentQueue()),
          queuePath.getParentQueue(), queuePath.getLeafQueue());
      return new QueueMapping(mapping.getType(), mapping.getSource(),
          queuePath.getLeafQueue(), queuePath.getParentQueue());
    }

    return null;
  }

  private static boolean isStaticQueueMapping(QueueMapping mapping) {
    return !mapping.getQueue().contains(
        UserGroupMappingPlacementRule.CURRENT_USER_MAPPING) && !mapping
        .getQueue().contains(
            UserGroupMappingPlacementRule.PRIMARY_GROUP_MAPPING);
  }

  private static class QueuePath {

    public String parentQueue;
    public String leafQueue;

    public QueuePath(final String leafQueue) {
      this.leafQueue = leafQueue;
    }

    public QueuePath(final String parentQueue, final String leafQueue) {
      this.parentQueue = parentQueue;
      this.leafQueue = leafQueue;
    }

    public String getParentQueue() {
      return parentQueue;
    }

    public String getLeafQueue() {
      return leafQueue;
    }

    public boolean hasParentQueue() {
      return parentQueue != null;
    }

    @Override
    public String toString() {
      return parentQueue + DOT + leafQueue;
    }
  }

  private static QueuePath extractQueuePath(String queueName)
      throws IOException {
    int parentQueueNameEndIndex = queueName.lastIndexOf(DOT);

    if (parentQueueNameEndIndex > -1) {
      final String parentQueue = queueName.substring(0, parentQueueNameEndIndex)
          .trim();
      final String leafQueue = queueName.substring(parentQueueNameEndIndex + 1)
          .trim();
      return new QueuePath(parentQueue, leafQueue);
    }

    return new QueuePath(queueName);
  }

  private static void validateParentQueue(CSQueue parentQueue,
      String parentQueueName, String leafQueueName) throws IOException {
    if (parentQueue == null) {
      throw new IOException(
          "mapping contains invalid or non-leaf queue [" + leafQueueName
              + "] and invalid parent queue [" + parentQueueName + "]");
    } else if (!(parentQueue instanceof ManagedParentQueue)) {
      throw new IOException("mapping contains leaf queue [" + leafQueueName
          + "] and invalid parent queue which "
          + "does not have auto creation of leaf queues enabled ["
          + parentQueueName + "]");
    } else if (!parentQueue.getQueueName().equals(parentQueueName)) {
      throw new IOException(
          "mapping contains invalid or non-leaf queue [" + leafQueueName
              + "] and invalid parent queue "
              + "which does not match existing leaf queue's parent : ["
              + parentQueueName + "] does not match [ " + parentQueue
              .getQueueName() + "]");
    }
  }

  @VisibleForTesting
  public List<QueueMapping> getQueueMappings() {
    return mappings;
  }
}

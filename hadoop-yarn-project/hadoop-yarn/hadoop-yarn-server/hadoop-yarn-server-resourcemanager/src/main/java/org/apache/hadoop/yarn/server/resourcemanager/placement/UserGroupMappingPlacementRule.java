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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping.MappingType;
import org.apache.hadoop.yarn.server.resourcemanager.placement.QueueMapping.QueueMappingBuilder;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.AutoCreatedLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerQueueManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.LeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

public class UserGroupMappingPlacementRule extends PlacementRule {
  private static final Logger LOG = LoggerFactory
      .getLogger(UserGroupMappingPlacementRule.class);

  public static final String CURRENT_USER_MAPPING = "%user";

  public static final String PRIMARY_GROUP_MAPPING = "%primary_group";

  public static final String SECONDARY_GROUP_MAPPING = "%secondary_group";

  private boolean overrideWithQueueMappings = false;
  private List<QueueMapping> mappings = null;
  private Groups groups;
  private CapacitySchedulerQueueManager queueManager;

  public UserGroupMappingPlacementRule(){
    this(false, null, null);
  }

  @VisibleForTesting
  UserGroupMappingPlacementRule(boolean overrideWithQueueMappings,
      List<QueueMapping> newMappings, Groups groups) {
    this.mappings = newMappings;
    this.overrideWithQueueMappings = overrideWithQueueMappings;
    this.groups = groups;
  }

  private String getPrimaryGroup(String user) throws IOException {
    return groups.getGroups(user).get(0);
  }

  private String getSecondaryGroup(String user) throws IOException {
    List<String> groupsList = groups.getGroups(user);
    String secondaryGroup = null;
    // Traverse all secondary groups (as there could be more than one
    // and position is not guaranteed) and ensure there is queue with
    // the same name
    for (int i = 1; i < groupsList.size(); i++) {
      if (this.queueManager.getQueue(groupsList.get(i)) != null) {
        secondaryGroup = groupsList.get(i);
        break;
      }
    }

    if (secondaryGroup == null && LOG.isDebugEnabled()) {
      LOG.debug("User {} is not associated with any Secondary "
          + "Group. Hence it may use the 'default' queue", user);
    }
    return secondaryGroup;
  }

  private ApplicationPlacementContext getPlacementForUser(String user)
      throws IOException {
    for (QueueMapping mapping : mappings) {
      if (mapping.getType().equals(MappingType.USER)) {
        if (mapping.getSource().equals(CURRENT_USER_MAPPING)) {
          if (mapping.getParentQueue() != null
              && mapping.getParentQueue().equals(PRIMARY_GROUP_MAPPING)
              && mapping.getQueue().equals(CURRENT_USER_MAPPING)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Creating placement context for user {} using " +
                  "primary group current user mapping", user);
            }
            return getContextForGroupParent(user, mapping,
                getPrimaryGroup(user));
          } else if (mapping.getParentQueue() != null
              && mapping.getParentQueue().equals(SECONDARY_GROUP_MAPPING)
              && mapping.getQueue().equals(CURRENT_USER_MAPPING)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Creating placement context for user {} using " +
                  "secondary group current user mapping", user);
            }
            return getContextForGroupParent(user, mapping,
                getSecondaryGroup(user));
          } else if (mapping.getQueue().equals(CURRENT_USER_MAPPING)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Creating placement context for user {} using " +
                  "current user mapping", user);
            }
            return getPlacementContext(mapping, user);
          } else if (mapping.getQueue().equals(PRIMARY_GROUP_MAPPING)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Creating placement context for user {} using " +
                  "primary group mapping", user);
            }
            return getPlacementContext(mapping, getPrimaryGroup(user));
          } else if (mapping.getQueue().equals(SECONDARY_GROUP_MAPPING)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Creating placement context for user {} using " +
                  "secondary group mapping", user);
            }
            return getPlacementContext(mapping, getSecondaryGroup(user));
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Creating placement context for user {} using " +
                  "static user static mapping", user);
            }
            return getPlacementContext(mapping);
          }
        }

        if (user.equals(mapping.getSource())) {
          if (mapping.getQueue().equals(PRIMARY_GROUP_MAPPING)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Creating placement context for user {} using " +
                  "static user primary group mapping", user);
            }
            return getPlacementContext(mapping, getPrimaryGroup(user));
          } else if (mapping.getQueue().equals(SECONDARY_GROUP_MAPPING)) {
            String secondaryGroup = getSecondaryGroup(user);
            if (secondaryGroup != null) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Creating placement context for user {} using " +
                    "static user secondary group mapping", user);
              }
              return getPlacementContext(mapping, secondaryGroup);
            } else {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Wanted to create placement context for user {}" +
                    " using static user secondary group mapping," +
                    " but user has no secondary group!", user);
              }
              return null;
            }
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Creating placement context for user {} using " +
                  "static user static mapping", user);
            }
            return getPlacementContext(mapping);
          }
        }
      }
      if (mapping.getType().equals(MappingType.GROUP)) {
        for (String userGroups : groups.getGroups(user)) {
          if (userGroups.equals(mapping.getSource())) {
            if (mapping.getQueue().equals(CURRENT_USER_MAPPING)) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Creating placement context for user {} using " +
                    "static group current user mapping", user);
              }
              return getPlacementContext(mapping, user);
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug("Creating placement context for user {} using " +
                  "static group static mapping", user);
            }
            return getPlacementContext(mapping);
          }
        }
      }
    }
    return null;
  }

  /**
   * This convenience method allows to change the parent path or a leafName in
   * a mapping object, by creating a new one, using the builder and copying the
   * rest of the parameters.
   * @param mapping The mapping to be changed
   * @param parentPath The new parentPath of the mapping
   * @param leafName The new leafQueueName of the mapping
   * @return The updated NEW mapping
   */
  private QueueMapping alterMapping(
      QueueMapping mapping, String parentPath, String leafName) {
    return QueueMappingBuilder.create()
            .type(mapping.getType())
            .source(mapping.getSource())
            .queue(leafName)
            .parentQueue(parentPath)
            .build();
  }

  // invoked for mappings:
  //    u:%user:%primary_group.%user
  //    u:%user:%secondary_group.%user
  private ApplicationPlacementContext getContextForGroupParent(
      String user,
      QueueMapping mapping,
      String group) throws IOException {

    CSQueue groupQueue = this.queueManager.getQueue(group);
    if (groupQueue != null) {
      // replace the group string
      QueueMapping resolvedGroupMapping = alterMapping(
          mapping,
          groupQueue.getQueuePath(),
          user);
      validateQueueMapping(resolvedGroupMapping);
      return getPlacementContext(resolvedGroupMapping, user);
    } else {

      if (queueManager.isAmbiguous(group)) {
        LOG.info("Queue mapping rule expect group queue to exist with name {}" +
            " but the reference is ambiguous!", group);
      } else {
        LOG.info("Queue mapping rule expect group queue to exist with name {}" +
            " but it does not exist!", group);
      }
      return null;
    }
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
            LOG.info("Application {} user {} mapping [{}] to [{}] override {}",
                applicationId, user, queueName, mappedQueue.getQueue(),
                overrideWithQueueMappings);
            return mappedQueue;
          }
        }
      } catch (IOException ioex) {
        String message = "Failed to submit application " + applicationId +
            " submitted by user " + user + " reason: " + ioex.getMessage();
        throw new YarnException(message, ioex);
      }
    }
    return null;
  }

  private ApplicationPlacementContext getPlacementContext(
      QueueMapping mapping) throws IOException {
    return getPlacementContext(mapping, mapping.getQueue());
  }

  private ApplicationPlacementContext getPlacementContext(QueueMapping mapping,
      String leafQueueName) throws IOException {
    //leafQueue name no longer identifies a queue uniquely checking ambiguity
    if (!mapping.hasParentQueue() && queueManager.isAmbiguous(leafQueueName)) {
      throw new IOException("mapping contains ambiguous leaf queue reference " +
          leafQueueName);
    }

    if (!StringUtils.isEmpty(mapping.getParentQueue())) {
      return getPlacementContextWithParent(mapping, leafQueueName);
    } else {
      return getPlacementContextNoParent(leafQueueName);
    }
  }

  private ApplicationPlacementContext getPlacementContextWithParent(
      QueueMapping mapping,
      String leafQueueName) {
    CSQueue parent = queueManager.getQueue(mapping.getParentQueue());
    //we don't find the specified parent, so the placement rule is invalid
    //for this case
    if (parent == null) {
      if (queueManager.isAmbiguous(mapping.getParentQueue())) {
        LOG.warn("Placement rule specified a parent queue {}, but it is" +
            "ambiguous.", mapping.getParentQueue());
      } else {
        LOG.warn("Placement rule specified a parent queue {}, but it does" +
            "not exist.", mapping.getParentQueue());
      }
      return null;
    }

    String parentPath = parent.getQueuePath();

    //if we have a parent which is not a managed parent, we check if the leaf
    //queue exists under this parent
    if (!(parent instanceof ManagedParentQueue)) {
      CSQueue queue = queueManager.getQueue(
          parentPath + "." + leafQueueName);
      //if the queue doesn't exit we return null
      if (queue == null) {
          LOG.warn("Placement rule specified a parent queue {}, but it is" +
              " not a managed parent queue, and no queue exists with name {} " +
              "under it.", mapping.getParentQueue(), leafQueueName);
        return null;
      }
    }
    //at this point we either have a managed parent or the queue actually
    //exists so we have a placement context, returning it
    return new ApplicationPlacementContext(leafQueueName, parentPath);
  }

  private ApplicationPlacementContext getPlacementContextNoParent(
      String leafQueueName) {
    //in this case we don't have a parent specified so we expect the queue to
    //exist, otherwise the mapping will not be valid for this case
    CSQueue queue = queueManager.getQueue(leafQueueName);
    if (queue == null) {
      if (queueManager.isAmbiguous(leafQueueName)) {
        LOG.warn("Queue {} specified in placement rule is ambiguous",
            leafQueueName);
      } else {
        LOG.warn("Queue {} specified in placement rule does not exist",
            leafQueueName);
      }
      return null;
    }

    //getting parent path to make sure if the leaf name would become ambiguous
    //the placement context stays valid.
    CSQueue parent = queueManager.getQueue(leafQueueName).getParent();
    return new ApplicationPlacementContext(
        leafQueueName, parent.getQueuePath());
  }

  @VisibleForTesting
  @Override
  public boolean initialize(ResourceScheduler scheduler)
      throws IOException {
    if (!(scheduler instanceof CapacityScheduler)) {
      throw new IOException(
          "UserGroupMappingPlacementRule can be configured only for "
              + "CapacityScheduler");
    }
    CapacitySchedulerContext schedulerContext =
        (CapacitySchedulerContext) scheduler;
    CapacitySchedulerConfiguration conf = schedulerContext.getConfiguration();
    boolean overrideWithQueueMappings = conf.getOverrideWithQueueMappings();
    LOG.info(
        "Initialized queue mappings, override: " + overrideWithQueueMappings);

    List<QueueMapping> queueMappings = conf.getQueueMappings();

    // Get new user/group mappings
    List<QueueMapping> newMappings = new ArrayList<>();

    queueManager = schedulerContext.getCapacitySchedulerQueueManager();

    // check if mappings refer to valid queues
    for (QueueMapping mapping : queueMappings) {
      //at this point mapping.getQueueName() return only the queue name, since
      //the config parsing have been changed making QueueMapping more consistent

      if (isStaticQueueMapping(mapping)) {
        //Try getting queue by its full path name, if it exists it is a static
        //leaf queue indeed, without any auto creation magic
        CSQueue queue = queueManager.getQueue(mapping.getFullPath());
        if (ifQueueDoesNotExist(queue)) {
          //We might not be able to find the queue, because the reference was
          // ambiguous this should only happen if the queue was referenced by
          // leaf name only
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
          QueueMapping newMapping = validateAndGetAutoCreatedQueueMapping(
              queueManager, mapping);
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
          QueueMapping newMapping = validateAndGetQueueMapping(queueManager,
              queue, mapping);
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
            queueManager, mapping);
        if (newMapping != null) {
          newMappings.add(newMapping);
        } else{
          newMappings.add(mapping);
        }
      }
    }

    // initialize groups if mappings are present
    if (newMappings.size() > 0) {
      this.mappings = newMappings;
      this.groups = Groups.getUserToGroupsMappingService(
          ((CapacityScheduler)scheduler).getConf());
      this.overrideWithQueueMappings = overrideWithQueueMappings;
      return true;
    }
    return false;
  }

  private static QueueMapping validateAndGetQueueMapping(
      CapacitySchedulerQueueManager queueManager, CSQueue queue,
      QueueMapping mapping) throws IOException {
    if (!(queue instanceof LeafQueue)) {
      throw new IOException(
          "mapping contains invalid or non-leaf queue : " +
          mapping.getFullPath());
    }

    if (queue instanceof AutoCreatedLeafQueue && queue
        .getParent() instanceof ManagedParentQueue) {

      QueueMapping newMapping = validateAndGetAutoCreatedQueueMapping(
          queueManager, mapping);
      if (newMapping == null) {
        throw new IOException(
            "mapping contains invalid or non-leaf queue "
            + mapping.getFullPath());
      }
      return newMapping;
    }
    return mapping;
  }

  private static boolean ifQueueDoesNotExist(CSQueue queue) {
    return queue == null;
  }

  private static QueueMapping validateAndGetAutoCreatedQueueMapping(
      CapacitySchedulerQueueManager queueManager, QueueMapping mapping)
      throws IOException {
    if (mapping.hasParentQueue()
        && (mapping.getParentQueue().equals(PRIMARY_GROUP_MAPPING)
            || mapping.getParentQueue().equals(SECONDARY_GROUP_MAPPING))) {
      // dynamic parent queue
      return mapping;
    } else if (mapping.hasParentQueue()) {
      //if parent queue is specified,
      // then it should exist and be an instance of ManagedParentQueue
      QueuePlacementRuleUtils.validateQueueMappingUnderParentQueue(
              queueManager.getQueue(mapping.getParentQueue()),
          mapping.getParentQueue(), mapping.getQueue());
      return mapping;
    }

    return null;
  }

  private static boolean isStaticQueueMapping(QueueMapping mapping) {
    return !mapping.getQueue()
        .contains(UserGroupMappingPlacementRule.CURRENT_USER_MAPPING)
        && !mapping.getQueue()
            .contains(UserGroupMappingPlacementRule.PRIMARY_GROUP_MAPPING)
        && !mapping.getQueue()
            .contains(UserGroupMappingPlacementRule.SECONDARY_GROUP_MAPPING);
  }

  private void validateQueueMapping(QueueMapping queueMapping)
      throws IOException {
    String parentQueueName = queueMapping.getParentQueue();
    String leafQueueFullName = queueMapping.getFullPath();
    CSQueue parentQueue = queueManager.getQueueByFullName(parentQueueName);
    CSQueue leafQueue = queueManager.getQueue(leafQueueFullName);

    if (leafQueue == null || (!(leafQueue instanceof LeafQueue))) {
      //this might be confusing, but a mapping is not guaranteed to provide the
      //parent queue's name, which can result in ambiguous queue references
      //if no parent queueName is provided mapping.getFullPath() is the same
      //as mapping.getQueue()
      if (leafQueue == null && queueManager.isAmbiguous(leafQueueFullName)) {
        throw new IOException("mapping contains ambiguous leaf queue name: "
            + leafQueueFullName);
      } else if (parentQueue == null ||
          (!(parentQueue instanceof ManagedParentQueue))) {
        throw new IOException("mapping contains invalid or non-leaf queue " +
            " and no managed parent is found: "
            + leafQueueFullName);
      }
    } else if (parentQueue == null || (!(parentQueue instanceof ParentQueue))) {
      throw new IOException(
          "mapping contains invalid parent queue [" + parentQueueName + "]");
    } else if (!parentQueue.getQueuePath()
        .equals(leafQueue.getParent().getQueuePath())) {
      throw new IOException("mapping contains invalid parent queue "
          + "which does not match existing leaf queue's parent : ["
          + parentQueue.getQueuePath() + "] does not match [ "
          + leafQueue.getParent().getQueuePath() + "]");
    }
  }

  @VisibleForTesting
  public List<QueueMapping> getQueueMappings() {
    return mappings;
  }

  @VisibleForTesting
  @Private
  public void setQueueManager(CapacitySchedulerQueueManager queueManager) {
    this.queueManager = queueManager;
  }
}

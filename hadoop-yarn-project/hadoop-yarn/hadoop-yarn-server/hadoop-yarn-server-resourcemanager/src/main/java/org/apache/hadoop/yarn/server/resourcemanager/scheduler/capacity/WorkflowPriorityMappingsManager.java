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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.VisibleForTesting;

@Private
@VisibleForTesting
public class WorkflowPriorityMappingsManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(WorkflowPriorityMappingsManager.class);

  private static final String WORKFLOW_PART_SEPARATOR = ":";

  private static final String WORKFLOW_SEPARATOR = ",";

  private CapacityScheduler scheduler;

  private CapacitySchedulerConfiguration conf;

  private boolean overrideWithPriorityMappings = false;
  // Map of queue to a map of workflow ID to priority
  private Map<String, Map<String, Priority>> priorityMappings =
      new HashMap<>();

  public static class WorkflowPriorityMapping {
    String workflowID;
    String queue;
    Priority priority;

    public WorkflowPriorityMapping(String workflowID, String queue,
        Priority priority) {
      this.workflowID = workflowID;
      this.queue = queue;
      this.priority = priority;
    }

    public Priority getPriority() {
      return this.priority;
    }

    @Override
    public int hashCode() {
      return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof WorkflowPriorityMapping) {
        WorkflowPriorityMapping other = (WorkflowPriorityMapping) obj;
        return (other.workflowID.equals(workflowID) &&
            other.queue.equals(queue) &&
            other.priority.equals(priority));
      } else {
        return false;
      }
    }

    public String toString() {
      return workflowID + WORKFLOW_PART_SEPARATOR + queue
          + WORKFLOW_PART_SEPARATOR + priority.getPriority();
    }
  }

  @VisibleForTesting
  public void initialize(CapacityScheduler scheduler) throws IOException {
    this.scheduler = scheduler;
    this.conf = scheduler.getConfiguration();
    boolean overrideWithWorkflowPriorityMappings =
        conf.getOverrideWithWorkflowPriorityMappings();
    LOG.info("Initialized workflow priority mappings, override: "
        + overrideWithWorkflowPriorityMappings);
    this.overrideWithPriorityMappings = overrideWithWorkflowPriorityMappings;
    this.priorityMappings = getWorkflowPriorityMappings();
  }

  /**
   * Get workflow ID to priority mappings for a queue.
   *
   * @return workflowID to priority mappings for a queue
   */
  public Map<String, Map<String, Priority>>
      getWorkflowPriorityMappings() {
    Map<String, Map<String, Priority>> mappings = new HashMap<>();

    Collection<String> workflowMappings = conf.getWorkflowPriorityMappings();
    for (String workflowMapping : workflowMappings) {
      WorkflowPriorityMapping mapping =
          getWorkflowMappingFromString(workflowMapping);
      if (mapping != null) {
        if (!mappings.containsKey(mapping.queue)) {
          mappings.put(mapping.queue,
              new HashMap<String, Priority>());
        }
        mappings.get(mapping.queue).put(mapping.workflowID, mapping.priority);
      }
    }
    return mappings;
  }

  private WorkflowPriorityMapping getWorkflowMappingFromString(
      String mappingString) {
    if (mappingString == null) {
      return null;
    }
    String[] mappingArray = StringUtils
        .getTrimmedStringCollection(mappingString, WORKFLOW_PART_SEPARATOR)
            .toArray(new String[] {});
    if (mappingArray.length != 3 || mappingArray[0].length() == 0
        || mappingArray[1].length() == 0 || mappingArray[2].length() == 0) {
      throw new IllegalArgumentException(
          "Illegal workflow priority mapping " + mappingString);
    }
    WorkflowPriorityMapping mapping;
    try {
      //Converting workflow id to lowercase as yarn converts application tags also to lowercase
      mapping = new WorkflowPriorityMapping(StringUtils.toLowerCase(mappingArray[0]),
          mappingArray[1], Priority.newInstance(Integer.parseInt(mappingArray[2])));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Illegal workflow priority for mapping " + mappingString);
    }
    return mapping;
  }

  public Priority getMappedPriority(String workflowID, CSQueue queue) {
    // Recursively fetch the priority mapping for the given workflow tracing
    // up the queue hierarchy until the first match.
    if (queue.equals(scheduler.getRootQueue())) {
      return null;
    }
    String queuePath = queue.getQueuePath();
    if (priorityMappings.containsKey(queuePath)
        && priorityMappings.get(queuePath).containsKey(workflowID)) {
      return priorityMappings.get(queuePath).get(workflowID);
    } else {
      queue = queue.getParent();
      return getMappedPriority(workflowID, queue);
    }
  }

  public Priority mapWorkflowPriorityForApp(ApplicationId applicationId,
      CSQueue queue, String user, Priority priority) throws YarnException {
    if (overrideWithPriorityMappings) {
      // Set the correct workflow priority
      RMApp rmApp = scheduler.getRMContext().getRMApps().get(applicationId);
      if (rmApp != null && rmApp.getApplicationTags() != null
          && rmApp.getApplicationSubmissionContext() != null) {
        String workflowTagPrefix = scheduler.getConf().get(
            YarnConfiguration.YARN_WORKFLOW_ID_TAG_PREFIX,
            YarnConfiguration.DEFAULT_YARN_WORKFLOW_ID_TAG_PREFIX);
        String workflowID = null;
        for(String tag : rmApp.getApplicationTags()) {
          if (tag.trim().startsWith(workflowTagPrefix)) {
            workflowID = tag.trim().substring(workflowTagPrefix.length());
          }
        }
        if (workflowID != null && !workflowID.isEmpty()
            && priorityMappings != null && priorityMappings.size() > 0) {
          Priority mappedPriority = getMappedPriority(workflowID, queue);
          if (mappedPriority != null) {
            LOG.info("Application " + applicationId + " user " + user
                + " workflow " + workflowID + " queue " + queue.getQueuePath()
                + " mapping [" + priority + "] to [" + mappedPriority
                + "] override " + overrideWithPriorityMappings);

            // If workflow ID exists in workflow mapping, change this
            // application's priority to mapped value. Else, use queue
            // default priority.
            priority = mappedPriority;
            priority = scheduler.checkAndGetApplicationPriority(
                priority, UserGroupInformation.createRemoteUser(user),
                queue.getQueuePath(), applicationId);
            rmApp.getApplicationSubmissionContext().setPriority(priority);
            ((RMAppImpl)rmApp).setApplicationPriority(priority);
          }
        }
      }
    }
    return priority;
  }

  public static String getWorkflowPriorityMappingStr(
      List<WorkflowPriorityMapping> workflowPriorityMappings) {
    if (workflowPriorityMappings == null) {
      return "";
    }
    List<String> workflowPriorityMappingStrs = new ArrayList<>();
    for (WorkflowPriorityMapping mapping : workflowPriorityMappings) {
      workflowPriorityMappingStrs.add(mapping.toString());
    }
    return StringUtils.join(WORKFLOW_SEPARATOR, workflowPriorityMappingStrs);
  }
}

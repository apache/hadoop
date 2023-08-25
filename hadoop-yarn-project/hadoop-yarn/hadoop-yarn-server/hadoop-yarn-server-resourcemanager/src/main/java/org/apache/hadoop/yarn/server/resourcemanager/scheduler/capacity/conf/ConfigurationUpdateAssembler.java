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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ORDERING_POLICY;

public final class ConfigurationUpdateAssembler {

  private ConfigurationUpdateAssembler() {
  }

  public static Map<String, String> constructKeyValueConfUpdate(
          CapacitySchedulerConfiguration proposedConf,
          SchedConfUpdateInfo mutationInfo) throws IOException {

    Map<String, String> confUpdate = new HashMap<>();
    for (String queueToRemove : mutationInfo.getRemoveQueueInfo()) {
      removeQueue(queueToRemove, proposedConf, confUpdate);
    }
    for (QueueConfigInfo addQueueInfo : mutationInfo.getAddQueueInfo()) {
      addQueue(addQueueInfo, proposedConf, confUpdate);
    }
    for (QueueConfigInfo updateQueueInfo : mutationInfo.getUpdateQueueInfo()) {
      updateQueue(updateQueueInfo, proposedConf, confUpdate);
    }
    for (Map.Entry<String, String> global : mutationInfo.getGlobalParams()
            .entrySet()) {
      confUpdate.put(global.getKey(), global.getValue());
    }
    return confUpdate;
  }

  private static void removeQueue(
          String queueToRemove, CapacitySchedulerConfiguration proposedConf,
          Map<String, String> confUpdate) throws IOException {
    if (queueToRemove == null) {
      return;
    }
    if (queueToRemove.lastIndexOf('.') == -1) {
      throw new IOException("Can't remove queue " + queueToRemove);
    }
    String queueName = queueToRemove.substring(
            queueToRemove.lastIndexOf('.') + 1);
    List<String> siblingQueues = getSiblingQueues(queueToRemove,
            proposedConf);
    if (!siblingQueues.contains(queueName)) {
      throw new IOException("Queue " + queueToRemove + " not found");
    }
    siblingQueues.remove(queueName);
    String parentQueuePath = queueToRemove.substring(0, queueToRemove
            .lastIndexOf('.'));
    proposedConf.setQueues(parentQueuePath, siblingQueues.toArray(
            new String[0]));
    String queuesConfig = CapacitySchedulerConfiguration.PREFIX
            + parentQueuePath + CapacitySchedulerConfiguration.DOT
            + CapacitySchedulerConfiguration.QUEUES;
    if (siblingQueues.isEmpty()) {
      confUpdate.put(queuesConfig, null);
      // Unset Ordering Policy of Leaf Queue converted from
      // Parent Queue after removeQueue
      String queueOrderingPolicy = CapacitySchedulerConfiguration.PREFIX
              + parentQueuePath + CapacitySchedulerConfiguration.DOT
              + ORDERING_POLICY;
      proposedConf.unset(queueOrderingPolicy);
      confUpdate.put(queueOrderingPolicy, null);
    } else {
      confUpdate.put(queuesConfig, Joiner.on(',').join(siblingQueues));
    }
    for (Map.Entry<String, String> confRemove : proposedConf.getValByRegex(
                    ".*" + queueToRemove + "\\..*")
            .entrySet()) {
      proposedConf.unset(confRemove.getKey());
      confUpdate.put(confRemove.getKey(), null);
    }
  }

  private static void addQueue(
          QueueConfigInfo addInfo, CapacitySchedulerConfiguration proposedConf,
          Map<String, String> confUpdate) throws IOException {
    if (addInfo == null) {
      return;
    }
    String queuePath = addInfo.getQueue();
    String queueName = queuePath.substring(queuePath.lastIndexOf('.') + 1);
    if (queuePath.lastIndexOf('.') == -1) {
      throw new IOException("Can't add invalid queue " + queuePath);
    } else if (getSiblingQueues(queuePath, proposedConf).contains(
            queueName)) {
      throw new IOException("Can't add existing queue " + queuePath);
    }
    String parentQueue = queuePath.substring(0, queuePath.lastIndexOf('.'));
    String[] siblings = proposedConf.getQueues(parentQueue);
    List<String> siblingQueues = siblings == null ? new ArrayList<>() :
            new ArrayList<>(Arrays.asList(siblings));
    siblingQueues.add(queuePath.substring(queuePath.lastIndexOf('.') + 1));
    proposedConf.setQueues(parentQueue,
            siblingQueues.toArray(new String[0]));
    confUpdate.put(CapacitySchedulerConfiguration.PREFIX
                    + parentQueue + CapacitySchedulerConfiguration.DOT
                    + CapacitySchedulerConfiguration.QUEUES,
            Joiner.on(',').join(siblingQueues));
    String keyPrefix = CapacitySchedulerConfiguration.PREFIX
            + queuePath + CapacitySchedulerConfiguration.DOT;
    for (Map.Entry<String, String> kv : addInfo.getParams().entrySet()) {
      String keyValue = kv.getValue();
      if (keyValue == null || keyValue.isEmpty()) {
        proposedConf.unset(keyPrefix + kv.getKey());
        confUpdate.put(keyPrefix + kv.getKey(), null);
      } else {
        proposedConf.set(keyPrefix + kv.getKey(), keyValue);
        confUpdate.put(keyPrefix + kv.getKey(), keyValue);
      }
    }
    // Unset Ordering Policy of Parent Queue converted from
    // Leaf Queue after addQueue
    String queueOrderingPolicy = CapacitySchedulerConfiguration.PREFIX
            + parentQueue + CapacitySchedulerConfiguration.DOT + ORDERING_POLICY;
    if (siblingQueues.size() == 1) {
      proposedConf.unset(queueOrderingPolicy);
      confUpdate.put(queueOrderingPolicy, null);
    }
  }

  private static void updateQueue(QueueConfigInfo updateInfo,
                                  CapacitySchedulerConfiguration proposedConf,
                                  Map<String, String> confUpdate) {
    if (updateInfo == null) {
      return;
    }
    String queuePath = updateInfo.getQueue();
    String keyPrefix = CapacitySchedulerConfiguration.PREFIX
            + queuePath + CapacitySchedulerConfiguration.DOT;
    for (Map.Entry<String, String> kv : updateInfo.getParams().entrySet()) {
      String keyValue = kv.getValue();
      if (keyValue == null || keyValue.isEmpty()) {
        proposedConf.unset(keyPrefix + kv.getKey());
        confUpdate.put(keyPrefix + kv.getKey(), null);
      } else {
        proposedConf.set(keyPrefix + kv.getKey(), keyValue);
        confUpdate.put(keyPrefix + kv.getKey(), keyValue);
      }
    }
  }

  private static List<String> getSiblingQueues(String queuePath, Configuration conf) {
    String parentQueue = queuePath.substring(0, queuePath.lastIndexOf('.'));
    String childQueuesKey = CapacitySchedulerConfiguration.PREFIX +
            parentQueue + CapacitySchedulerConfiguration.DOT +
            CapacitySchedulerConfiguration.QUEUES;
    return new ArrayList<>(conf.getTrimmedStringCollection(childQueuesKey));
  }
}

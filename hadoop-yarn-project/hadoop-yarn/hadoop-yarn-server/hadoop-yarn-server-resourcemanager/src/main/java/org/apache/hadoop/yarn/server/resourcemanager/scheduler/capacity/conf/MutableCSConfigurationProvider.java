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

import com.google.common.base.Joiner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ConfigurationMutationACLPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ConfigurationMutationACLPolicyFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore.LogMutation;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.SchedConfUpdateInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * CS configuration provider which implements
 * {@link MutableConfigurationProvider} for modifying capacity scheduler
 * configuration.
 */
public class MutableCSConfigurationProvider implements CSConfigurationProvider,
    MutableConfigurationProvider {

  private Configuration schedConf;
  private YarnConfigurationStore confStore;
  private ConfigurationMutationACLPolicy aclMutationPolicy;
  private RMContext rmContext;
  private Configuration conf;

  public MutableCSConfigurationProvider(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  @Override
  public void init(Configuration config) throws IOException {
    String store = config.get(
        YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.DEFAULT_CONFIGURATION_STORE);
    switch (store) {
    case YarnConfiguration.MEMORY_CONFIGURATION_STORE:
      this.confStore = new InMemoryConfigurationStore();
      break;
    default:
      this.confStore = YarnConfigurationStoreFactory.getStore(config);
      break;
    }
    Configuration initialSchedConf = new Configuration(false);
    initialSchedConf.addResource(YarnConfiguration.CS_CONFIGURATION_FILE);
    this.schedConf = new Configuration(false);
    // We need to explicitly set the key-values in schedConf, otherwise
    // these configuration keys cannot be deleted when
    // configuration is reloaded.
    for (Map.Entry<String, String> kv : initialSchedConf) {
      schedConf.set(kv.getKey(), kv.getValue());
    }
    confStore.initialize(config, schedConf);
    this.aclMutationPolicy = ConfigurationMutationACLPolicyFactory
        .getPolicy(config);
    aclMutationPolicy.init(config, rmContext);
    this.conf = config;
  }

  @Override
  public CapacitySchedulerConfiguration loadConfiguration(Configuration
      configuration) throws IOException {
    Configuration loadedConf = new Configuration(schedConf);
    loadedConf.addResource(configuration);
    return new CapacitySchedulerConfiguration(loadedConf, false);
  }

  @Override
  public void mutateConfiguration(UserGroupInformation user,
      SchedConfUpdateInfo confUpdate) throws IOException {
    if (!aclMutationPolicy.isMutationAllowed(user, confUpdate)) {
      throw new AccessControlException("User is not admin of all modified" +
          " queues.");
    }
    Configuration oldConf = new Configuration(schedConf);
    Map<String, String> kvUpdate = constructKeyValueConfUpdate(confUpdate);
    LogMutation log = new LogMutation(kvUpdate, user.getShortUserName());
    long id = confStore.logMutation(log);
    for (Map.Entry<String, String> kv : kvUpdate.entrySet()) {
      if (kv.getValue() == null) {
        schedConf.unset(kv.getKey());
      } else {
        schedConf.set(kv.getKey(), kv.getValue());
      }
    }
    try {
      rmContext.getScheduler().reinitialize(conf, rmContext);
    } catch (IOException e) {
      schedConf = oldConf;
      confStore.confirmMutation(id, false);
      throw e;
    }
    confStore.confirmMutation(id, true);
  }


  private Map<String, String> constructKeyValueConfUpdate(
      SchedConfUpdateInfo mutationInfo) throws IOException {
    CapacityScheduler cs = (CapacityScheduler) rmContext.getScheduler();
    CapacitySchedulerConfiguration proposedConf =
        new CapacitySchedulerConfiguration(cs.getConfiguration(), false);
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

  private void removeQueue(
      String queueToRemove, CapacitySchedulerConfiguration proposedConf,
      Map<String, String> confUpdate) throws IOException {
    if (queueToRemove == null) {
      return;
    } else {
      CapacityScheduler cs = (CapacityScheduler) rmContext.getScheduler();
      String queueName = queueToRemove.substring(
          queueToRemove.lastIndexOf('.') + 1);
      CSQueue queue = cs.getQueue(queueName);
      if (queue == null ||
          !queue.getQueuePath().equals(queueToRemove)) {
        throw new IOException("Queue " + queueToRemove + " not found");
      } else if (queueToRemove.lastIndexOf('.') == -1) {
        throw new IOException("Can't remove queue " + queueToRemove);
      }
      String parentQueuePath = queueToRemove.substring(0, queueToRemove
          .lastIndexOf('.'));
      String[] siblingQueues = proposedConf.getQueues(parentQueuePath);
      List<String> newSiblingQueues = new ArrayList<>();
      for (String siblingQueue : siblingQueues) {
        if (!siblingQueue.equals(queueName)) {
          newSiblingQueues.add(siblingQueue);
        }
      }
      proposedConf.setQueues(parentQueuePath, newSiblingQueues
          .toArray(new String[0]));
      String queuesConfig = CapacitySchedulerConfiguration.PREFIX
          + parentQueuePath + CapacitySchedulerConfiguration.DOT
          + CapacitySchedulerConfiguration.QUEUES;
      if (newSiblingQueues.size() == 0) {
        confUpdate.put(queuesConfig, null);
      } else {
        confUpdate.put(queuesConfig, Joiner.on(',').join(newSiblingQueues));
      }
      for (Map.Entry<String, String> confRemove : proposedConf.getValByRegex(
          ".*" + queueToRemove.replaceAll("\\.", "\\.") + "\\..*")
          .entrySet()) {
        proposedConf.unset(confRemove.getKey());
        confUpdate.put(confRemove.getKey(), null);
      }
    }
  }

  private void addQueue(
      QueueConfigInfo addInfo, CapacitySchedulerConfiguration proposedConf,
      Map<String, String> confUpdate) throws IOException {
    if (addInfo == null) {
      return;
    } else {
      CapacityScheduler cs = (CapacityScheduler) rmContext.getScheduler();
      String queuePath = addInfo.getQueue();
      String queueName = queuePath.substring(queuePath.lastIndexOf('.') + 1);
      if (cs.getQueue(queueName) != null) {
        throw new IOException("Can't add existing queue " + queuePath);
      } else if (queuePath.lastIndexOf('.') == -1) {
        throw new IOException("Can't add invalid queue " + queuePath);
      }
      String parentQueue = queuePath.substring(0, queuePath.lastIndexOf('.'));
      String[] siblings = proposedConf.getQueues(parentQueue);
      List<String> siblingQueues = siblings == null ? new ArrayList<String>() :
          new ArrayList<String>(Arrays.<String>asList(siblings));
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
        if (kv.getValue() == null) {
          proposedConf.unset(keyPrefix + kv.getKey());
        } else {
          proposedConf.set(keyPrefix + kv.getKey(), kv.getValue());
        }
        confUpdate.put(keyPrefix + kv.getKey(), kv.getValue());
      }
    }
  }

  private void updateQueue(QueueConfigInfo updateInfo,
      CapacitySchedulerConfiguration proposedConf,
      Map<String, String> confUpdate) {
    if (updateInfo == null) {
      return;
    } else {
      String queuePath = updateInfo.getQueue();
      String keyPrefix = CapacitySchedulerConfiguration.PREFIX
          + queuePath + CapacitySchedulerConfiguration.DOT;
      for (Map.Entry<String, String> kv : updateInfo.getParams().entrySet()) {
        if (kv.getValue() == null) {
          proposedConf.unset(keyPrefix + kv.getKey());
        } else {
          proposedConf.set(keyPrefix + kv.getKey(), kv.getValue());
        }
        confUpdate.put(keyPrefix + kv.getKey(), kv.getValue());
      }
    }
  }
}

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ConfigurationMutationACLPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ConfigurationMutationACLPolicyFactory;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.MutableConfigurationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore.LogMutation;
import org.apache.hadoop.yarn.webapp.dao.QueueConfigInfo;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

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

  public static final Logger LOG =
      LoggerFactory.getLogger(MutableCSConfigurationProvider.class);

  private Configuration schedConf;
  private Configuration oldConf;
  private YarnConfigurationStore confStore;
  private ConfigurationMutationACLPolicy aclMutationPolicy;
  private RMContext rmContext;

  public MutableCSConfigurationProvider(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  @Override
  public void init(Configuration config) throws IOException {
    String store = config.get(
        YarnConfiguration.SCHEDULER_CONFIGURATION_STORE_CLASS,
        YarnConfiguration.MEMORY_CONFIGURATION_STORE);
    switch (store) {
    case YarnConfiguration.MEMORY_CONFIGURATION_STORE:
      this.confStore = new InMemoryConfigurationStore();
      break;
    case YarnConfiguration.LEVELDB_CONFIGURATION_STORE:
      this.confStore = new LeveldbConfigurationStore();
      break;
    case YarnConfiguration.ZK_CONFIGURATION_STORE:
      this.confStore = new ZKConfigurationStore();
      break;
    case YarnConfiguration.FS_CONFIGURATION_STORE:
      this.confStore = new FSSchedulerConfigurationStore();
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
    try {
      confStore.initialize(config, schedConf, rmContext);
      confStore.checkVersion();
    } catch (Exception e) {
      throw new IOException(e);
    }
    // After initializing confStore, the store may already have an existing
    // configuration. Use this one.
    schedConf = confStore.retrieve();
    this.aclMutationPolicy = ConfigurationMutationACLPolicyFactory
        .getPolicy(config);
    aclMutationPolicy.init(config, rmContext);
  }

  @Override
  public void close() throws IOException {
    confStore.close();
  }

  @VisibleForTesting
  public YarnConfigurationStore getConfStore() {
    return confStore;
  }

  @Override
  public CapacitySchedulerConfiguration loadConfiguration(Configuration
      configuration) throws IOException {
    Configuration loadedConf = new Configuration(schedConf);
    loadedConf.addResource(configuration);
    return new CapacitySchedulerConfiguration(loadedConf, false);
  }

  @Override
  public Configuration getConfiguration() {
    return new Configuration(schedConf);
  }

  @Override
  public ConfigurationMutationACLPolicy getAclMutationPolicy() {
    return aclMutationPolicy;
  }

  @Override
  public void logAndApplyMutation(UserGroupInformation user,
      SchedConfUpdateInfo confUpdate) throws Exception {
    oldConf = new Configuration(schedConf);
    Map<String, String> kvUpdate = constructKeyValueConfUpdate(confUpdate);
    LogMutation log = new LogMutation(kvUpdate, user.getShortUserName());
    confStore.logMutation(log);
    for (Map.Entry<String, String> kv : kvUpdate.entrySet()) {
      if (kv.getValue() == null) {
        schedConf.unset(kv.getKey());
      } else {
        schedConf.set(kv.getKey(), kv.getValue());
      }
    }
  }

  @Override
  public void confirmPendingMutation(boolean isValid) throws Exception {
    confStore.confirmMutation(isValid);
    if (!isValid) {
      schedConf = oldConf;
    }
  }

  @Override
  public void reloadConfigurationFromStore() throws Exception {
    schedConf = confStore.retrieve();
  }

  private List<String> getSiblingQueues(String queuePath, Configuration conf) {
    String parentQueue = queuePath.substring(0, queuePath.lastIndexOf('.'));
    String childQueuesKey = CapacitySchedulerConfiguration.PREFIX +
        parentQueue + CapacitySchedulerConfiguration.DOT +
        CapacitySchedulerConfiguration.QUEUES;
    return new ArrayList<>(conf.getStringCollection(childQueuesKey));
  }

  private Map<String, String> constructKeyValueConfUpdate(
      SchedConfUpdateInfo mutationInfo) throws IOException {
    CapacitySchedulerConfiguration proposedConf =
        new CapacitySchedulerConfiguration(schedConf, false);
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
      String queueName = queueToRemove.substring(
          queueToRemove.lastIndexOf('.') + 1);
      if (queueToRemove.lastIndexOf('.') == -1) {
        throw new IOException("Can't remove queue " + queueToRemove);
      } else {
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
        if (siblingQueues.size() == 0) {
          confUpdate.put(queuesConfig, null);
        } else {
          confUpdate.put(queuesConfig, Joiner.on(',').join(siblingQueues));
        }
        for (Map.Entry<String, String> confRemove : proposedConf.getValByRegex(
            ".*" + queueToRemove.replaceAll("\\.", "\\.") + "\\..*")
            .entrySet()) {
          proposedConf.unset(confRemove.getKey());
          confUpdate.put(confRemove.getKey(), null);
        }
      }
    }
  }

  private void addQueue(
      QueueConfigInfo addInfo, CapacitySchedulerConfiguration proposedConf,
      Map<String, String> confUpdate) throws IOException {
    if (addInfo == null) {
      return;
    } else {
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
          new ArrayList<>(Arrays.<String>asList(siblings));
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

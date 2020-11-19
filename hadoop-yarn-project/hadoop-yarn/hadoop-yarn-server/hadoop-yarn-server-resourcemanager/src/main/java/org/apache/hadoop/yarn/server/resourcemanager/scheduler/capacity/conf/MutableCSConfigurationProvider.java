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

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
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
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.ORDERING_POLICY;

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

  private final ReentrantReadWriteLock formatLock =
      new ReentrantReadWriteLock();

  public MutableCSConfigurationProvider(RMContext rmContext) {
    this.rmContext = rmContext;
  }

  @Override
  public void init(Configuration config) throws IOException {
    this.confStore = YarnConfigurationStoreFactory.getStore(config);
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
  public long getConfigVersion() throws Exception {
    return confStore.getConfigVersion();
  }

  @Override
  public ConfigurationMutationACLPolicy getAclMutationPolicy() {
    return aclMutationPolicy;
  }

  @Override
  public LogMutation logAndApplyMutation(UserGroupInformation user,
      SchedConfUpdateInfo confUpdate) throws Exception {
    oldConf = new Configuration(schedConf);
    CapacitySchedulerConfiguration proposedConf =
            new CapacitySchedulerConfiguration(schedConf, false);
    Map<String, String> kvUpdate
            = constructKeyValueConfUpdate(proposedConf, confUpdate);
    LogMutation log = new LogMutation(kvUpdate, user.getShortUserName());
    confStore.logMutation(log);
    applyMutation(proposedConf, kvUpdate);
    schedConf = proposedConf;
    return log;
  }

  public Configuration applyChanges(Configuration oldConfiguration,
                           SchedConfUpdateInfo confUpdate) throws IOException {
    CapacitySchedulerConfiguration proposedConf =
            new CapacitySchedulerConfiguration(oldConfiguration, false);
    Map<String, String> kvUpdate
            = constructKeyValueConfUpdate(proposedConf, confUpdate);
    applyMutation(proposedConf, kvUpdate);
    return proposedConf;
  }

  private void applyMutation(Configuration conf, Map<String, String> kvUpdate) {
    for (Map.Entry<String, String> kv : kvUpdate.entrySet()) {
      if (kv.getValue() == null) {
        conf.unset(kv.getKey());
      } else {
        conf.set(kv.getKey(), kv.getValue());
      }
    }
  }

  @Override
  public void formatConfigurationInStore(Configuration config)
      throws Exception {
    formatLock.writeLock().lock();
    try {
      confStore.format();
      oldConf = new Configuration(schedConf);
      Configuration initialSchedConf = new Configuration(false);
      initialSchedConf.addResource(YarnConfiguration.CS_CONFIGURATION_FILE);
      this.schedConf = new Configuration(false);
      // We need to explicitly set the key-values in schedConf, otherwise
      // these configuration keys cannot be deleted when
      // configuration is reloaded.
      for (Map.Entry<String, String> kv : initialSchedConf) {
        schedConf.set(kv.getKey(), kv.getValue());
      }
      confStore.initialize(config, schedConf, rmContext);
      confStore.checkVersion();
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      formatLock.writeLock().unlock();
    }
  }

  @Override
  public void revertToOldConfig(Configuration config) throws Exception {
    formatLock.writeLock().lock();
    try {
      schedConf = oldConf;
      confStore.format();
      confStore.initialize(config, oldConf, rmContext);
      confStore.checkVersion();
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      formatLock.writeLock().unlock();
    }
  }

  @Override
  public void confirmPendingMutation(LogMutation pendingMutation,
      boolean isValid) throws Exception {
    formatLock.readLock().lock();
    try {
      confStore.confirmMutation(pendingMutation, isValid);
      if (!isValid) {
        schedConf = oldConf;
      }
    } finally {
      formatLock.readLock().unlock();
    }
  }

  @Override
  public void reloadConfigurationFromStore() throws Exception {
    formatLock.readLock().lock();
    try {
      schedConf = confStore.retrieve();
    } finally {
      formatLock.readLock().unlock();
    }
  }

  private List<String> getSiblingQueues(String queuePath, Configuration conf) {
    String parentQueue = queuePath.substring(0, queuePath.lastIndexOf('.'));
    String childQueuesKey = CapacitySchedulerConfiguration.PREFIX +
        parentQueue + CapacitySchedulerConfiguration.DOT +
        CapacitySchedulerConfiguration.QUEUES;
    return new ArrayList<>(conf.getStringCollection(childQueuesKey));
  }

  private Map<String, String> constructKeyValueConfUpdate(
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
      // Unset Ordering Policy of Parent Queue converted from
      // Leaf Queue after addQueue
      String queueOrderingPolicy = CapacitySchedulerConfiguration.PREFIX
          + parentQueue + CapacitySchedulerConfiguration.DOT + ORDERING_POLICY;
      if (siblingQueues.size() == 1) {
        proposedConf.unset(queueOrderingPolicy);
        confUpdate.put(queueOrderingPolicy, null);
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
        String keyValue = kv.getValue();
        if (keyValue == null || keyValue.isEmpty()) {
          keyValue = null;
          proposedConf.unset(keyPrefix + kv.getKey());
        } else {
          proposedConf.set(keyPrefix + kv.getKey(), keyValue);
        }
        confUpdate.put(keyPrefix + kv.getKey(), keyValue);
      }
    }
  }
}

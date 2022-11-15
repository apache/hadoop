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

import org.apache.hadoop.classification.VisibleForTesting;
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
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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

  // Unit test can overwrite this method
  protected Configuration getInitSchedulerConfig() {
    Configuration initialSchedConf = new Configuration(false);
    initialSchedConf.
        addResource(YarnConfiguration.CS_CONFIGURATION_FILE);
    return initialSchedConf;
  }

  @Override
  public void init(Configuration config) throws IOException {
    this.confStore = YarnConfigurationStoreFactory.getStore(config);
    initializeSchedConf();
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
  protected YarnConfigurationStore getConfStore() {
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
            = ConfigurationUpdateAssembler.constructKeyValueConfUpdate(proposedConf, confUpdate);
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
            = ConfigurationUpdateAssembler.constructKeyValueConfUpdate(proposedConf, confUpdate);
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
      initializeSchedConf();
      confStore.initialize(config, schedConf, rmContext);
      confStore.checkVersion();
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      formatLock.writeLock().unlock();
    }
  }

  private void initializeSchedConf() {
    Configuration initialSchedConf = getInitSchedulerConfig();
    this.schedConf = new Configuration(false);
    // We need to explicitly set the key-values in schedConf, otherwise
    // these configuration keys cannot be deleted when
    // configuration is reloaded.
    for (Map.Entry<String, String> kv : initialSchedConf) {
      schedConf.set(kv.getKey(), kv.getValue());
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
}

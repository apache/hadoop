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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateVersionIncompatibleException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * YarnConfigurationStore exposes the methods needed for retrieving and
 * persisting {@link CapacityScheduler} configuration via key-value
 * using write-ahead logging. When configuration mutation is requested, caller
 * should first log it with {@code logMutation}, which persists this pending
 * mutation. This mutation is merged to the persisted configuration only after
 * {@code confirmMutation} is called.
 *
 * On startup/recovery, caller should call {@code retrieve} to get all
 * confirmed mutations, then get pending mutations which were not confirmed via
 * {@code getPendingMutations}, and replay/confirm them via
 * {@code confirmMutation} as in the normal case.
 */
public abstract class YarnConfigurationStore {

  public static final Log LOG =
      LogFactory.getLog(YarnConfigurationStore.class);
  /**
   * LogMutation encapsulates the fields needed for configuration mutation
   * audit logging and recovery.
   */
  static class LogMutation implements Serializable {
    private Map<String, String> updates;
    private String user;

    /**
     * Create log mutation.
     * @param updates key-value configuration updates
     * @param user user who requested configuration change
     */
    LogMutation(Map<String, String> updates, String user) {
      this.updates = updates;
      this.user = user;
    }

    /**
     * Get key-value configuration updates.
     * @return map of configuration updates
     */
    public Map<String, String> getUpdates() {
      return updates;
    }

    /**
     * Get user who requested configuration change.
     * @return user who requested configuration change
     */
    public String getUser() {
      return user;
    }
  }

  /**
   * Initialize the configuration store, with schedConf as the initial
   * scheduler configuration. If a persisted store already exists, use the
   * scheduler configuration stored there, and ignore schedConf.
   * @param conf configuration to initialize store with
   * @param schedConf Initial key-value scheduler configuration to persist.
   * @param rmContext RMContext for this configuration store
   * @throws IOException if initialization fails
   */
  public abstract void initialize(Configuration conf, Configuration schedConf,
      RMContext rmContext) throws Exception;

  /**
   * Closes the configuration store, releasing any required resources.
   * @throws IOException on failure to close
   */
  public void close() throws IOException {}

  /**
   * Logs the configuration change to backing store.
   * @param logMutation configuration change to be persisted in write ahead log
   * @throws IOException if logging fails
   */
  public abstract void logMutation(LogMutation logMutation) throws Exception;

  /**
   * Should be called after {@code logMutation}. Gets the pending mutation
   * last logged by {@code logMutation} and marks the mutation as persisted (no
   * longer pending). If isValid is true, merge the mutation with the persisted
   * configuration.
   * @param isValid if true, update persisted configuration with pending
   *                mutation.
   * @throws Exception if mutation confirmation fails
   */
  public abstract void confirmMutation(boolean isValid) throws Exception;

  /**
   * Retrieve the persisted configuration.
   * @return configuration as key-value
   */
  public abstract Configuration retrieve() throws IOException;

  /**
   * Get a list of confirmed configuration mutations starting from a given id.
   * @param fromId id from which to start getting mutations, inclusive
   * @return list of configuration mutations
   */
  public abstract List<LogMutation> getConfirmedConfHistory(long fromId);

  /**
   * Get schema version of persisted conf store, for detecting compatibility
   * issues when changing conf store schema.
   * @return Schema version currently used by the persisted configuration store.
   * @throws Exception On version fetch failure
   */
  protected abstract Version getConfStoreVersion() throws Exception;

  /**
   * Persist the hard-coded schema version to the conf store.
   * @throws Exception On storage failure
   */
  protected abstract void storeVersion() throws Exception;

  /**
   * Get the hard-coded schema version, for comparison against the schema
   * version currently persisted.
   * @return Current hard-coded schema version
   */
  protected abstract Version getCurrentVersion();

  public void checkVersion() throws Exception {
    // TODO this was taken from RMStateStore. Should probably refactor
    Version loadedVersion = getConfStoreVersion();
    LOG.info("Loaded configuration store version info " + loadedVersion);
    if (loadedVersion != null && loadedVersion.equals(getCurrentVersion())) {
      return;
    }
    // if there is no version info, treat it as CURRENT_VERSION_INFO;
    if (loadedVersion == null) {
      loadedVersion = getCurrentVersion();
    }
    if (loadedVersion.isCompatibleTo(getCurrentVersion())) {
      LOG.info("Storing configuration store version info "
          + getCurrentVersion());
      storeVersion();
    } else {
      throw new RMStateVersionIncompatibleException(
          "Expecting configuration store version " + getCurrentVersion()
              + ", but loading version " + loadedVersion);
    }
  }

}

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
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;

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
public interface YarnConfigurationStore {

  /**
   * LogMutation encapsulates the fields needed for configuration mutation
   * audit logging and recovery.
   */
  class LogMutation {
    private Map<String, String> updates;
    private String user;
    private long id;

    /**
     * Create log mutation prior to logging.
     * @param updates key-value configuration updates
     * @param user user who requested configuration change
     */
    public LogMutation(Map<String, String> updates, String user) {
      this(updates, user, 0);
    }

    /**
     * Create log mutation for recovery.
     * @param updates key-value configuration updates
     * @param user user who requested configuration change
     * @param id transaction id of configuration change
     */
    LogMutation(Map<String, String> updates, String user, long id) {
      this.updates = updates;
      this.user = user;
      this.id = id;
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

    /**
     * Get transaction id of this configuration change.
     * @return transaction id
     */
    public long getId() {
      return id;
    }

    /**
     * Set transaction id of this configuration change.
     * @param id transaction id
     */
    public void setId(long id) {
      this.id = id;
    }
  }

  /**
   * Initialize the configuration store.
   * @param conf configuration to initialize store with
   * @param schedConf Initial key-value configuration to persist
   */
  void initialize(Configuration conf, Configuration schedConf);

  /**
   * Logs the configuration change to backing store. Generates an id associated
   * with this mutation, sets it in {@code logMutation}, and returns it.
   * @param logMutation configuration change to be persisted in write ahead log
   * @return id which configuration store associates with this mutation
   */
  long logMutation(LogMutation logMutation);

  /**
   * Should be called after {@code logMutation}. Gets the pending mutation
   * associated with {@code id} and marks the mutation as persisted (no longer
   * pending). If isValid is true, merge the mutation with the persisted
   * configuration.
   *
   * If {@code confirmMutation} is called with ids in a different order than
   * was returned by {@code logMutation}, the result is implementation
   * dependent.
   * @param id id of mutation to be confirmed
   * @param isValid if true, update persisted configuration with mutation
   *                associated with {@code id}.
   * @return true on success
   */
  boolean confirmMutation(long id, boolean isValid);

  /**
   * Retrieve the persisted configuration.
   * @return configuration as key-value
   */
  Configuration retrieve();

  /**
   * Get the list of pending mutations, in the order they were logged.
   * @return list of mutations
   */
  List<LogMutation> getPendingMutations();

  /**
   * Get a list of confirmed configuration mutations starting from a given id.
   * @param fromId id from which to start getting mutations, inclusive
   * @return list of configuration mutations
   */
  List<LogMutation> getConfirmedConfHistory(long fromId);
}

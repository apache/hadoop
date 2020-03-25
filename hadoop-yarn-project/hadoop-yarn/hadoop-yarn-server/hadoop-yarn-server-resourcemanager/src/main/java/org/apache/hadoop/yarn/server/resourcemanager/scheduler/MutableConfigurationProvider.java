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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.conf.YarnConfigurationStore.LogMutation;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

import java.io.IOException;

/**
 * Interface for allowing changing scheduler configurations.
 */
public interface MutableConfigurationProvider {

  /**
   * Get the acl mutation policy for this configuration provider.
   * @return The acl mutation policy.
   */
  ConfigurationMutationACLPolicy getAclMutationPolicy();

  /**
   * Called when a new ResourceManager is starting/becomes active. Ensures
   * configuration is up-to-date.
   * @throws Exception if configuration could not be refreshed from store
   */
  void reloadConfigurationFromStore() throws Exception;

  /**
   * Log user's requested configuration mutation, and applies it in-memory.
   * @param user User who requested the change
   * @param confUpdate User's requested configuration change
   * @return LogMutation with update info from given SchedConfUpdateInfo
   * @throws Exception if logging the mutation fails
   */
  LogMutation logAndApplyMutation(UserGroupInformation user,
      SchedConfUpdateInfo confUpdate) throws Exception;

  /**
   * Apply the changes on top of the actual configuration.
   * @param oldConfiguration actual configuration
   * @param confUpdate changelist
   * @return new configuration with the applied changed
   * @throws IOException if the merge failed
   */
  Configuration applyChanges(Configuration oldConfiguration,
                             SchedConfUpdateInfo confUpdate) throws IOException;

  /**
   * Confirm last logged mutation.
   * @param pendingMutation the log mutation to apply
   * @param isValid if the last logged mutation is applied to scheduler
   *                properly.
   * @throws Exception if confirming mutation fails
   */
  void confirmPendingMutation(LogMutation pendingMutation,
      boolean isValid) throws Exception;

  /**
   * Returns scheduler configuration cached in this provider.
   * @return scheduler configuration.
   */
  Configuration getConfiguration();

  /**
   * Get the last updated scheduler config version.
   * @return Last updated scheduler config version.
   */
  long getConfigVersion() throws Exception;

  void formatConfigurationInStore(Configuration conf) throws Exception;

  void revertToOldConfig(Configuration config) throws Exception;

  /**
   * Closes the configuration provider, releasing any required resources.
   * @throws IOException on failure to close
   */
  void close() throws IOException;
}

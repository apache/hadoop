/*
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

package org.apache.hadoop.registry.server.integration;

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.registry.client.impl.zk.RegistryBindingSource;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.registry.server.services.DeleteCompletionCallback;
import org.apache.hadoop.registry.server.services.RegistryAdminService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * Handle RM events by updating the registry
 * <p>
 * These actions are all implemented as event handlers to operations
 * which come from the RM.
 * <p>
 * This service is expected to be executed by a user with the permissions
 * to manipulate the entire registry,
 */
@InterfaceAudience.LimitedPrivate("YARN")
@InterfaceStability.Evolving
public class RMRegistryOperationsService extends RegistryAdminService {
  private static final Logger LOG =
      LoggerFactory.getLogger(RMRegistryOperationsService.class);

  private PurgePolicy purgeOnCompletionPolicy = PurgePolicy.PurgeAll;

  public RMRegistryOperationsService(String name) {
    this(name, null);
  }

  public RMRegistryOperationsService(String name,
      RegistryBindingSource bindingSource) {
    super(name, bindingSource);
  }


  /**
   * Extend the parent service initialization by verifying that the
   * service knows —in a secure cluster— the realm in which it is executing.
   * It needs this to properly build up the user names and hence their
   * access rights.
   *
   * @param conf configuration of the service
   * @throws Exception
   */
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);

    verifyRealmValidity();
  }

  public PurgePolicy getPurgeOnCompletionPolicy() {
    return purgeOnCompletionPolicy;
  }

  public void setPurgeOnCompletionPolicy(PurgePolicy purgeOnCompletionPolicy) {
    this.purgeOnCompletionPolicy = purgeOnCompletionPolicy;
  }

  public void onApplicationAttemptRegistered(ApplicationAttemptId attemptId,
      String host, int rpcport, String trackingurl) throws IOException {

  }

  public void onApplicationLaunched(ApplicationId id) throws IOException {

  }

  /**
   * Actions to take as an AM registers itself with the RM.
   * @param attemptId attempt ID
   * @throws IOException problems
   */
  public void onApplicationMasterRegistered(ApplicationAttemptId attemptId) throws
      IOException {
  }

  /**
   * Actions to take when the AM container is completed
   * @param containerId  container ID
   * @throws IOException problems
   */
  public void onAMContainerFinished(ContainerId containerId) throws
      IOException {
    LOG.info("AM Container {} finished, purging application attempt records",
        containerId);

    // remove all application attempt entries
    purgeAppAttemptRecords(containerId.getApplicationAttemptId());

    // also treat as a container finish to remove container
    // level records for the AM container
    onContainerFinished(containerId);
  }

  /**
   * remove all application attempt entries
   * @param attemptId attempt ID
   */
  protected void purgeAppAttemptRecords(ApplicationAttemptId attemptId) {
    purgeRecordsAsync("/",
        attemptId.toString(),
        PersistencePolicies.APPLICATION_ATTEMPT);
  }

  /**
   * Actions to take when an application attempt is completed
   * @param attemptId  application  ID
   * @throws IOException problems
   */
  public void onApplicationAttemptUnregistered(ApplicationAttemptId attemptId)
      throws IOException {
    LOG.info("Application attempt {} unregistered, purging app attempt records",
        attemptId);
    purgeAppAttemptRecords(attemptId);
  }

  /**
   * Actions to take when an application is completed
   * @param id  application  ID
   * @throws IOException problems
   */
  public void onApplicationCompleted(ApplicationId id)
      throws IOException {
    LOG.info("Application {} completed, purging application-level records",
        id);
    purgeRecordsAsync("/",
        id.toString(),
        PersistencePolicies.APPLICATION);
  }

  public void onApplicationAttemptAdded(ApplicationAttemptId appAttemptId) {
  }

  /**
   * This is the event where the user is known, so the user directory
   * can be created
   * @param applicationId application  ID
   * @param user username
   * @throws IOException problems
   */
  public void onStateStoreEvent(ApplicationId applicationId, String user) throws
      IOException {
    initUserRegistryAsync(user);
  }

  /**
   * Actions to take when the AM container is completed
   * @param id  container ID
   * @throws IOException problems
   */
  public void onContainerFinished(ContainerId id) throws IOException {
    LOG.info("Container {} finished, purging container-level records",
        id);
    purgeRecordsAsync("/",
        id.toString(),
        PersistencePolicies.CONTAINER);
  }

  /**
   * Queue an async operation to purge all matching records under a base path.
   * <ol>
   *   <li>Uses a depth first search</li>
   *   <li>A match is on ID and persistence policy, or, if policy==-1, any match</li>
   *   <li>If a record matches then it is deleted without any child searches</li>
   *   <li>Deletions will be asynchronous if a callback is provided</li>
   * </ol>
   * @param path base path
   * @param id ID for service record.id
   * @param persistencePolicyMatch ID for the persistence policy to match:
   * no match, no delete.
   * @return a future that returns the #of records deleted
   */
  @VisibleForTesting
  public Future<Integer> purgeRecordsAsync(String path,
      String id,
      String persistencePolicyMatch) {

    return purgeRecordsAsync(path,
        id, persistencePolicyMatch,
        purgeOnCompletionPolicy,
        new DeleteCompletionCallback());
  }

  /**
   * Queue an async operation to purge all matching records under a base path.
   * <ol>
   *   <li>Uses a depth first search</li>
   *   <li>A match is on ID and persistence policy, or, if policy==-1, any match</li>
   *   <li>If a record matches then it is deleted without any child searches</li>
   *   <li>Deletions will be asynchronous if a callback is provided</li>
   * </ol>
   * @param path base path
   * @param id ID for service record.id
   * @param persistencePolicyMatch ID for the persistence policy to match:
   * no match, no delete.
   * @param purgePolicy how to react to children under the entry
   * @param callback an optional callback
   * @return a future that returns the #of records deleted
   */
  @VisibleForTesting
  public Future<Integer> purgeRecordsAsync(String path,
      String id,
      String persistencePolicyMatch,
      PurgePolicy purgePolicy,
      BackgroundCallback callback) {
    LOG.info(" records under {} with ID {} and policy {}: {}",
        path, id, persistencePolicyMatch);
    return submit(
        new AsyncPurge(path,
            new SelectByYarnPersistence(id, persistencePolicyMatch),
            purgePolicy,
            callback));
  }

}

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

package org.apache.hadoop.yarn.server.federation.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.records.Version;

/**
 * FederationStore extends the three interfaces used to coordinate the state of
 * a federated cluster: {@link FederationApplicationHomeSubClusterStore},
 * {@link FederationMembershipStateStore}, and {@link FederationPolicyStore}.
 *
 */
public interface FederationStateStore
    extends FederationApplicationHomeSubClusterStore,
    FederationMembershipStateStore, FederationPolicyStore {

  /**
   * Initialize the FederationStore.
   *
   * @param conf the cluster configuration
   * @throws YarnException if initialization fails
   */
  void init(Configuration conf) throws YarnException;

  /**
   * Perform any cleanup operations of the StateStore.
   *
   * @throws Exception if cleanup fails
   */
  void close() throws Exception;

  /**
   * Get the {@link Version} of the underlying federation state store client.
   *
   * @return the {@link Version} of the underlying federation store client
   */
  Version getCurrentVersion();

  /**
   * Load the version information from the federation state store.
   *
   * @return the {@link Version} of the federation state store
   */
  Version loadVersion();

}

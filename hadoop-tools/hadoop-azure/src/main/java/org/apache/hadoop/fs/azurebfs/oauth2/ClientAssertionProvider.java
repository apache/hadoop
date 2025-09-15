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

package org.apache.hadoop.fs.azurebfs.oauth2;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * Interface for providing client assertions for Azure Workload Identity authentication.
 *
 * This interface allows custom implementations to provide JWT tokens through various mechanisms:
 * - Kubernetes Token Request API
 * - HashiCorp Vault
 * - Custom token services
 * - File-based tokens with custom logic
 *
 * Implementations should be thread-safe as they may be called concurrently.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ClientAssertionProvider {

  /**
   * Initializes the provider with the given configuration.
   * This method is called once after the provider is instantiated via reflection.
   *
   * @param configuration Hadoop configuration containing provider-specific settings
   * @param accountName Azure storage account name for account-specific configuration
   * @throws IOException if initialization fails
   */
  void initialize(Configuration configuration, String accountName) throws IOException;

  /**
   * Retrieves a client assertion (JWT token) for Azure Workload Identity authentication.
   *
   * The returned string should be a valid JWT token that can be used as a client assertion
   * in OAuth 2.0 client credentials flow with JWT bearer assertion.
   *
   * @return JWT token as a string
   * @throws IOException if token retrieval fails
   */
  String getClientAssertion() throws IOException;

  /**
   * Optional: Cleanup resources when the provider is no longer needed.
   * Default implementation does nothing.
   *
   * @throws IOException if cleanup fails
   */
  default void close() throws IOException {
    // Default: no-op
  }
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A storage policy specifies the placement of block replicas on specific
 * storage types.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface BlockStoragePolicySpi {

  /**
   * Return the name of the storage policy. Policies are uniquely
   * identified by name.
   *
   * @return the name of the storage policy.
   */
  String getName();

  /**
   * Return the preferred storage types associated with this policy. These
   * storage types are used sequentially for successive block replicas.
   *
   * @return preferred storage types used for placing block replicas.
   */
  StorageType[] getStorageTypes();

  /**
   * Get the fallback storage types for creating new block replicas. Fallback
   * storage types are used if the preferred storage types are not available.
   *
   * @return fallback storage types for new block replicas..
   */
  StorageType[] getCreationFallbacks();

  /**
   * Get the fallback storage types for replicating existing block replicas.
   * Fallback storage types are used if the preferred storage types are not
   * available.
   *
   * @return fallback storage types for replicating existing block replicas.
   */
  StorageType[] getReplicationFallbacks();

  /**
   * Returns true if the policy is inherit-only and cannot be changed for
   * an existing file.
   *
   * @return true if the policy is inherit-only.
   */
  boolean isCopyOnCreateFile();
}

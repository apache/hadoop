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

package org.apache.hadoop.fs.azurebfs.services;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.KeyProviderException;

/**
 * The interface that every Azure file system key provider must implement.
 */
public interface KeyProvider {
  /**
   * Key providers must implement this method. Given a list of configuration
   * parameters for the specified Azure storage account, retrieve the plaintext
   * storage account key.
   *
   * @param accountName
   *          the storage account name
   * @param conf
   *          Hadoop configuration parameters
   * @return the plaintext storage account key
   * @throws KeyProviderException if an error occurs while attempting to get
   *         the storage account key.
   */
  String getStorageAccountKey(String accountName, Configuration conf)
      throws KeyProviderException;
}

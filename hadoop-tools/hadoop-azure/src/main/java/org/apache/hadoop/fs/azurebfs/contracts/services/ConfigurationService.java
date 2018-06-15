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

package org.apache.hadoop.fs.azurebfs.contracts.services;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.ConfigurationPropertyNotFoundException;

/**
 * Configuration service collects required Azure Hadoop configurations and provides it to the consumers.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ConfigurationService extends InjectableService {
  /**
   * Checks if ABFS is running from Emulator;
   * @return is emulator mode.
   */
  boolean isEmulator();

  /**
   * Retrieves storage secure mode from Hadoop configuration;
   * @return storage secure mode;
   */
  boolean isSecureMode();

  /**
   * Retrieves storage account key for provided account name from Hadoop configuration.
   * @param accountName the account name to retrieve the key.
   * @return storage account key;
   */
  String getStorageAccountKey(String accountName) throws ConfigurationPropertyNotFoundException;

  /**
   * Returns Hadoop configuration.
   * @return Hadoop configuration.
   */
  Configuration getConfiguration();

  /**
   * Retrieves configured write buffer size
   * @return the size of the write buffer
   */
  int getWriteBufferSize();

  /**
   * Retrieves configured read buffer size
   * @return the size of the read buffer
   */
  int getReadBufferSize();

  /**
   * Retrieves configured min backoff interval
   * @return min backoff interval
   */
  int getMinBackoffIntervalMilliseconds();

  /**
   * Retrieves configured max backoff interval
   * @return max backoff interval
   */
  int getMaxBackoffIntervalMilliseconds();

  /**
   * Retrieves configured backoff interval
   * @return backoff interval
   */
  int getBackoffIntervalMilliseconds();

  /**
   * Retrieves configured num of retries
   * @return num of retries
   */
  int getMaxIoRetries();

  /**
   * Retrieves configured azure block size
   * @return azure block size
   */
  long getAzureBlockSize();

  /**
   * Retrieves configured azure block location host
   * @return azure block location host
   */
  String getAzureBlockLocationHost();

  /**
   * Retrieves configured number of concurrent threads
   * @return number of concurrent write threads
   */
  int getMaxConcurrentWriteThreads();

  /**
   * Retrieves configured number of concurrent threads
   * @return number of concurrent read threads
   */
  int getMaxConcurrentReadThreads();

  /**
   * Retrieves configured boolean for tolerating out of band writes to files
   * @return configured boolean for tolerating out of band writes to files
   */
  boolean getTolerateOobAppends();

  /**
   * Retrieves the comma-separated list of directories to receive special treatment so that folder
   * rename is made atomic. The default value for this setting is just '/hbase'.
   * Example directories list : <value>/hbase,/data</value>
   * @see <a href="https://hadoop.apache.org/docs/stable/hadoop-azure/index.html#Configuring_Credentials">AtomicRenameProperty</a>
   * @return atomic rename directories
   */
  String getAzureAtomicRenameDirs();

  /**
   * Retrieves configured boolean for creating remote file system during initialization
   * @return configured boolean for creating remote file system during initialization
   */
  boolean getCreateRemoteFileSystemDuringInitialization();

  /**
   * Retrieves configured value of read ahead queue
   * @return depth of read ahead
   */
  int getReadAheadQueueDepth();
}
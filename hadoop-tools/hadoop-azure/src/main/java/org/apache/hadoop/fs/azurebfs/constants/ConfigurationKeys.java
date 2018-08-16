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

package org.apache.hadoop.fs.azurebfs.constants;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Responsible to keep all the Azure Blob File System configurations keys in Hadoop configuration file.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ConfigurationKeys {
  public static final String FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME = "fs.azure.account.key.";
  public static final String FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME_REGX = "fs\\.azure\\.account\\.key\\.(.*)";
  public static final String FS_AZURE_SECURE_MODE = "fs.azure.secure.mode";

  // Retry strategy defined by the user
  public static final String AZURE_MIN_BACKOFF_INTERVAL = "fs.azure.io.retry.min.backoff.interval";
  public static final String AZURE_MAX_BACKOFF_INTERVAL = "fs.azure.io.retry.max.backoff.interval";
  public static final String AZURE_BACKOFF_INTERVAL = "fs.azure.io.retry.backoff.interval";
  public static final String AZURE_MAX_IO_RETRIES = "fs.azure.io.retry.max.retries";

  // Remove this and use common azure storage emulator property for public release.
  public static final String FS_AZURE_EMULATOR_ENABLED = "fs.azure.abfs.emulator.enabled";

  // Read and write buffer sizes defined by the user
  public static final String AZURE_WRITE_BUFFER_SIZE = "fs.azure.write.request.size";
  public static final String AZURE_READ_BUFFER_SIZE = "fs.azure.read.request.size";
  public static final String AZURE_BLOCK_SIZE_PROPERTY_NAME = "fs.azure.block.size";
  public static final String AZURE_BLOCK_LOCATION_HOST_PROPERTY_NAME = "fs.azure.block.location.impersonatedhost";
  public static final String AZURE_CONCURRENT_CONNECTION_VALUE_OUT = "fs.azure.concurrentRequestCount.out";
  public static final String AZURE_CONCURRENT_CONNECTION_VALUE_IN = "fs.azure.concurrentRequestCount.in";
  public static final String AZURE_TOLERATE_CONCURRENT_APPEND = "fs.azure.io.read.tolerate.concurrent.append";
  public static final String AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION = "fs.azure.createRemoteFileSystemDuringInitialization";
  public static final String AZURE_SKIP_USER_GROUP_METADATA_DURING_INITIALIZATION = "fs.azure.skipUserGroupMetadataDuringInitialization";
  public static final String FS_AZURE_AUTOTHROTTLING_ENABLE = "fs.azure.autothrottling.enable";
  public static final String FS_AZURE_ATOMIC_RENAME_KEY = "fs.azure.atomic.rename.key";
  public static final String FS_AZURE_READ_AHEAD_QUEUE_DEPTH = "fs.azure.readaheadqueue.depth";
  public static final String FS_AZURE_ENABLE_FLUSH = "fs.azure.enable.flush";
  public static final String FS_AZURE_USER_AGENT_PREFIX_KEY = "fs.azure.user.agent.prefix";
  public static final String FS_AZURE_SSL_CHANNEL_MODE_KEY = "fs.azure.ssl.channel.mode";

  public static final String AZURE_KEY_ACCOUNT_KEYPROVIDER_PREFIX = "fs.azure.account.keyprovider.";
  public static final String AZURE_KEY_ACCOUNT_SHELLKEYPROVIDER_SCRIPT = "fs.azure.shellkeyprovider.script";

  private ConfigurationKeys() {}
}

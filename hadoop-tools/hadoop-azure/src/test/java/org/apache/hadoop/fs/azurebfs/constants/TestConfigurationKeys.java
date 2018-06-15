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
public final class TestConfigurationKeys {
  public static final String FS_AZURE_TEST_ACCOUNT_NAME = "fs.azure.test.account.name";
  public static final String FS_AZURE_TEST_ACCOUNT_KEY_PREFIX = "fs.azure.test.account.key.";
  public static final String FS_AZURE_TEST_HOST_NAME = "fs.azure.test.host.name";
  public static final String FS_AZURE_TEST_HOST_PORT = "fs.azure.test.host.port";
  public static final String FS_AZURE_CONTRACT_TEST_URI = "fs.contract.test.fs.abfs";

  private TestConfigurationKeys() {}
}
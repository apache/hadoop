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

package org.apache.hadoop.fs.azure;

import org.apache.hadoop.conf.Configuration;

import static org.apache.hadoop.fs.azure.SecureStorageInterfaceImpl.KEY_USE_CONTAINER_SASKEY_FOR_ALL_ACCESS;

/**
 * Test class to hold all WASB authorization tests that use blob-specific keys
 * to access storage.
 */
public class ITestNativeAzureFSAuthWithBlobSpecificKeys
    extends ITestNativeAzureFileSystemAuthorizationWithOwner {


  @Override
  public Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    conf.set(KEY_USE_CONTAINER_SASKEY_FOR_ALL_ACCESS, "false");
    return conf;
  }

}

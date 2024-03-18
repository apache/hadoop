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

package org.apache.hadoop.fs.azurebfs.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;

import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;

/**
 * This looks like a test, but it is really a command to invoke to
 * clean up containers created in other test runs.
 *
 */
public class CleanupTestContainers extends AbstractAbfsIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(CleanupTestContainers.class);
  private static final String CONTAINER_PREFIX = "abfs-testcontainer-";

  public CleanupTestContainers() throws Exception {
  }

  @org.junit.Test
  public void testDeleteContainers() throws Throwable {
    int count = 0;
    AbfsConfiguration abfsConfig = getAbfsStore(getFileSystem()).getAbfsConfiguration();
    String accountName = abfsConfig.getAccountName().split("\\.")[0];
    LOG.debug("Deleting test containers in account - {}", abfsConfig.getAccountName());

    String accountKey = abfsConfig.getStorageAccountKey();
    if ((accountKey == null) || (accountKey.isEmpty())) {
      LOG.debug("Clean up not possible. Account ket not present in config");
    }
    final StorageCredentials credentials;
    credentials = new StorageCredentialsAccountAndKey(
        accountName, accountKey);
    CloudStorageAccount storageAccount = new CloudStorageAccount(credentials, true);
    CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
    Iterable<CloudBlobContainer> containers
        = blobClient.listContainers(CONTAINER_PREFIX);
    for (CloudBlobContainer container : containers) {
      LOG.info("Container {} URI {}",
          container.getName(),
          container.getUri());
      if (container.deleteIfExists()) {
        count++;
        LOG.info("Current deleted test containers count - #{}", count);
      }
    }
    LOG.info("Summary: Deleted {} test containers", count);
  }
}

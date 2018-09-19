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

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

import org.junit.Assume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount;
import org.apache.hadoop.fs.azurebfs.AbstractAbfsIntegrationTest;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import static org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys.TEST_CONTAINER_PREFIX;

/**
 * Some Utils for ABFS tests.
 */
public final class AbfsTestUtils extends AbstractAbfsIntegrationTest{
    private static final Logger LOG =
            LoggerFactory.getLogger(AbfsTestUtils.class);

  public AbfsTestUtils() throws Exception {
    super();
  }

  /**
   * If unit tests were interrupted and crushed accidentally, the test containers won't be deleted.
   * In that case, dev can use this tool to list and delete all test containers.
   * By default, all test container used in E2E tests sharing same prefix: "abfs-testcontainer-"
   */

  public void checkContainers() throws Throwable {
    Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
    int count = 0;
    CloudStorageAccount storageAccount = AzureBlobStorageTestAccount.createTestAccount();
    CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
    Iterable<CloudBlobContainer> containers
            = blobClient.listContainers(TEST_CONTAINER_PREFIX);
    for (CloudBlobContainer container : containers) {
      count++;
      LOG.info("Container {}, URI {}",
              container.getName(),
              container.getUri());
    }
    LOG.info("Found {} test containers", count);
  }


  public void deleteContainers() throws Throwable {
    Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
    int count = 0;
    CloudStorageAccount storageAccount = AzureBlobStorageTestAccount.createTestAccount();
    CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
    Iterable<CloudBlobContainer> containers
            = blobClient.listContainers(TEST_CONTAINER_PREFIX);
    for (CloudBlobContainer container : containers) {
      LOG.info("Container {} URI {}",
              container.getName(),
              container.getUri());
      if (container.deleteIfExists()) {
        count++;
      }
    }
    LOG.info("Deleted {} test containers", count);
  }
}

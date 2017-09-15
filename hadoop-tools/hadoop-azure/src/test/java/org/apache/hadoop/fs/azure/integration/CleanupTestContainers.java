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

package org.apache.hadoop.fs.azure.integration;

import java.util.EnumSet;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.junit.Test;

import org.apache.hadoop.fs.azure.AbstractWasbTestBase;
import org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount;

/**
 * This looks like a test, but it is really a command to invoke to
 * clean up containers created in other test runs.
 *
 */
public class CleanupTestContainers extends AbstractWasbTestBase {

  private static final String CONTAINER_PREFIX = "wasbtests-";

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create(
        "CleanupTestContainers",
        EnumSet.noneOf(AzureBlobStorageTestAccount.CreateOptions.class),
        createConfiguration(),
        true);
  }

  @Test
  public void testEnumContainers() throws Throwable {
    describe("Enumerating all the WASB test containers");

    int count = 0;
    CloudStorageAccount storageAccount = getTestAccount().getRealAccount();
    CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
    Iterable<CloudBlobContainer> containers
        = blobClient.listContainers(CONTAINER_PREFIX);
    for (CloudBlobContainer container : containers) {
      count++;
      LOG.info("Container {} URI {}",
          container.getName(),
          container.getUri());
    }
    LOG.info("Found {} test containers", count);
  }

  @Test
  public void testDeleteContainers() throws Throwable {
    describe("Delete all the WASB test containers");
    int count = 0;
    CloudStorageAccount storageAccount = getTestAccount().getRealAccount();
    CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
    Iterable<CloudBlobContainer> containers
        = blobClient.listContainers(CONTAINER_PREFIX);
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

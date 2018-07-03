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

import org.apache.hadoop.fs.azure.AzureBlobStorageTestAccount;
import org.junit.Test;

import static org.apache.hadoop.fs.azurebfs.DependencyInjectedTest.TEST_CONTAINER_PREFIX;

/**
 * If unit tests were interrupted and crushed accidentally, the test containers won't be deleted.
 * In that case, dev can use this tool to list and delete all test containers.
 * By default, all test container used in E2E tests sharing same prefix: "abfs-testcontainer-"
 */
public class CleanUpAbfsTestContainer {
  @Test
  public void testEnumContainers() throws Throwable {
    int count = 0;
    CloudStorageAccount storageAccount = AzureBlobStorageTestAccount.createTestAccount();
    CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
    Iterable<CloudBlobContainer> containers
            = blobClient.listContainers(TEST_CONTAINER_PREFIX);
    for (CloudBlobContainer container : containers) {
      count++;
      System.out.println(String.format("Container %s URI %s",
              container.getName(),
              container.getUri()));
    }
    System.out.println(String.format("Found %d test containers", count));
  }

  @Test
  public void testDeleteContainers() throws Throwable {
    int count = 0;
    CloudStorageAccount storageAccount = AzureBlobStorageTestAccount.createTestAccount();
    CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
    Iterable<CloudBlobContainer> containers
            = blobClient.listContainers(TEST_CONTAINER_PREFIX);
    for (CloudBlobContainer container : containers) {
      System.out.println(String.format("Container %s URI %s",
              container.getName(),
              container.getUri()));
      if (container.deleteIfExists()) {
        count++;
      }
    }
    System.out.println(String.format("Deleted %s test containers", count));
  }
}

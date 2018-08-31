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

package org.apache.hadoop.fs.azurebfs;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/**
 * Test AzureBlobFileSystem back compatibility with WASB.
 */
public class ITestAzureBlobFileSystemBackCompat extends
    AbstractAbfsIntegrationTest {
  public ITestAzureBlobFileSystemBackCompat() {
    super();
    Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
  }

  @Test
  public void testBlobBackCompat() throws Exception {
    final AzureBlobFileSystem fs = this.getFileSystem();
    // test only valid for non-namespace enabled account
    Assume.assumeFalse(fs.getIsNamespaceEnabeld());
    String storageConnectionString = getBlobConnectionString();
    CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
    CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
    CloudBlobContainer container = blobClient.getContainerReference(this.getFileSystemName());
    container.createIfNotExists();

    CloudBlockBlob blockBlob = container.getBlockBlobReference("test/10/10/10");
    blockBlob.uploadText("");

    blockBlob = container.getBlockBlobReference("test/10/123/3/2/1/3");
    blockBlob.uploadText("");

    FileStatus[] fileStatuses = fs.listStatus(new Path("/test/10/"));
    assertEquals(2, fileStatuses.length);
    assertEquals("10", fileStatuses[0].getPath().getName());
    assertTrue(fileStatuses[0].isDirectory());
    assertEquals(0, fileStatuses[0].getLen());
    assertEquals("123", fileStatuses[1].getPath().getName());
    assertTrue(fileStatuses[1].isDirectory());
    assertEquals(0, fileStatuses[1].getLen());
  }

  private String getBlobConnectionString() {
    String connectionString;
    if (isIPAddress()) {
      connectionString = "DefaultEndpointsProtocol=http;BlobEndpoint=http://"
              + this.getHostName() + ":8880/" + this.getAccountName().split("\\.") [0]
              + ";AccountName=" + this.getAccountName().split("\\.")[0]
              + ";AccountKey=" + this.getAccountKey();
    }
    else {
      connectionString = "DefaultEndpointsProtocol=http;BlobEndpoint=http://"
              + this.getAccountName().replaceFirst("\\.dfs\\.", ".blob.")
              + ";AccountName=" + this.getAccountName().split("\\.")[0]
              + ";AccountKey=" + this.getAccountKey();
    }

    return connectionString;
  }
}

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
import java.net.URI;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;


public class TestFileSystemOperationExceptionMessage extends
  NativeAzureFileSystemBaseTest {

  @Test
  public void testAnonymouseCredentialExceptionMessage() throws Throwable{

    Configuration conf = AzureBlobStorageTestAccount.createTestConfiguration();
    String testStorageAccount = conf.get("fs.azure.test.account.name");
    conf = new Configuration();
    conf.set("fs.AbstractFileSystem.wasb.impl", "org.apache.hadoop.fs.azure.Wasb");
    conf.set("fs.azure.skip.metrics", "true");

    String testContainer = UUID.randomUUID().toString();
    String wasbUri = String.format("wasb://%s@%s",
        testContainer, testStorageAccount);

    String expectedErrorMessage =
        String.format("Container %s in account %s not found, and we can't create it "
            + "using anoynomous credentials, and no credentials found for "
            + "them in the configuration.", testContainer, testStorageAccount);

    fs = new NativeAzureFileSystem();
    try {
      fs.initialize(new URI(wasbUri), conf);
    } catch (Exception ex) {

      Throwable innerException = ex.getCause();
      while (innerException != null
             && !(innerException instanceof AzureException)) {
        innerException = innerException.getCause();
      }

      if (innerException != null) {
        String exceptionMessage = innerException.getMessage();
        if (exceptionMessage == null
            || exceptionMessage.length() == 0) {
          Assert.fail();}
        else {
          Assert.assertTrue(exceptionMessage.equals(expectedErrorMessage));
        }
      } else {
        Assert.fail();
      }
    }
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.create();
  }
}
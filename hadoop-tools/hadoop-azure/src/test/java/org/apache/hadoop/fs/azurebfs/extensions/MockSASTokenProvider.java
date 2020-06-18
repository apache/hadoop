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

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.AccessControlException;

import org.apache.hadoop.fs.azurebfs.AbfsConfiguration;
import org.apache.hadoop.fs.azurebfs.utils.Base64;
import org.apache.hadoop.fs.azurebfs.utils.ServiceSASGenerator;

/**
 * A mock SAS token provider implementation
 */
public class MockSASTokenProvider implements SASTokenProvider {

  private byte[] accountKey;
  private ServiceSASGenerator generator;
  private boolean skipAuthorizationForTestSetup = false;

  // For testing we use a container SAS for all operations.
  private String generateSAS(byte[] accountKey, String accountName, String fileSystemName) {
     return generator.getContainerSASWithFullControl(accountName, fileSystemName);
  }

  @Override
  public void initialize(Configuration configuration, String accountName) throws IOException {
    try {
      AbfsConfiguration abfsConfig = new AbfsConfiguration(configuration, accountName);
      accountKey = Base64.decode(abfsConfig.getStorageAccountKey());
    } catch (Exception ex) {
      throw new IOException(ex);
    }
    generator = new ServiceSASGenerator(accountKey);
  }

  /**
   * Invokes the authorizer to obtain a SAS token.
   *
   * @param accountName the name of the storage account.
   * @param fileSystem the name of the fileSystem.
   * @param path the file or directory path.
   * @param operation the operation to be performed on the path.
   * @return a SAS token to perform the request operation.
   * @throws IOException if there is a network error.
   * @throws AccessControlException if access is denied.
   */
  @Override
  public String getSASToken(String accountName, String fileSystem, String path,
                     String operation) throws IOException, AccessControlException {
    if (!isSkipAuthorizationForTestSetup() && path.contains("unauthorized")) {
      throw new AccessControlException(
          "The user is not authorized to perform this operation.");
    }

    return generateSAS(accountKey, accountName, fileSystem);
  }

  public boolean isSkipAuthorizationForTestSetup() {
    return skipAuthorizationForTestSetup;
  }

  public void setSkipAuthorizationForTestSetup(
      boolean skipAuthorizationForTestSetup) {
    this.skipAuthorizationForTestSetup = skipAuthorizationForTestSetup;
  }
}

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

/**
 * Iterface used by AzureNativeFileSysteStore to retrieve SAS Keys for the
 * respective azure storage entity. This interface is expected to be
 * implemented in two modes:
 * 1) Local Mode: In this mode SAS Keys are generated
 *    in same address space as the WASB. This will be primarily used for
 *    testing purposes.
 * 2) Remote Mode: In this mode SAS Keys are generated in a sepearte process
 *    other than WASB and will be communicated via client.
 */
public interface SASKeyGeneratorInterface {

  /**
   * Interface method to retrieve SAS Key for a container within the storage
   * account.
   *
   * @param accountName
   *          - Storage account name
   * @param container
   *          - Container name within the storage account.
   * @return SAS URI for the container.
   * @throws SASKeyGenerationException Exception that gets thrown during
   * generation of SAS Key.
   */
  URI getContainerSASUri(String accountName, String container)
      throws SASKeyGenerationException;

  /**
   * Interface method to retrieve SAS Key for a blob within the container of the
   * storage account.
   *
   * @param accountName
   *          - Storage account name
   * @param container
   *          - Container name within the storage account.
   * @param relativePath
   *          - Relative path within the container
   * @return SAS URI for the relative path blob.
   * @throws SASKeyGenerationException Exception that gets thrown during
   * generation of SAS Key.
   */
  URI getRelativeBlobSASUri(String accountName, String container,
      String relativePath) throws SASKeyGenerationException;
}
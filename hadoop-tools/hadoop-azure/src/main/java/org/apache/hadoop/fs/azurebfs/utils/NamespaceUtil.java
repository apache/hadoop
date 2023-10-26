/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.utils;

import java.net.HttpURLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azurebfs.constants.AbfsHttpConstants;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;

/**
 * Utility class to provide method which can return if the account is namespace
 * enabled or not.
 */
public final class NamespaceUtil {

  public static final Logger LOG = LoggerFactory.getLogger(NamespaceUtil.class);

  private NamespaceUtil() {

  }

  /**
   * Return if the account used in the provided abfsClient object namespace enabled
   * or not.
   * It would call {@link org.apache.hadoop.fs.azurebfs.services.AbfsClient#getAclStatus(String, TracingContext)}.
   * <ol>
   *   <li>
   *     If the API call is successful, then the account is namespace enabled.
   *   </li>
   *   <li>
   *     If the server returns with {@link java.net.HttpURLConnection#HTTP_BAD_REQUEST}, the account is non-namespace enabled.
   *   </li>
   *   <li>
   *     If the server call gets some other exception, then the method would throw the exception.
   *   </li>
   * </ol>
   * @param abfsClient client for which namespace-enabled to be checked.
   * @param tracingContext object to correlate Store requests.
   * @return if the account corresponding to the given client is namespace-enabled
   * or not.
   * @throws AzureBlobFileSystemException throws back the exception the method receives
   * from the {@link AbfsClient#getAclStatus(String, TracingContext)}. In case it gets
   * {@link AbfsRestOperationException}, it checks if the exception statusCode is
   * BAD_REQUEST or not. If not, then it will pass the exception to the calling method.
   */
  public static Boolean isNamespaceEnabled(final AbfsClient abfsClient,
      final TracingContext tracingContext)
      throws AzureBlobFileSystemException {
    Boolean isNamespaceEnabled;
    try {
      LOG.debug("Get root ACL status");
      abfsClient.getAclStatus(AbfsHttpConstants.ROOT_PATH, tracingContext);
      isNamespaceEnabled = true;
    } catch (AbfsRestOperationException ex) {
      // Get ACL status is a HEAD request, its response doesn't contain
      // errorCode
      // So can only rely on its status code to determine its account type.
      if (HttpURLConnection.HTTP_BAD_REQUEST != ex.getStatusCode()) {
        throw ex;
      }
      isNamespaceEnabled = false;
    } catch (AzureBlobFileSystemException ex) {
      throw ex;
    }
    return isNamespaceEnabled;
  }
}

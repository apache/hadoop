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
package org.apache.hadoop.yarn.server.federation.store.utils;

import org.apache.hadoop.yarn.security.client.YARNDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.federation.store.exception.FederationStateStoreInvalidInputException;
import org.apache.hadoop.yarn.server.federation.store.records.RouterRMTokenRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterStoreToken;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKeyRequest;
import org.apache.hadoop.yarn.server.federation.store.records.RouterMasterKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FederationRouterRMTokenInputValidator {

  private static final Logger LOG =
      LoggerFactory.getLogger(FederationRouterRMTokenInputValidator.class);

  private FederationRouterRMTokenInputValidator() {
  }

  /**
   * We will check with the RouterRMTokenRequest{@link RouterRMTokenRequest}
   * to ensure that the request object is not empty and that the RouterStoreToken is not empty.
   *
   * @param request RouterRMTokenRequest Request.
   * @throws FederationStateStoreInvalidInputException if the request is invalid.
   */
  public static void validate(RouterRMTokenRequest request)
      throws FederationStateStoreInvalidInputException {

    if (request == null) {
      String message = "Missing RouterRMToken Request."
          + " Please try again by specifying a router rm token information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    RouterStoreToken storeToken = request.getRouterStoreToken();
    if (storeToken == null) {
      String message = "Missing RouterStoreToken."
          + " Please try again by specifying a router rm token information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    try {
      YARNDelegationTokenIdentifier identifier = storeToken.getTokenIdentifier();
      if (identifier == null) {
        String message = "Missing YARNDelegationTokenIdentifier."
            + " Please try again by specifying a router rm token information.";
        LOG.warn(message);
        throw new FederationStateStoreInvalidInputException(message);
      }
    } catch (Exception e) {
      throw new FederationStateStoreInvalidInputException(e);
    }
  }

  /**
   * We will check with the RouterMasterKeyRequest{@link RouterMasterKeyRequest}
   * to ensure that the request object is not empty and that the RouterMasterKey is not empty.
   *
   * @param request RouterMasterKey Request.
   * @throws FederationStateStoreInvalidInputException if the request is invalid.
   */
  public static void validate(RouterMasterKeyRequest request)
      throws FederationStateStoreInvalidInputException {

    // Verify the request to ensure that the request is not empty,
    // if the request is found to be empty, an exception will be thrown.
    if (request == null) {
      String message = "Missing RouterMasterKey Request."
          + " Please try again by specifying a router master key request information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }

    // Check whether the masterKey is empty,
    // if the masterKey is empty, throw an exception message.
    RouterMasterKey masterKey = request.getRouterMasterKey();
    if (masterKey == null) {
      String message = "Missing RouterMasterKey."
          + " Please try again by specifying a router master key information.";
      LOG.warn(message);
      throw new FederationStateStoreInvalidInputException(message);
    }
  }
}

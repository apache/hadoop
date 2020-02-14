/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.extensions;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsAuthorizationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsAuthorizerUnhandledException;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

/**
 * Interface to support authorization in Azure Blob File System.
 * Authorizer could choose to provide
 * - just the authorization status or
 * - additionally provide ABFS SAS tokens, in which case they need
 * to inherit from the AbfsSASAuthorizer class.
 */
@InterfaceAudience.LimitedPrivate("authorization-subsystems")
@InterfaceStability.Unstable
public interface AbfsAuthorizer {
  /**
   * Initialize authorizer for Azure Blob File System.
   *
   * @throws AbfsAuthorizationException if unable to initialize the authorizer.
   * @throws IOException network problems or similar.
   */
  void init()
      throws AbfsAuthorizationException, AbfsAuthorizerUnhandledException;

  /**
   * Get AuthType supported by Authorizer if Authorizer would be providing
   * SAS token to ABFS server.
   *
   * If Authorizer is not going to provide any SAS tokens, return AuthType.None
   * @return AuthType supported by AbfsAuthorizer
   */
  AuthType getAuthType();

  /**
   * Checks if the provided {@link AuthorizationResource} is authorized to
   * perform the requested action.
   *
   * @param authorizationResource which contains the store path and the store
   * operation action string for which authorization is requested.
   * @return AuthorizationResult which contains the SAS token for each
   * AuthorizationResource present in the request
   **/
  AuthorizationResult checkPrivileges(
      AuthorizationResource... authorizationResource)
      throws AbfsAuthorizationException, AbfsAuthorizerUnhandledException;
}
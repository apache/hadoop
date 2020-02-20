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

import java.io.*;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.*;
import org.apache.hadoop.fs.azurebfs.services.AuthType;

import static org.apache.hadoop.fs.azurebfs.constants.AbfsAuthorizerConstants.*;
import static org.apache.hadoop.fs.azurebfs.extensions.MockAbfsAuthorizerEnums.*;

/**
 * A mock Azure Blob File System Authorization Implementation
 */
public abstract class MockAbfsAuthorizer implements AbfsAuthorizer {
  private boolean skipAuthCheck = false;

  public MockAbfsAuthorizer(Configuration conf) {
  }

  public abstract AuthorizationResourceResult getAuthzResourceResult(AuthorizationResource resource)
      throws InvalidUriException, AbfsAuthorizerUnhandledException,
      AbfsAuthorizationException, UnsupportedEncodingException;

  @Override
  public AuthType getAuthType() {
    return AuthType.None;
  }

  @Override
  public AuthorizationResult checkPrivileges(
      AuthorizationResource... authorizationResource)
      throws AbfsAuthorizationException, AbfsAuthorizerUnhandledException {
    AuthorizationResourceResult[] authResourceResult =
        new AuthorizationResourceResult[authorizationResource.length];
    int index = -1;

    try {
      for (AuthorizationResource authzResource : authorizationResource) {
        if (!isSkipAuthCheck()
        && authzResource.getStorePathUri().getPath().contains(
            "unauthorized"))
        {
          throw new AbfsAuthorizationException(
              "User is not authorized to perform" + " action");
        }

        authResourceResult[++index] = getAuthzResourceResult(authzResource);
      }
    } catch (InvalidUriException | UnsupportedEncodingException ex) {
      throw new AbfsAuthorizerUnhandledException(ex);
    }

    AuthorizationResult result = new AuthorizationResult(true,
        authResourceResult);

    return result;
  }

  public boolean isSkipAuthCheck() {
    return skipAuthCheck;
  }

  public void setSkipAuthCheck(boolean skipAuthCheck) {
    this.skipAuthCheck = skipAuthCheck;
  }
}

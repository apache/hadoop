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

/**
 * AuthorizationResult will be returned by Authorizer with store path URI and
 * supported Auth token if the AuthorizationResource is found authorized for
 * specified action
 */
public class AuthorizationResult {

  private boolean isAuthorized;
  private AuthorizationResourceResult[] authResourceResult;

  public boolean isAuthorized() {
    return isAuthorized;
  }

  public void setAuthorized(boolean authorized) {
    isAuthorized = authorized;
  }

  public void setAuthResourceResult(
      final AuthorizationResourceResult[] authResourceResult) {
    this.authResourceResult =
        new AuthorizationResourceResult[authResourceResult.length];
    System.arraycopy(authResourceResult, 0, this.authResourceResult, 0,
        authResourceResult.length);
  }

  public final AuthorizationResourceResult[] getAuthResourceResult() {
    AuthorizationResourceResult[] authResourceResult =
        new AuthorizationResourceResult[this.authResourceResult.length];
    System.arraycopy(this.authResourceResult, 0,
        authResourceResult, 0,
        this.authResourceResult.length);
    return authResourceResult;
  }
}
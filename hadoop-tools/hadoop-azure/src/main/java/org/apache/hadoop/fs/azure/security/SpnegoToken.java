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

package org.apache.hadoop.fs.azure.security;

import org.apache.hadoop.security.authentication.client.AuthenticatedURL;

/**
 * Class to represent SPNEGO token.
 */
public class SpnegoToken {
  private AuthenticatedURL.Token token;
  private long expiryTime;
  private static final long TOKEN_VALIDITY_TIME_IN_MS = 60 * 60 * 1000L;

  public SpnegoToken(AuthenticatedURL.Token token) {
    this.token = token;
    //set the expiry time of the token to be 60 minutes,
    // actual token will be valid for more than few hours and treating token as opaque.
    this.expiryTime = System.currentTimeMillis() + TOKEN_VALIDITY_TIME_IN_MS;
  }

  public AuthenticatedURL.Token getToken() {
    return token;
  }

  public long getExpiryTime() {
    return expiryTime;
  }

  public boolean isTokenValid() {
    return (expiryTime >= System.currentTimeMillis());
  }
}

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

package org.apache.hadoop.runc.docker.auth;

import org.apache.http.annotation.Contract;
import org.apache.http.annotation.ThreadingBehavior;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.BasicUserPrincipal;
import org.apache.http.auth.Credentials;

import java.security.Principal;

@Contract(threading = ThreadingBehavior.SAFE)
public class BearerCredentials implements Credentials {

  private final AuthScope authScope;
  private final BasicUserPrincipal principal;
  private AuthToken token;

  public BearerCredentials(AuthScope authScope) {
    this(authScope, null);
  }

  public BearerCredentials(AuthScope authScope, AuthToken token) {
    this.authScope = authScope;
    this.principal = new BasicUserPrincipal("bearer");
    this.token = token;
  }

  @Override
  public Principal getUserPrincipal() {
    return principal;
  }

  @Override
  public synchronized String getPassword() {
    return (token == null) ? null : token.getToken();
  }

  public synchronized boolean isValid() {
    return (token != null) && (!token.isExpired());
  }

  public synchronized AuthToken getToken() {
    return token;
  }

  public synchronized void setToken(AuthToken token) {
    this.token = token;
  }

  public AuthScope getAuthScope() {
    return authScope;
  }

}

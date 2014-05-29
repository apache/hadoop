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

package org.apache.hadoop.yarn.security.client;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;

/**
 * DelegationToken operations.
 */
@Unstable
@Private
public enum TimelineDelegationTokenOperation {
  // TODO: need think about which ops can be done without kerberos
  // credentials, for safety, we enforces all need kerberos credentials now.
  GETDELEGATIONTOKEN(HttpGet.METHOD_NAME, true),
  RENEWDELEGATIONTOKEN(HttpPut.METHOD_NAME, true),
  CANCELDELEGATIONTOKEN(HttpPut.METHOD_NAME, true);

  private String httpMethod;
  private boolean requiresKerberosCredentials;

  private TimelineDelegationTokenOperation(String httpMethod,
      boolean requiresKerberosCredentials) {
    this.httpMethod = httpMethod;
    this.requiresKerberosCredentials = requiresKerberosCredentials;
  }

  public String getHttpMethod() {
    return httpMethod;
  }

  public boolean requiresKerberosCredentials() {
    return requiresKerberosCredentials;
  }

}
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.swift.auth;

/**
 * Class that represents authentication to OpenStack Keystone.
 * Contains basic authentication information.
 * Used when {@link ApiKeyAuthenticationRequest} is not applicable.
 * (problem with different Keystone installations/versions/modifications)
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS.
 */
public class KeyStoneAuthRequest extends AuthenticationRequest {

  /**
   * Credentials for Keystone authentication
   */
  private KeystoneApiKeyCredentials apiAccessKeyCredentials;

  /**
   * @param tenant                  Keystone tenant name for authentication
   * @param apiAccessKeyCredentials Credentials for authentication
   */
  public KeyStoneAuthRequest(String tenant, KeystoneApiKeyCredentials apiAccessKeyCredentials) {
    this.apiAccessKeyCredentials = apiAccessKeyCredentials;
    this.tenantName = tenant;
  }

  public KeystoneApiKeyCredentials getApiAccessKeyCredentials() {
    return apiAccessKeyCredentials;
  }

  public void setApiAccessKeyCredentials(KeystoneApiKeyCredentials apiAccessKeyCredentials) {
    this.apiAccessKeyCredentials = apiAccessKeyCredentials;
  }

  @Override
  public String toString() {
    return "KeyStoneAuthRequest as " +
            "tenant '" + tenantName + "' "
            + apiAccessKeyCredentials;
  }
}

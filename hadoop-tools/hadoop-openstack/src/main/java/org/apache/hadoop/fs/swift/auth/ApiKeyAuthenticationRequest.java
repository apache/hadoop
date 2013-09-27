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

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Class that represents authentication request to Openstack Keystone.
 * Contains basic authentication information.
 * THIS FILE IS MAPPED BY JACKSON TO AND FROM JSON.
 * DO NOT RENAME OR MODIFY FIELDS AND THEIR ACCESSORS
 */
public class ApiKeyAuthenticationRequest extends AuthenticationRequest {
  /**
   * Credentials for login
   */
  private ApiKeyCredentials apiKeyCredentials;

  /**
   * API key auth
   * @param tenantName tenant
   * @param apiKeyCredentials credentials
   */
  public ApiKeyAuthenticationRequest(String tenantName, ApiKeyCredentials apiKeyCredentials) {
    this.tenantName = tenantName;
    this.apiKeyCredentials = apiKeyCredentials;
  }

  /**
   * @return credentials for login into Keystone
   */
  @JsonProperty("RAX-KSKEY:apiKeyCredentials")
  public ApiKeyCredentials getApiKeyCredentials() {
    return apiKeyCredentials;
  }

  /**
   * @param apiKeyCredentials credentials for login into Keystone
   */
  public void setApiKeyCredentials(ApiKeyCredentials apiKeyCredentials) {
    this.apiKeyCredentials = apiKeyCredentials;
  }

  @Override
  public String toString() {
    return "Auth as " +
           "tenant '" + tenantName + "' "
           + apiKeyCredentials;
  }
}

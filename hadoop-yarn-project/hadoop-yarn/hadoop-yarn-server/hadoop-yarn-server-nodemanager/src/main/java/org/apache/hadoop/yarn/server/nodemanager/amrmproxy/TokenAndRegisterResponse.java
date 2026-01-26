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

package org.apache.hadoop.yarn.server.nodemanager.amrmproxy;

import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;

/**
 * This class contains information about the AMRM token and the RegisterApplicationMasterResponse.
 */
public class TokenAndRegisterResponse {
  private Token<AMRMTokenIdentifier> token;
  private RegisterApplicationMasterResponse response;

  public TokenAndRegisterResponse(Token<AMRMTokenIdentifier> pToken,
      RegisterApplicationMasterResponse pResponse) {
    this.token = pToken;
    this.response = pResponse;
  }

  public Token<AMRMTokenIdentifier> getToken() {
    return token;
  }

  public RegisterApplicationMasterResponse getResponse() {
    return response;
  }
}

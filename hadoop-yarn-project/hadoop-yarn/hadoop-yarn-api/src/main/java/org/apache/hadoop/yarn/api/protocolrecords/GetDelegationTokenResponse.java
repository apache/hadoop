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

package org.apache.hadoop.yarn.api.protocolrecords;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.util.Records;


/**
 * Response to a {@link GetDelegationTokenRequest} request 
 * from the client. The response contains the token that 
 * can be used by the containers to talk to  ClientRMService.
 *
 */
@Public
@Stable
public abstract class GetDelegationTokenResponse {

  @Private
  @Unstable
  public static GetDelegationTokenResponse newInstance(Token rmDTToken) {
    GetDelegationTokenResponse response =
        Records.newRecord(GetDelegationTokenResponse.class);
    response.setRMDelegationToken(rmDTToken);
    return response;
  }

  /**
   * The Delegation tokens have a identifier which maps to
   * {@link AbstractDelegationTokenIdentifier}.
   * @return the delegation tokens
   */
  @Public
  @Stable
  public abstract Token getRMDelegationToken();

  @Private
  @Unstable
  public abstract void setRMDelegationToken(Token rmDTToken);
}

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

package org.apache.hadoop.security.msgraph.oauth2;

import java.util.Date;

import org.apache.hadoop.util.Time;


/**
 * Class representing an Azure AD token used to access resources in Azure.
 */
public class AzureADToken {

  /** Access token. */
  private final String accessToken;
  /** Expiration date. */
  private final Date expiry;

  public AzureADToken(String accessToken, Date expiry) {
    this.accessToken = accessToken;
    this.expiry = expiry;
  }

  /**
   * Return the token payload.
   * @return token payload.
   */
  public String getAccessToken() {
    return accessToken;
  }

  /**
   * Return the expiry date of this token.
   * @return expiry date.
   */
  public Date getExpiry() {
    return expiry;
  }

  /**
   * Check whether the token is expired.
   * @return true if the token has expired.
   */
  public boolean isExpired() {
    return expiry != null && expiry.getTime() < Time.now();
  }

  @Override
  public String toString() {
    return "[AzureADToken, expires: " + expiry + "]";
  }
}
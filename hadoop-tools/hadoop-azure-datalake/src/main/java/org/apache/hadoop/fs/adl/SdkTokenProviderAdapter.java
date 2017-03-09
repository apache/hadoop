/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.adl;

import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;
import org.apache.hadoop.fs.adl.oauth2.AzureADTokenProvider;

import java.io.IOException;

final class SdkTokenProviderAdapter extends AccessTokenProvider {

  private AzureADTokenProvider tokenProvider;

  SdkTokenProviderAdapter(AzureADTokenProvider tp) {
    this.tokenProvider = tp;
  }

  protected AzureADToken refreshToken() throws IOException {
    AzureADToken azureADToken = new AzureADToken();
    azureADToken.accessToken = tokenProvider.getAccessToken();
    azureADToken.expiry = tokenProvider.getExpiryTime();
    return azureADToken;
  }
}

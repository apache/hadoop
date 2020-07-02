/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurebfs.constants;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Responsible to keep all the Azure Blob File System auth related
 * configurations.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class AuthConfigurations {

  /** Default OAuth token end point for the MSI flow. */
  public static final String DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_ENDPOINT =
      "http://169.254.169.254/metadata/identity/oauth2/token";
  /** Default value for authority for the MSI flow. */
  public static final String DEFAULT_FS_AZURE_ACCOUNT_OAUTH_MSI_AUTHORITY =
      "https://login.microsoftonline.com/";
  /** Default OAuth token end point for the refresh token flow. */
  public static final String
      DEFAULT_FS_AZURE_ACCOUNT_OAUTH_REFRESH_TOKEN_ENDPOINT =
      "https://login.microsoftonline.com/Common/oauth2/token";

  private AuthConfigurations() {
  }
}

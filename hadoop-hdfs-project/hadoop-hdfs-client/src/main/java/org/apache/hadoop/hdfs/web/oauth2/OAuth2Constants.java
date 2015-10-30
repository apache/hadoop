/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hdfs.web.oauth2;

import com.squareup.okhttp.MediaType;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Sundry constants relating to OAuth2 within WebHDFS.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class OAuth2Constants {
  private OAuth2Constants() { /** Private constructor. **/ }

  public static final MediaType URLENCODED
      = MediaType.parse("application/x-www-form-urlencoded; charset=utf-8");

  /* Constants for OAuth protocol */
  public static final String ACCESS_TOKEN = "access_token";
  public static final String BEARER = "bearer";
  public static final String CLIENT_CREDENTIALS = "client_credentials";
  public static final String CLIENT_ID = "client_id";
  public static final String CLIENT_SECRET = "client_secret";
  public static final String EXPIRES_IN = "expires_in";
  public static final String GRANT_TYPE = "grant_type";
  public static final String REFRESH_TOKEN = "refresh_token";
  public static final String TOKEN_TYPE = "token_type";
}

/*
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

package org.apache.hadoop.fs.s3a.auth.delegation;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.io.Text;

/**
 * All the constants related to delegation tokens.
 * Not in the normal S3 constants while unstable.
 *
 * Where possible, the existing assumed role properties are used to configure
 * STS binding, default ARN, etc. This makes documenting everything that
 * much easier and avoids trying to debug precisely which sts endpoint
 * property should be set.
 *
 * Most settings here are replicated in {@code core-default.xml}; the
 * values MUST be kept in sync.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public final class DelegationConstants {

  /**
   * Endpoint for session tokens, used when building delegation tokens:
   * {@value}.
   * @see <a href="https://docs.aws.amazon.com/general/latest/gr/rande.html#sts_region">STS regions</a>
   */
  public static final String DELEGATION_TOKEN_ENDPOINT =
      Constants.ASSUMED_ROLE_STS_ENDPOINT;

  /**
   * Default endpoint for session tokens: {@value}.
   */
  public static final String DEFAULT_DELEGATION_TOKEN_ENDPOINT =
      Constants.DEFAULT_ASSUMED_ROLE_STS_ENDPOINT;

  /**
   * Region for DT issuing; must be non-empty if the endpoint is set: {@value}.
   */
  public static final String DELEGATION_TOKEN_REGION =
      Constants.ASSUMED_ROLE_STS_ENDPOINT_REGION;

  /**
   * Region default: {@value}.
   */
  public static final String DEFAULT_DELEGATION_TOKEN_REGION =
      Constants.ASSUMED_ROLE_STS_ENDPOINT_REGION_DEFAULT;

  /**
   * Duration of tokens in time: {@value}.
   */
  public static final String DELEGATION_TOKEN_DURATION =
      Constants.ASSUMED_ROLE_SESSION_DURATION;

  /**
   * Default duration of a delegation token: {@value}.
   * Must be in the range supported by STS.
   */
  public static final String DEFAULT_DELEGATION_TOKEN_DURATION =
      Constants.ASSUMED_ROLE_SESSION_DURATION_DEFAULT;

  /**
   * Key to list AWS credential providers for Session/role
   * credentials: {@value}.
   */
  public static final String DELEGATION_TOKEN_CREDENTIALS_PROVIDER =
      Constants.AWS_CREDENTIALS_PROVIDER;

  /**
   * ARN of the delegation token: {@value}.
   * Required for the role token.
   */
  public static final String DELEGATION_TOKEN_ROLE_ARN =
      Constants.ASSUMED_ROLE_ARN;

  /**
   * Property containing classname for token binding: {@value}.
   */
  public static final String DELEGATION_TOKEN_BINDING =
      "fs.s3a.delegation.token.binding";
  /**
   * Session Token binding classname: {@value}.
   */
  public static final String DELEGATION_TOKEN_SESSION_BINDING =
      "org.apache.hadoop.fs.s3a.auth.delegation.SessionTokenBinding";

  /**
   * Default token binding {@value}.
   */
  public static final String DEFAULT_DELEGATION_TOKEN_BINDING = "";

  /**
   * Token binding to pass full credentials: {@value}.
   */
  public static final String DELEGATION_TOKEN_FULL_CREDENTIALS_BINDING =
      "org.apache.hadoop.fs.s3a.auth.delegation.FullCredentialsTokenBinding";

  /**
   * Role DTs: {@value}.
   */
  public static final String DELEGATION_TOKEN_ROLE_BINDING =
      "org.apache.hadoop.fs.s3a.auth.delegation.RoleTokenBinding";

  /** Prefix for token names: {@value}. */
  public static final String TOKEN_NAME_PREFIX = "S3ADelegationToken/";

  /** Name of session token: {@value}. */
  public static final String SESSION_TOKEN_NAME = TOKEN_NAME_PREFIX + "Session";

  /** Kind of the session token; value is {@link #SESSION_TOKEN_NAME}. */
  public static final Text SESSION_TOKEN_KIND = new Text(SESSION_TOKEN_NAME);

  /** Name of full token: {@value}. */
  public static final String FULL_TOKEN_NAME = TOKEN_NAME_PREFIX + "Full";

  /** Kind of the full token; value is {@link #FULL_TOKEN_NAME}. */
  public static final Text FULL_TOKEN_KIND = new Text(FULL_TOKEN_NAME);

  /** Name of role token: {@value}. */
  public static final String ROLE_TOKEN_NAME = TOKEN_NAME_PREFIX + "Role";

  /** Kind of the role token; value is {@link #ROLE_TOKEN_NAME}. */
  public static final Text ROLE_TOKEN_KIND = new Text(ROLE_TOKEN_NAME);

  /**
   * Package-scoped option to control level that duration info on token
   * binding operations are logged at.
   * Value: {@value}.
   */
  static final boolean DURATION_LOG_AT_INFO = true;

  /**
   * If the token binding auth chain is only session-level auth, you
   * can't use the role binding: {@value}.
   */
  public static final String E_NO_SESSION_TOKENS_FOR_ROLE_BINDING
      = "Cannot issue S3A Role Delegation Tokens without full AWS credentials";

  /**
   * The standard STS server.
   */
  public static final String STS_STANDARD = "sts.amazonaws.com";

  private DelegationConstants() {
  }
}

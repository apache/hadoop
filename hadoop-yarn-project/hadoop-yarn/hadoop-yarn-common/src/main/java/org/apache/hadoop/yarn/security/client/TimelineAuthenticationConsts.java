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

/**
 * The constants that are going to be used by the timeline Kerberos + delegation
 * token authentication.
 */

@Private
@Unstable
public class TimelineAuthenticationConsts {

  public static final String ERROR_EXCEPTION_JSON = "exception";
  public static final String ERROR_CLASSNAME_JSON = "javaClassName";
  public static final String ERROR_MESSAGE_JSON = "message";

  public static final String OP_PARAM = "op";
  public static final String DELEGATION_PARAM = "delegation";
  public static final String TOKEN_PARAM = "token";
  public static final String RENEWER_PARAM = "renewer";
  public static final String DELEGATION_TOKEN_URL = "url";
  public static final String DELEGATION_TOKEN_EXPIRATION_TIME =
      "expirationTime";
}

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

import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.DELEGATION_TOKEN_ROLE_BINDING;

/**
 * This looks at the cost of assume role, to see if it is more expensive
 * than creating simple session credentials.
 */
public class ILoadTestRoleCredentials extends ILoadTestSessionCredentials {

  @Override
  protected String getDelegationBinding() {
    return DELEGATION_TOKEN_ROLE_BINDING;
  }

  @Override
  protected String getFilePrefix() {
    return "role";
  }
}

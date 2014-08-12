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
package org.apache.hadoop.security.token.delegation.web;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation;

import javax.servlet.http.HttpServletRequest;

/**
 * Util class that returns the remote {@link UserGroupInformation} in scope
 * for the HTTP request.
 */
@InterfaceAudience.Private
public class HttpUserGroupInformation {

  /**
   * Returns the remote {@link UserGroupInformation} in context for the current
   * HTTP request, taking into account proxy user requests.
   *
   * @return the remote {@link UserGroupInformation}, <code>NULL</code> if none.
   */
  public static UserGroupInformation get() {
    return DelegationTokenAuthenticationFilter.
        getHttpUserGroupInformationInContext();
  }

}

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

package org.apache.hadoop.security.authorize;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.security.UserGroupInformation;

@InterfaceStability.Unstable
@InterfaceAudience.Public
public interface ImpersonationProvider  extends Configurable {


  /**
   * Specifies the configuration prefix for the proxy user properties and
   * initializes the provider.
   *
   * @param configurationPrefix the configuration prefix for the proxy user
   * properties
   */
  public void init(String configurationPrefix);

  /**
   * Authorize the superuser which is doing doAs
   * 
   * @param user ugi of the effective or proxy user which contains a real user
   * @param remoteAddress the ip address of client
   * @throws AuthorizationException
   */
  public void authorize(UserGroupInformation user, String remoteAddress)
      throws AuthorizationException;
}
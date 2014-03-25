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

package org.apache.hadoop.ipc;

import org.apache.hadoop.security.UserGroupInformation;

/**
 * The UserIdentityProvider creates uses the username as the
 * identity. All jobs launched by a user will be grouped together.
 */
public class UserIdentityProvider implements IdentityProvider {
  public String makeIdentity(Schedulable obj) {
    UserGroupInformation ugi = obj.getUserGroupInformation();
    if (ugi == null) {
      return null;
    }

    return ugi.getUserName();
  }
}
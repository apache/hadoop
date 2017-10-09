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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.security.YarnAuthorizationProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.webapp.dao.SchedConfUpdateInfo;

/**
 * Default configuration mutation ACL policy. Checks if user is YARN admin.
 */
public class DefaultConfigurationMutationACLPolicy implements
    ConfigurationMutationACLPolicy {

  private YarnAuthorizationProvider authorizer;

  @Override
  public void init(Configuration conf, RMContext rmContext) {
    authorizer = YarnAuthorizationProvider.getInstance(conf);
  }

  @Override
  public boolean isMutationAllowed(UserGroupInformation user,
      SchedConfUpdateInfo confUpdate) {
    return authorizer.isAdmin(user);
  }
}

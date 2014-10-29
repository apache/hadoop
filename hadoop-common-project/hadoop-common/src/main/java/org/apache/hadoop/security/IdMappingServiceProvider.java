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
package org.apache.hadoop.security;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

/**
 * An interface for the implementation of <userId, userName> mapping
 * and <groupId, groupName> mapping
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface IdMappingServiceProvider {

  // Return uid for given user name
  public int getUid(String user) throws IOException;

  // Return gid for given group name
  public int getGid(String group) throws IOException;

  // Return user name for given user id uid, if not found, return 
  // <unknown> passed to this method
  public String getUserName(int uid, String unknown);

  // Return group name for given groupd id gid, if not found, return 
  // <unknown> passed to this method
  public String getGroupName(int gid, String unknown);
  
  // Return uid for given user name.
  // When can't map user, return user name's string hashcode
  public int getUidAllowingUnknown(String user);

  // Return gid for given group name.
  // When can't map group, return group name's string hashcode
  public int getGidAllowingUnknown(String group);
}

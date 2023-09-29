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
package org.apache.hadoop.fs.azurebfs.oauth2;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.permission.AclEntry;

/**
 * {@code IdentityTransformerInterface} defines the set of translation
 * operations that any identity transformer implementation must provide.
 */
public interface IdentityTransformerInterface {

  /**
   * Perform identity transformation for the Get request.
   * @param originalIdentity the original user or group in the get request.
   * @param isUserName indicate whether the input originalIdentity is an owner name or owning group name.
   * @param localIdentity the local user or group, should be parsed from UserGroupInformation.
   * @return owner or group after transformation.
   */
  String transformIdentityForGetRequest(String originalIdentity, boolean isUserName, String localIdentity)
      throws IOException;

  /**
   * Perform Identity transformation when setting owner on a path.
   * @param userOrGroup the user or group to be set as owner.
   * @return user or group after transformation.
   */
  String transformUserOrGroupForSetRequest(String userOrGroup);

  /**
   * Perform Identity transformation when calling setAcl(),removeAclEntries() and modifyAclEntries().
   * @param aclEntries list of AclEntry.
   */
  void transformAclEntriesForSetRequest(final List<AclEntry> aclEntries);

  /**
   * Perform Identity transformation when calling GetAclStatus().
   * @param aclEntries list of AclEntry.
   * @param localUser local user name.
   * @param localGroup local primary group.
   */
  void transformAclEntriesForGetRequest(final List<AclEntry> aclEntries, String localUser, String localGroup)
      throws IOException;
}

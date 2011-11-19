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

package org.apache.hadoop.hbase.security.access;

import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;
import java.util.List;

/**
 * A custom protocol defined for maintaining and querying access control lists.
 */
public interface AccessControllerProtocol extends CoprocessorProtocol {
  /**
   * Grants the given user or group the privilege to perform the given actions
   * over the specified scope contained in {@link TablePermission}
   * @param user the user name, or, if prefixed with "@", group name receiving
   * the grant
   * @param permission the details of the provided permissions
   * @throws IOException if the grant could not be applied
   */
  public void grant(byte[] user, TablePermission permission)
      throws IOException;

  /**
   * Revokes a previously granted privilege from a user or group.
   * Note that the provided {@link TablePermission} details must exactly match
   * a stored grant.  For example, if user "bob" has been granted "READ" access
   * to table "data", over column family and qualifer "info:colA", then the
   * table, column family and column qualifier must all be specified.
   * Attempting to revoke permissions over just the "data" table will have
   * no effect.
   * @param user the user name, or, if prefixed with "@", group name whose
   * privileges are being revoked
   * @param permission the details of the previously granted permission to revoke
   * @throws IOException if the revocation could not be performed
   */
  public void revoke(byte[] user, TablePermission permission)
      throws IOException;

  /**
   * Queries the permissions currently stored for the given table, returning
   * a list of currently granted permissions, along with the user or group
   * each is associated with.
   * @param tableName the table of the permission grants to return
   * @return a list of the currently granted permissions, with associated user
   * or group names
   * @throws IOException if there is an error querying the permissions
   */
  public List<UserPermission> getUserPermissions(byte[] tableName)
      throws IOException;
}

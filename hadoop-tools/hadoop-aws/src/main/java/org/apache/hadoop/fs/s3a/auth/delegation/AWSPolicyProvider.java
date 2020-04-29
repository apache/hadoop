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

import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.s3a.auth.RoleModel;

/**
 * Interface for providers of AWS policy for accessing data.
 * This is used when building up the role permissions for a delegation
 * token.
 *
 * The permissions requested are from the perspective of
 * S3A filesystem operations on the data, <i>not</i> the simpler
 * model of "permissions on the the remote service".
 * As an example, to use S3Guard effectively, the client needs full CRUD
 * access to the table, even for {@link AccessLevel#READ}.
 */
public interface AWSPolicyProvider {

  /**
   * Get the AWS policy statements required for accessing this service.
   *
   * @param access access level desired.
   * @return a possibly empty list of statements to grant access at that
   * level.
   */
  List<RoleModel.Statement> listAWSPolicyRules(Set<AccessLevel> access);

  /**
   * Access levels.
   */
  enum AccessLevel {
    /** Filesystem data read operations. */
    READ,
    /** Data write, encryption, etc. */
    WRITE,
    /** Administration of the data, tables, etc. */
    ADMIN,
  }
}

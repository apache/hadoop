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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.AclException;

/**
 * Support for ACLs is controlled by a configuration flag.  If the configuration
 * flag is false, then the NameNode will reject all ACL-related operations.
 */
final class AclConfigFlag {
  private final boolean enabled;

  /**
   * Creates a new AclConfigFlag from configuration.
   *
   * @param conf Configuration to check
   */
  public AclConfigFlag(Configuration conf) {
    enabled = conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY,
      DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_DEFAULT);
    LogFactory.getLog(AclConfigFlag.class).info("ACLs enabled? " + enabled);
  }

  /**
   * Checks the flag on behalf of an ACL API call.
   *
   * @throws AclException if ACLs are disabled
   */
  public void checkForApiCall() throws AclException {
    if (!enabled) {
      throw new AclException(String.format(
        "The ACL operation has been rejected.  "
        + "Support for ACLs has been disabled by setting %s to false.",
        DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY));
    }
  }
}

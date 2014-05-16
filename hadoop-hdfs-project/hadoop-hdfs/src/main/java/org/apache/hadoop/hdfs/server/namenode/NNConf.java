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

import java.io.IOException;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.AclException;

import com.google.common.base.Preconditions;

/**
 * This class is a common place for NN configuration.
 */
@InterfaceAudience.Private
final class NNConf {
  /**
   * Support for ACLs is controlled by a configuration flag. If the 
   * configuration flag is false, then the NameNode will reject all 
   * ACL-related operations.
   */
  private final boolean aclsEnabled;
  
  /**
   * Support for XAttrs is controlled by a configuration flag. If the 
   * configuration flag is false, then the NameNode will reject all 
   * XAttr-related operations.
   */
  private final boolean xattrsEnabled;
  
  /**
   * Maximum size of a single name-value extended attribute.
   */
  final int xattrMaxSize;

  /**
   * Creates a new NNConf from configuration.
   *
   * @param conf Configuration to check
   */
  public NNConf(Configuration conf) {
    aclsEnabled = conf.getBoolean(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY,
      DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_DEFAULT);
    LogFactory.getLog(NNConf.class).info("ACLs enabled? " + aclsEnabled);
    xattrsEnabled = conf.getBoolean(
        DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY,
        DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_DEFAULT);
    LogFactory.getLog(NNConf.class).info("XAttrs enabled? " + xattrsEnabled);
    xattrMaxSize = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_DEFAULT);
    Preconditions.checkArgument(xattrMaxSize >= 0,
        "Cannot set a negative value for the maximum size of an xattr (%s).",
        DFSConfigKeys.DFS_NAMENODE_MAX_XATTR_SIZE_KEY);
    final String unlimited = xattrMaxSize == 0 ? " (unlimited)" : "";
    LogFactory.getLog(NNConf.class).info(
        "Maximum size of an xattr: " + xattrMaxSize + unlimited);
  }

  /**
   * Checks the flag on behalf of an ACL API call.
   *
   * @throws AclException if ACLs are disabled
   */
  public void checkAclsConfigFlag() throws AclException {
    if (!aclsEnabled) {
      throw new AclException(String.format(
        "The ACL operation has been rejected.  "
        + "Support for ACLs has been disabled by setting %s to false.",
        DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY));
    }
  }
  
  /**
   * Checks the flag on behalf of an XAttr API call.
   * @throws IOException if XAttrs are disabled
   */
  public void checkXAttrsConfigFlag() throws IOException {
    if (!xattrsEnabled) {
      throw new IOException(String.format(
        "The XAttr operation has been rejected.  "
        + "Support for XAttrs has been disabled by setting %s to false.",
        DFSConfigKeys.DFS_NAMENODE_XATTRS_ENABLED_KEY));
    }
  }
}

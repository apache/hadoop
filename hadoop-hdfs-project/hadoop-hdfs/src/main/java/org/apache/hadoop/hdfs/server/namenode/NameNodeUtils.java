/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfs.server.namenode;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSUtilClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Collection;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HA_NAMENODES_KEY_PREFIX;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_RPC_ADDRESS_KEY;

/**
 * Utility functions for the NameNode.
 */
@InterfaceAudience.Private
public final class NameNodeUtils {
  public static final Logger LOG = LoggerFactory.getLogger(NameNodeUtils.class);

  /**
   * Return the namenode address that will be used by clients to access this
   * namenode or name service. This needs to be called before the config
   * is overriden.
   *
   * This method behaves as follows:
   *
   * 1. fs.defaultFS is undefined:
   *    - return null.
   * 2. fs.defaultFS is defined but has no hostname (logical or physical):
   *    - return null.
   * 3. Single NN (no HA, no federation):
   *    - return URI authority from fs.defaultFS
   * 4. Current NN is in an HA nameservice (with or without federation):
   *    - return nameservice for current NN.
   * 5. Current NN is in non-HA namespace, federated cluster:
   *    - return value of dfs.namenode.rpc-address.[nsId].[nnId]
   *    - If the above key is not defined, then return authority from
   *      fs.defaultFS if the port number is > 0.
   * 6. If port number in the authority is missing or zero in step 6:
   *    - return null
   */
  @VisibleForTesting
  @Nullable
  static String getClientNamenodeAddress(
      Configuration conf, @Nullable String nsId) {
    final Collection<String> nameservices =
        DFSUtilClient.getNameServiceIds(conf);

    final String nnAddr = conf.get(FS_DEFAULT_NAME_KEY);
    if (nnAddr == null) {
      // default fs is not set.
      return null;
    }

    LOG.info("{} is {}", FS_DEFAULT_NAME_KEY, nnAddr);
    final URI nnUri = URI.create(nnAddr);

    String defaultNnHost = nnUri.getHost();
    if (defaultNnHost == null) {
      return null;
    }

    // Current Nameservice is HA.
    if (nsId != null && nameservices.contains(nsId)) {
      final Collection<String> namenodes = conf.getTrimmedStringCollection(
          DFS_HA_NAMENODES_KEY_PREFIX + "." + nsId);
      if (namenodes.size() > 1) {
        return nsId;
      }
    }

    // Federation without HA. We must handle the case when the current NN
    // is not in the default nameservice.
    String currentNnAddress = null;
    if (nsId != null) {
      String hostNameKey = DFS_NAMENODE_RPC_ADDRESS_KEY + "." + nsId;
      currentNnAddress = conf.get(hostNameKey);
    }

    // Fallback to the address in fs.defaultFS.
    if (currentNnAddress == null) {
      currentNnAddress = nnUri.getAuthority();
    }

    int port = 0;
    if (currentNnAddress.contains(":")) {
      port = Integer.parseInt(currentNnAddress.split(":")[1]);
    }

    if (port > 0) {
      return currentNnAddress;
    } else {
      // the port is missing or 0. Figure out real bind address later.
      return null;
    }
  }

  private NameNodeUtils() {
    // Disallow construction
  }
}

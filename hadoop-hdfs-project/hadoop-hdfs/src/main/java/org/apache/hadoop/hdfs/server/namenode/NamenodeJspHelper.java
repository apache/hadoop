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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.server.blockmanagement.DatanodeDescriptor;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocols;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;

class NamenodeJspHelper {

  static String getDelegationToken(final NamenodeProtocols nn,
      HttpServletRequest request, Configuration conf,
      final UserGroupInformation ugi) throws IOException, InterruptedException {
    Token<DelegationTokenIdentifier> token = ugi
        .doAs(new PrivilegedExceptionAction<Token<DelegationTokenIdentifier>>() {
          @Override
          public Token<DelegationTokenIdentifier> run() throws IOException {
            return nn.getDelegationToken(new Text(ugi.getUserName()));
          }
        });
    return token == null ? null : token.encodeToUrlString();
  }

  /** @return a randomly chosen datanode. */
  static DatanodeDescriptor getRandomDatanode(final NameNode namenode) {
    return (DatanodeDescriptor)namenode.getNamesystem().getBlockManager(
        ).getDatanodeManager().getNetworkTopology().chooseRandom(
        NodeBase.ROOT);
  }
}

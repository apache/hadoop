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
package org.apache.hadoop.hdfs.security.token.delegation;

//import org.apache.hadoop.classification.InterfaceAudience;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenSelector;

/**
 * A delegation token that is specialized for HDFS
 */
//@InterfaceAudience.Private
public class DelegationTokenSelector
    extends AbstractDelegationTokenSelector<DelegationTokenIdentifier>{
  private static final String SERVICE_NAME_KEY = "hdfs.service.host_";

  private static final DelegationTokenSelector INSTANCE = new DelegationTokenSelector();

  /** Select the delegation token for hdfs from the ugi. */
  public static Token<DelegationTokenIdentifier> selectHdfsDelegationToken(
      final InetSocketAddress nnAddr, final UserGroupInformation ugi,
      final Configuration conf) {
    // this guesses the remote cluster's rpc service port.
    // the current token design assumes it's the same as the local cluster's
    // rpc port unless a config key is set.  there should be a way to automatic
    // and correctly determine the value
    final String key = SERVICE_NAME_KEY + SecurityUtil.buildTokenService(nnAddr);
    final String nnServiceName = conf.get(key);
    
    int nnRpcPort = NameNode.DEFAULT_PORT;
    if (nnServiceName != null) {
      nnRpcPort = NetUtils.createSocketAddr(nnServiceName, nnRpcPort).getPort(); 
    }
    
    final Text serviceName = SecurityUtil.buildTokenService(
        NetUtils.makeSocketAddr(nnAddr.getHostName(), nnRpcPort));
    return INSTANCE.selectToken(serviceName, ugi.getTokens());
  }

  public DelegationTokenSelector() {
    super(DelegationTokenIdentifier.HDFS_DELEGATION_KIND);
  }
}

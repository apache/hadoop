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
package org.apache.hadoop.hdfs.protocolR23Compatible;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This class forwards NN's ClientProtocol calls as RPC calls to the NN server
 * while translating from the parameter types used in ClientProtocol to those
 * used in protocolR23Compatile.*.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class JournalProtocolTranslatorR23 implements
    JournalProtocol, Closeable {
  final private JournalWireProtocol rpcProxyWithoutRetry;
  final private JournalWireProtocol rpcProxy;

  private static JournalWireProtocol createNamenode(
      InetSocketAddress nameNodeAddr, Configuration conf,
      UserGroupInformation ugi) throws IOException {
    return RPC.getProxy(JournalWireProtocol.class,
        JournalWireProtocol.versionID, nameNodeAddr, ugi, conf,
        NetUtils.getSocketFactory(conf, JournalWireProtocol.class));
  }

  /** Create a {@link NameNode} proxy */
  static JournalWireProtocol createNamenodeWithRetry(
      JournalWireProtocol rpcNamenode) {
    RetryPolicy createPolicy = RetryPolicies
        .retryUpToMaximumCountWithFixedSleep(5,
            HdfsConstants.LEASE_SOFTLIMIT_PERIOD, TimeUnit.MILLISECONDS);

    Map<Class<? extends Exception>, RetryPolicy> remoteExceptionToPolicyMap = 
        new HashMap<Class<? extends Exception>, RetryPolicy>();
    remoteExceptionToPolicyMap.put(AlreadyBeingCreatedException.class,
        createPolicy);

    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap = 
        new HashMap<Class<? extends Exception>, RetryPolicy>();
    exceptionToPolicyMap.put(RemoteException.class, RetryPolicies
        .retryByRemoteException(RetryPolicies.TRY_ONCE_THEN_FAIL,
            remoteExceptionToPolicyMap));
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String, RetryPolicy> methodNameToPolicyMap = new HashMap<String, RetryPolicy>();

    methodNameToPolicyMap.put("create", methodPolicy);

    return (JournalWireProtocol) RetryProxy.create(
        JournalWireProtocol.class, rpcNamenode, methodNameToPolicyMap);
  }

  public JournalProtocolTranslatorR23(InetSocketAddress nameNodeAddr,
      Configuration conf) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    rpcProxyWithoutRetry = createNamenode(nameNodeAddr, conf, ugi);
    rpcProxy = createNamenodeWithRetry(rpcProxyWithoutRetry);
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxyWithoutRetry);
  }

  @Override
  public long getProtocolVersion(String protocolName, long clientVersion)
      throws IOException {
    return rpcProxy.getProtocolVersion(protocolName, clientVersion);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    return ProtocolSignatureWritable.convert(rpcProxy.getProtocolSignature2(
        protocol, clientVersion, clientMethodsHash));
  }

  @Override
  public void journal(NamenodeRegistration registration, long firstTxnId,
      int numTxns, byte[] records) throws IOException {
    rpcProxy.journal(NamenodeRegistrationWritable.convert(registration),
        firstTxnId, numTxns, records);
  }

  @Override
  public void startLogSegment(NamenodeRegistration registration, long txid)
      throws IOException {
    rpcProxy.startLogSegment(NamenodeRegistrationWritable.convert(registration),
        txid);
  }
}

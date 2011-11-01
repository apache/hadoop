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
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.security.token.block.ExportedBlockKeys;
import org.apache.hadoop.hdfs.server.namenode.CheckpointSignature;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.BlocksWithLocations;
import org.apache.hadoop.hdfs.server.protocol.NamenodeCommand;
import org.apache.hadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.RemoteEditLogManifest;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * This class forwards NN's NamenodeProtocol calls as RPC calls to the NN server
 * while translating from the parameter types used in NamenodeProtocol to those
 * used in protocolR23Compatile.*.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class NamenodeProtocolTranslatorR23 implements
    NamenodeProtocol, Closeable {
  final private NamenodeWireProtocol rpcProxy;

  private static NamenodeWireProtocol createNamenode(
      InetSocketAddress nameNodeAddr, Configuration conf,
      UserGroupInformation ugi) throws IOException {
    return RPC.getProxy(NamenodeWireProtocol.class,
        NamenodeWireProtocol.versionID, nameNodeAddr, ugi, conf,
        NetUtils.getSocketFactory(conf, NamenodeWireProtocol.class));
  }

  /** Create a {@link NameNode} proxy */
  static NamenodeWireProtocol createNamenodeWithRetry(
      NamenodeWireProtocol rpcNamenode) {
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

    return (NamenodeWireProtocol) RetryProxy.create(
        NamenodeWireProtocol.class, rpcNamenode, methodNameToPolicyMap);
  }

  public NamenodeProtocolTranslatorR23(InetSocketAddress nameNodeAddr,
      Configuration conf, UserGroupInformation ugi) throws IOException {
    rpcProxy = createNamenodeWithRetry(createNamenode(nameNodeAddr, conf, ugi));
  }

  public void close() {
    RPC.stopProxy(rpcProxy);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocolName,
      long clientVersion, int clientMethodHash)
      throws IOException {
    return ProtocolSignatureWritable.convert(rpcProxy.getProtocolSignature2(
        protocolName, clientVersion, clientMethodHash));
  }

  @Override
  public long getProtocolVersion(String protocolName, long clientVersion) throws IOException {
    return rpcProxy.getProtocolVersion(protocolName, clientVersion);
  }

  @Override
  public BlocksWithLocations getBlocks(DatanodeInfo datanode, long size)
      throws IOException {
    return rpcProxy.getBlocks(
        DatanodeInfoWritable.convertDatanodeInfo(datanode), size).convert();
  }

  @Override
  public ExportedBlockKeys getBlockKeys() throws IOException {
    return rpcProxy.getBlockKeys().convert();
  }

  @Override
  public long getTransactionID() throws IOException {
    return rpcProxy.getTransactionID();
  }

  @Override
  @SuppressWarnings("deprecation")
  public CheckpointSignature rollEditLog() throws IOException {
    return rpcProxy.rollEditLog().convert();
  }

  @Override
  public NamespaceInfo versionRequest() throws IOException {
    return rpcProxy.versionRequest().convert();
  }

  @Override
  public void errorReport(NamenodeRegistration registration, int errorCode,
      String msg) throws IOException {
    rpcProxy.errorReport(NamenodeRegistrationWritable.convert(registration),
        errorCode, msg);
  }

  @Override
  public NamenodeRegistration register(NamenodeRegistration registration)
      throws IOException {
    return rpcProxy
        .register(NamenodeRegistrationWritable.convert(registration)).convert();
  }

  @Override
  public NamenodeCommand startCheckpoint(NamenodeRegistration registration)
      throws IOException {
    return rpcProxy.startCheckpoint(
        NamenodeRegistrationWritable.convert(registration)).convert();
  }

  @Override
  public void endCheckpoint(NamenodeRegistration registration,
      CheckpointSignature sig) throws IOException {
    rpcProxy.endCheckpoint(NamenodeRegistrationWritable.convert(registration),
        CheckpointSignatureWritable.convert(sig));
  }

  @Override
  public RemoteEditLogManifest getEditLogManifest(long sinceTxId)
      throws IOException {
    return rpcProxy.getEditLogManifest(sinceTxId).convert();
  }
}

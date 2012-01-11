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
package org.apache.hadoop.hdfs.server.protocolR23Compatible;

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
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocolR23Compatible.DatanodeIDWritable;
import org.apache.hadoop.hdfs.protocolR23Compatible.ExtendedBlockWritable;
import org.apache.hadoop.hdfs.protocolR23Compatible.LocatedBlockWritable;
import org.apache.hadoop.hdfs.protocolR23Compatible.ProtocolSignatureWritable;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.protocol.DatanodeCommand;
import org.apache.hadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.hadoop.hdfs.server.protocol.DatanodeRegistration;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;
import org.apache.hadoop.hdfs.server.protocol.ReceivedDeletedBlockInfo;
import org.apache.hadoop.hdfs.server.protocol.UpgradeCommand;
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
public class DatanodeProtocolTranslatorR23 implements
    DatanodeProtocol, Closeable {
  final private DatanodeWireProtocol rpcProxy;

  private static DatanodeWireProtocol createNamenode(
      InetSocketAddress nameNodeAddr, Configuration conf,
      UserGroupInformation ugi) throws IOException {
    return RPC.getProxy(DatanodeWireProtocol.class,
        DatanodeWireProtocol.versionID, nameNodeAddr, ugi, conf,
        NetUtils.getSocketFactory(conf, DatanodeWireProtocol.class));
  }

  /** Create a {@link NameNode} proxy */
  static DatanodeWireProtocol createNamenodeWithRetry(
      DatanodeWireProtocol rpcNamenode) {
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

    return (DatanodeWireProtocol) RetryProxy.create(
        DatanodeWireProtocol.class, rpcNamenode, methodNameToPolicyMap);
  }

  public DatanodeProtocolTranslatorR23(InetSocketAddress nameNodeAddr,
      Configuration conf) throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    rpcProxy = createNamenodeWithRetry(createNamenode(nameNodeAddr, conf, ugi));
  }

  @Override
  public void close() {
    RPC.stopProxy(rpcProxy);
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
  public DatanodeRegistration registerDatanode(DatanodeRegistration registration)
      throws IOException {
    return rpcProxy.registerDatanode(
        DatanodeRegistrationWritable.convert(registration)).convert();
  }

  @Override
  public DatanodeCommand[] sendHeartbeat(DatanodeRegistration registration,
      long capacity, long dfsUsed, long remaining, long blockPoolUsed,
      int xmitsInProgress, int xceiverCount, int failedVolumes)
      throws IOException {
    return DatanodeCommandWritable.convert(rpcProxy.sendHeartbeat(
            DatanodeRegistrationWritable.convert(registration), capacity,
            dfsUsed, remaining, blockPoolUsed, xmitsInProgress, xceiverCount,
            failedVolumes));
  }

  @Override
  public DatanodeCommand blockReport(DatanodeRegistration registration,
      String poolId, long[] blocks) throws IOException {
    return rpcProxy.blockReport(
        DatanodeRegistrationWritable.convert(registration), poolId, blocks)
        .convert();
  }

  @Override
  public void blockReceivedAndDeleted(DatanodeRegistration registration,
      String poolId, ReceivedDeletedBlockInfo[] receivedAndDeletedBlocks)
      throws IOException {
    rpcProxy.blockReceivedAndDeleted(
        DatanodeRegistrationWritable.convert(registration), poolId,
        ReceivedDeletedBlockInfoWritable.convert(receivedAndDeletedBlocks));
  }

  @Override
  public void errorReport(DatanodeRegistration registration, int errorCode,
      String msg) throws IOException {
    rpcProxy.errorReport(DatanodeRegistrationWritable.convert(registration),
        errorCode, msg);
  }

  @Override
  public NamespaceInfo versionRequest() throws IOException {
    return rpcProxy.versionRequest().convert();
  }

  @Override
  public UpgradeCommand processUpgradeCommand(UpgradeCommand cmd)
      throws IOException {
    return rpcProxy.processUpgradeCommand(UpgradeCommandWritable.convert(cmd))
        .convert();
  }

  @Override
  public void reportBadBlocks(LocatedBlock[] blocks) throws IOException {
    rpcProxy.reportBadBlocks(LocatedBlockWritable.convertLocatedBlock(blocks));
  }

  @Override
  public void commitBlockSynchronization(ExtendedBlock block,
      long newgenerationstamp, long newlength, boolean closeFile,
      boolean deleteblock, DatanodeID[] newtargets) throws IOException {
    rpcProxy.commitBlockSynchronization(
        ExtendedBlockWritable.convertExtendedBlock(block), newgenerationstamp,
        newlength, closeFile, deleteblock,
        DatanodeIDWritable.convertDatanodeID(newtargets));
  }
}

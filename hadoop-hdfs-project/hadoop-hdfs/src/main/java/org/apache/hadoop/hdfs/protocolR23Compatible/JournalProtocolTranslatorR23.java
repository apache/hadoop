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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamenodeRegistration;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

/**
 * This class forwards NN's ClientProtocol calls as RPC calls to the NN server
 * while translating from the parameter types used in ClientProtocol to those
 * used in protocolR23Compatile.*.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class JournalProtocolTranslatorR23 implements
    JournalProtocol, Closeable {
  private final JournalWireProtocol rpcProxy;

  public JournalProtocolTranslatorR23(InetSocketAddress nameNodeAddr,
      Configuration conf) throws IOException {
    rpcProxy = RPC.getProxy(JournalWireProtocol.class,
        JournalWireProtocol.versionID, nameNodeAddr, conf);
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

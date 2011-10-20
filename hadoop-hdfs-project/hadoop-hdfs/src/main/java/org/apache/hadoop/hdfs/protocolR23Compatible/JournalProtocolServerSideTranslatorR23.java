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

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

/**
 * This class is used on the server side. Calls come across the wire for the
 * protocol family of Release 23 onwards. This class translates the R23 data
 * types to the native data types used inside the NN as specified in the generic
 * JournalProtocol.
 */
@InterfaceAudience.Private
@InterfaceStability.Stable
public class JournalProtocolServerSideTranslatorR23 implements
    JournalWireProtocol {
  final private JournalProtocol server;

  /**
   * Constructor
   * 
   * @param server - the NN server
   * @throws IOException
   */
  public JournalProtocolServerSideTranslatorR23(JournalProtocol server)
      throws IOException {
    this.server = server;
  }

  /**
   * The client side will redirect getProtocolSignature to
   * getProtocolSignature2.
   * 
   * However the RPC layer below on the Server side will call getProtocolVersion
   * and possibly in the future getProtocolSignature. Hence we still implement
   * it even though the end client's call will never reach here.
   */
  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link JournalProtocol}
     * 
     */
    if (!protocol.equals(RPC.getProtocolName(JournalWireProtocol.class))) {
      throw new IOException("Namenode Serverside implements " +
          RPC.getProtocolName(JournalWireProtocol.class) +
          ". The following requested protocol is unknown: " + protocol);
    }

    return ProtocolSignature.getProtocolSignature(clientMethodsHash,
        JournalWireProtocol.versionID, JournalWireProtocol.class);
  }

  @Override
  public ProtocolSignatureWritable 
          getProtocolSignature2(
      String protocol, long clientVersion, int clientMethodsHash)
      throws IOException {
    /**
     * Don't forward this to the server. The protocol version and signature is
     * that of {@link ClientNamenodeProtocol}
     * 
     */

    return ProtocolSignatureWritable.convert(
        this.getProtocolSignature(protocol, clientVersion, clientMethodsHash));
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion)
      throws IOException {
    if (protocol.equals(RPC.getProtocolName(JournalWireProtocol.class))) {
      return JournalWireProtocol.versionID;
    }
    throw new IOException("Namenode Serverside implements " +
        RPC.getProtocolName(JournalWireProtocol.class) +
        ". The following requested protocol is unknown: " + protocol);
  }

  @Override
  public void journal(NamenodeRegistrationWritable registration,
      long firstTxnId, int numTxns, byte[] records) throws IOException {
    server.journal(registration.convert(), firstTxnId, numTxns, records);
  }

  @Override
  public void startLogSegment(NamenodeRegistrationWritable registration,
      long txid) throws IOException {
    server.startLogSegment(registration.convert(), txid);
  }
}

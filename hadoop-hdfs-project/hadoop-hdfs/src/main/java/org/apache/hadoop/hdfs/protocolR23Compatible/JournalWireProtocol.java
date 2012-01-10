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
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.security.KerberosInfo;

/**
 * This class defines the actual protocol used to communicate with the
 * NN via RPC using writable types.
 * The parameters in the methods which are specified in the
 * package are separate from those used internally in the NN and DFSClient
 * and hence need to be converted using {@link JournalProtocolTranslatorR23}
 * and {@link JournalProtocolServerSideTranslatorR23}.
 *
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY,
    clientPrincipal = DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY)
@InterfaceAudience.Private
public interface JournalWireProtocol extends VersionedProtocol {
  
  /**
   * The  rules for changing this protocol are the same as that for
   * {@link ClientNamenodeWireProtocol} - see that java file for details.
   */
  public static final long versionID = 1L;

  /**
   * Journal edit records.
   * This message is sent by the active name-node to the backup node
   * via {@code EditLogBackupOutputStream} in order to synchronize meta-data
   * changes with the backup namespace image.
   * 
   * @param registration active node registration
   * @param firstTxnId the first transaction of this batch
   * @param numTxns number of transactions
   * @param records byte array containing serialized journal records
   */
  public void journal(NamenodeRegistrationWritable registration,
                      long firstTxnId,
                      int numTxns,
                      byte[] records) throws IOException;

  /**
   * Notify the BackupNode that the NameNode has rolled its edit logs
   * and is now writing a new log segment.
   * @param registration the registration of the active NameNode
   * @param txid the first txid in the new log
   */
  public void startLogSegment(NamenodeRegistrationWritable registration,
      long txid) throws IOException;
  
  /**
   * This method is defined to get the protocol signature using 
   * the R23 protocol - hence we have added the suffix of 2 the method name
   * to avoid conflict.
   */
  public org.apache.hadoop.hdfs.protocolR23Compatible.ProtocolSignatureWritable
           getProtocolSignature2(String protocol, long clientVersion,
      int clientMethodsHash) throws IOException;
}

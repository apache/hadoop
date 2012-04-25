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
package org.apache.hadoop.hdfs.server.protocol;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.KerberosInfo;

/**
 * Protocol used to sync journal edits. 
 * 
 */
@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_JOURNAL_USER_NAME_KEY,
    clientPrincipal = DFSConfigKeys.DFS_JOURNAL_USER_NAME_KEY)
@InterfaceAudience.Private
public interface JournalSyncProtocol {
  /**
   * 
   * This class is used by both the Namenode and Journal Service to insulate
   * from the protocol serialization.
   * 
   * If you are adding/changing journal sync protocol then you need to change both this
   * class and ALSO related protocol buffer wire protocol definition in
   * JournalSyncProtocol.proto.
   * 
   * For more details on protocol buffer wire protocol, please see
   * .../org/apache/hadoop/hdfs/protocolPB/overview.html
   */

  /**
   * Return a structure containing details about all edit logs available to be
   * fetched from the JournalNode.
   * 
   * @param journalInfo journal information 
   * @param sinceTxId return only logs containing transactions >= sinceTxI
   * @throws IOException
   */
  public RemoteEditLogManifest getEditLogManifest(JournalInfo info, long sinceTxId)
      throws IOException;
}
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

package org.apache.hadoop.hdfs.qjournal.protocol;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.qjournal.server.JournalNode;
import org.apache.hadoop.hdfs.qjournal.protocol.InterQJournalProtocolProtos.GetEditLogManifestFromJournalResponseProto;
import org.apache.hadoop.security.KerberosInfo;

import java.io.IOException;

/**
 * Protocol used to communicate between {@link JournalNode} for journalsync.
 *
 * This is responsible for sending edit log manifest.
 */

@KerberosInfo(
    serverPrincipal = DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY,
    clientPrincipal = DFSConfigKeys.DFS_JOURNALNODE_KERBEROS_PRINCIPAL_KEY)
@InterfaceAudience.Private
public interface InterQJournalProtocol {

  long versionID = 1L;

  /**
   * @param jid the journal from which to enumerate edits
   * @param sinceTxId the first transaction which the client cares about
   * @param inProgressOk whether or not to check the in-progress edit log
   *        segment
   * @return a list of edit log segments since the given transaction ID.
   */
  GetEditLogManifestFromJournalResponseProto getEditLogManifestFromJournal(
      String jid, String nameServiceId, long sinceTxId, boolean inProgressOk)
      throws IOException;

}

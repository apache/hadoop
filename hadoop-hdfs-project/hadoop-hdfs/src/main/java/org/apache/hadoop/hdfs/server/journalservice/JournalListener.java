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

package org.apache.hadoop.hdfs.server.journalservice;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.protocol.JournalProtocol;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

/**
 * JournalListener is a callback interface to handle journal records
 * received from the namenode.
 */
public interface JournalListener {
  /**
   * Check the namespace information returned by a namenode
   * @param service service that is making the callback
   * @param info returned namespace information from the namenode
   * 
   * The application using {@link JournalService} can stop the service if
   * {@code info} validation fails.
   */
  public void verifyVersion(JournalService service, NamespaceInfo info);
  
  /**
   * Process the received Journal record
   * @param service {@link JournalService} making the callback
   * @param firstTxnId first transaction Id in the journal
   * @param numTxns number of records
   * @param records journal records
   * @throws IOException on error
   * 
   * Any IOException thrown from the listener is thrown back in 
   * {@link JournalProtocol#journal}
   */
  public void journal(JournalService service, long firstTxnId, int numTxns,
      byte[] records) throws IOException;
  
  /**
   * Roll the editlog
   * @param service {@link JournalService} making the callback
   * @param txid transaction ID to roll at
   * 
   * Any IOException thrown from the listener is thrown back in 
   * {@link JournalProtocol#startLogSegment}
   */
  public void startLogSegment(JournalService service, long txid) throws IOException;
}
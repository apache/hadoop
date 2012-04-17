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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hdfs.server.protocol.NamespaceInfo;

/** A JournalListener for writing journal to an edit log. */
class JournalDiskWriter implements JournalListener {
  static final Log LOG = LogFactory.getLog(JournalDiskWriter.class);

  private final Journal journal;

  /**
   * Constructor. It is possible that the directory is not formatted. In this
   * case, it creates dummy entries and storage is later formatted.
   */
  JournalDiskWriter(Journal journal) throws IOException {
    this.journal = journal;
  }

  Journal getJournal() {
    return journal;
  }

  @Override
  public synchronized void verifyVersion(JournalService service,
      NamespaceInfo info) throws IOException {
    journal.verifyVersion(service, info);
   }

  @Override
  public synchronized void journal(JournalService service, long firstTxId,
      int numTxns, byte[] records) throws IOException {
    journal.getEditLog().journal(firstTxId, numTxns, records);
  }

  @Override
  public synchronized void startLogSegment(JournalService service, long txid
      ) throws IOException {
    journal.getEditLog().startLogSegment(txid, false);
  }
}
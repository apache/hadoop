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

package org.apache.hadoop.hdfs.inotify;

import org.apache.hadoop.classification.InterfaceAudience;

import java.util.List;

/**
 * Contains a list of event batches, the transaction ID in the edit log up to
 * which we read to produce these events, and the first txid we observed when
 * producing these events (the last of which is for the purpose of determining
 * whether we have missed events due to edit deletion). Also contains the most
 * recent txid that the NameNode has sync'ed, so the client can determine how
 * far behind in the edit log it is.
 */
@InterfaceAudience.Private
public class EventBatchList {
  private List<EventBatch> batches;
  private long firstTxid;
  private long lastTxid;
  private long syncTxid;

  public EventBatchList(List<EventBatch> batches, long firstTxid,
                         long lastTxid, long syncTxid) {
    this.batches = batches;
    this.firstTxid = firstTxid;
    this.lastTxid = lastTxid;
    this.syncTxid = syncTxid;
  }

  public List<EventBatch> getBatches() {
    return batches;
  }

  public long getFirstTxid() {
    return firstTxid;
  }

  public long getLastTxid() {
    return lastTxid;
  }

  public long getSyncTxid() {
    return syncTxid;
  }
}

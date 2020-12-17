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
package org.apache.hadoop.hdfs.qjournal.client;

import java.util.Comparator;
import java.util.Map.Entry;

import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.PrepareRecoveryResponseProto;
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.SegmentStateProto;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.thirdparty.com.google.common.collect.ComparisonChain;
import org.apache.hadoop.thirdparty.com.google.common.primitives.Booleans;

/**
 * Compares responses to the prepareRecovery RPC. This is responsible for
 * determining the correct length to recover.
 */
class SegmentRecoveryComparator
    implements Comparator<Entry<AsyncLogger, PrepareRecoveryResponseProto>> {

  static final SegmentRecoveryComparator INSTANCE = new SegmentRecoveryComparator();
  
  @Override
  public int compare(
      Entry<AsyncLogger, PrepareRecoveryResponseProto> a,
      Entry<AsyncLogger, PrepareRecoveryResponseProto> b) {
    
    PrepareRecoveryResponseProto r1 = a.getValue();
    PrepareRecoveryResponseProto r2 = b.getValue();
    
    // A response that has data for a segment is always better than one
    // that doesn't.
    if (r1.hasSegmentState() != r2.hasSegmentState()) {
      return Booleans.compare(r1.hasSegmentState(), r2.hasSegmentState());
    }
    
    if (!r1.hasSegmentState()) {
      // Neither has a segment, so neither can be used for recover.
      // Call them equal.
      return 0;
    }
    
    // They both have a segment.
    SegmentStateProto r1Seg = r1.getSegmentState();
    SegmentStateProto r2Seg = r2.getSegmentState();
    
    Preconditions.checkArgument(r1Seg.getStartTxId() == r2Seg.getStartTxId(),
        "Should only be called with responses for corresponding segments: " +
        "%s and %s do not have the same start txid.", r1, r2);

    // If one is in-progress but the other is finalized,
    // the finalized one is greater.
    if (r1Seg.getIsInProgress() != r2Seg.getIsInProgress()) {
      return Booleans.compare(!r1Seg.getIsInProgress(), !r2Seg.getIsInProgress());
    }
    
    if (!r1Seg.getIsInProgress()) {
      // If both are finalized, they should match lengths
      if (r1Seg.getEndTxId() != r2Seg.getEndTxId()) {
        throw new AssertionError("finalized segs with different lengths: " + 
            r1 + ", " + r2);
      }
      return 0;
    }
    
    // Both are in-progress.
    long r1SeenEpoch = Math.max(r1.getAcceptedInEpoch(), r1.getLastWriterEpoch());
    long r2SeenEpoch = Math.max(r2.getAcceptedInEpoch(), r2.getLastWriterEpoch());
    
    return ComparisonChain.start()
        .compare(r1SeenEpoch, r2SeenEpoch)
        .compare(r1.getSegmentState().getEndTxId(), r2.getSegmentState().getEndTxId())
        .result();
  }
}

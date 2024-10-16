/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.blockmanagement;


import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * When scheduling ReconstructionWork for a low-redundancy block, the scheduling may fail for overall 3 high-level reasons:
 * 1. No source node is available
 * 2. No Target node is available
 * 3. ReconstructionWork is built but validation failed
 * I put above 3 cases as ReconstructionSkipReason.
 * - For the detailed reason of `No source node is available`,  I put it into SourceUnavailableDetail enum
 * - For the detailed reason of `No Target node is available`,  we already has NodeNotChosenReason in BlockPlacementPolicyDefault
 */
public enum ReconstructionSkipReason {
    SOURCE_UNAVAILABLE("source node or storage unavailable"),
    TARGET_UNAVAILABLE("cannot find available target host"),
    VALIDATION_FAILED("validation for reconstruction work failed");

    enum SourceUnavailableDetail {
        CORRUPT_OR_EXCESS("stored replica state is corrupt or excess"),
        MAINTENANCE_NOT_FOR_READ("stored replica is maintenance not for read"),
        DECOMMISSIONED("replica is already decommissioned"),
        REPLICATION_SOFT_LIMIT("replica already reached replication soft limit"),
        REPLICATION_HARD_LIMIT("replica already reached replication hard limit");
        private final String text;

        SourceUnavailableDetail(final String logText) {
            text = logText;
        }

        @Override
        public String toString() {
            return text;
        }
    }

    public static final Logger LOG = LoggerFactory.getLogger(
            BlockManager.class);

    private static final ThreadLocal<HashMap<BlockInfo, StringBuilder>>
            blockNotChosenReasonMap = ThreadLocal
            .withInitial(() -> new HashMap<BlockInfo, StringBuilder>());

    private final String text;

    ReconstructionSkipReason(final String logText) {
        text = logText;
    }

    @Override
    public String toString() {
        return text;
    }

    public static void start() {
        blockNotChosenReasonMap.get().clear();
    }

    public static void genReasonWithDetail(BlockInfo block, DatanodeStorageInfo storage,
                                           ReconstructionSkipReason reason) {
        if (LOG.isDebugEnabled()) {
            genReasonImpl(block, storage, reason, null);
        }
    }

    public static void genReasonWithDetail(BlockInfo block, DatanodeStorageInfo storage,
                                           ReconstructionSkipReason reason, SourceUnavailableDetail reasonDetails) {
        if (LOG.isDebugEnabled()) {
            genReasonImpl(block, storage, reason, reasonDetails);
        }
    }

    @VisibleForTesting
    static void genReasonImpl(BlockInfo block, DatanodeStorageInfo storage,
                              ReconstructionSkipReason reason, SourceUnavailableDetail reasonDetails) {
        // build the error message for later use.
        HashMap<BlockInfo, StringBuilder> blockReason = blockNotChosenReasonMap.get();
        StringBuilder reasonForBlock = null;
        blockReason.putIfAbsent(block, new StringBuilder()
                .append("Block ")
                .append(block)
                .append(" is not scheduled for reconstruction since: ["));
        reasonForBlock = blockReason.get(block);
        reasonForBlock.append("\n").append(reason);
        if (storage != null)
            reasonForBlock.append(" on node ").append(storage);
        if (reasonDetails != null) {
            reasonForBlock.append(". Detail : [").append(reasonDetails).append("]");
        }
    }

    @VisibleForTesting
    static String summary() {
        StringBuilder finalReasonForAllBlocks = new StringBuilder();
        for (Map.Entry<BlockInfo, StringBuilder> blockReason : blockNotChosenReasonMap.get().entrySet()) {
            blockReason.getValue().append("\n]");
            finalReasonForAllBlocks.append(blockReason.getValue());
        }
        blockNotChosenReasonMap.get().clear();
        return finalReasonForAllBlocks.toString();
    }
}
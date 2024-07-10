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
package org.apache.hadoop.hdfs.server.blockmanagement;


import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * When scheduling ReconstructionWork for a low-redundancy block, the scheduling may fail for overall 3 reasons:
 * 1. No source node is available
 * 2. No Target node is available
 * 3. ReconstructionWork validation failed
 * I put above 3 cases as BlockSkippedForReconstruction.
 *  - For the detailed reason of `No source node is available`,  I put it into DetailedReason enum
 *  - For the detailed reason of `No Target node is available`,  we already has NodeNotChosenReason in BlockPlacementPolicyDefault
 */
public enum BlockSkippedForReconstructionReason {
    SOURCE_NODE_UNAVAILABLE("source node or storage unavailable"),
    NO_AVAILABLE_TARGET_HOST_FOUND("cannot find available target host"),
    RECONSTRUCTION_WORK_NOT_PASS_VALIDATION("validation for reconstruction work failed");

    enum DetailedReason{
        REPLICA_CORRUPT_OR_EXCESS("stored replica state is corrupt or excess"),
        REPLICA_MAINTENANCE_NOT_FOR_READ("stored replica is maintenance not for read"),
        REPLICA_DECOMMISSIONED("replica is already decommissioned"),
        REPLICA_ALREADY_REACH_REPLICATION_LIMIT("replica already reached replication soft limit"),
        REPLICA_ALREADY_REACH_REPLICATION_HARD_LIMIT("replica already reached replication hard limit");
        private final String text;

        DetailedReason(final String logText) {
            text = logText;
        }

        private String getText() {
            return text;
        }
    }
    public static final Logger LOG = LoggerFactory.getLogger(
            BlockManager.class);

    private static final ThreadLocal<HashMap<BlockInfo, StringBuilder>>
            blockNotChosenReasonMap = ThreadLocal
            .withInitial(() -> new HashMap<BlockInfo, StringBuilder>());

    private final String text;

    BlockSkippedForReconstructionReason(final String logText) {
        text = logText;
    }

    private String getText() {
        return text;
    }

    public static void start(){
        blockNotChosenReasonMap.get().clear();
    }

    public static void genSkipReconstructionReason(BlockInfo block, DatanodeStorageInfo storage,
                                                   BlockSkippedForReconstructionReason reason) {
        if(LOG.isDebugEnabled()){
            genStorageIsNotChooseForReplication(block, storage, reason, null);
        }
    }

    public static void genSkipReconstructionReason(BlockInfo block, DatanodeStorageInfo storage,
                                                   BlockSkippedForReconstructionReason reason, DetailedReason reasonDetails) {
        if(LOG.isDebugEnabled()){
            genStorageIsNotChooseForReplication(block, storage, reason, reasonDetails);
        }
    }

    @VisibleForTesting
    static void genStorageIsNotChooseForReplication(BlockInfo block, DatanodeStorageInfo storage,
                                                    BlockSkippedForReconstructionReason reason, DetailedReason reasonDetails){
        // build the error message for later use.
        HashMap<BlockInfo, StringBuilder> blockReason =  blockNotChosenReasonMap.get();
        StringBuilder reasonForBlock = null;
        blockReason.putIfAbsent(block, new StringBuilder()
                .append("Block ")
                .append(block)
                .append(" didn't schedule ReconstructionWork for below reasons: \n ["));
        reasonForBlock = blockReason.get(block);
        switch (reason){
            case SOURCE_NODE_UNAVAILABLE:
                reasonForBlock.append(" Source node storage ").append(storage==null?"None":storage).append(" is not chosen since ").append(reason);
                break;
            case NO_AVAILABLE_TARGET_HOST_FOUND:
            case RECONSTRUCTION_WORK_NOT_PASS_VALIDATION:
                reasonForBlock.append(" ").append(reason);
        }
        if (reasonDetails != null) {
            reasonForBlock.append(" ").append(reasonDetails.getText());
        }
        reasonForBlock.append(".");
    }

    @VisibleForTesting
    static String summaryBlockSkippedForReconstructionReason(){
        StringBuilder finalReasonForAllBlocks = new StringBuilder();
        for(Map.Entry<BlockInfo, StringBuilder> blockReason: blockNotChosenReasonMap.get().entrySet()){
            blockReason.getValue().append("]\n");
            finalReasonForAllBlocks.append(blockReason);
        }
        blockNotChosenReasonMap.get().clear();
        return finalReasonForAllBlocks.toString();
    }
}
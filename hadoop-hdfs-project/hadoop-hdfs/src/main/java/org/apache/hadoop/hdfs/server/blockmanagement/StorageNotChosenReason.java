package org.apache.hadoop.hdfs.server.blockmanagement;


import org.apache.hadoop.classification.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;


public enum StorageNotChosenReason {
    REPLICA_CORRUPT_OR_EXCESS("stored replica state is corrupt or excess"),
    REPLICA_MAINTENANCE_NOT_FOR_READ("stored replica is maintenance not for read"),
    REPLICA_DECOMMISSIONED("replica is already decommissioned"),
    REPLICA_ALREADY_REACH_REPLICATION_LIMIT("replica already reached replication soft limit"),
    REPLICA_ALREADY_REACH_REPLICATION_HARD_LIMIT("replica already reached replication hard limit");

    public static final Logger LOG = LoggerFactory.getLogger(
            BlockManager.class);

    private static final ThreadLocal<HashMap<StorageNotChosenReason, Integer>>
            REASONS_SUMMARY = ThreadLocal
            .withInitial(() -> new HashMap<StorageNotChosenReason, Integer>());


    private static final ThreadLocal<StringBuilder> debugLoggingBuilder
            = new ThreadLocal<StringBuilder>() {
        @Override
        protected StringBuilder initialValue() {
            return new StringBuilder();
        }
    };

    private final String text;

    StorageNotChosenReason(final String logText) {
        text = logText;
    }

    private String getText() {
        return text;
    }

    public static void start(){
        REASONS_SUMMARY.get().clear();
        debugLoggingBuilder.get().setLength(0);
    }

    public static void logStorageIsNotChooseForReplication(DatanodeStorageInfo storage,
                                                           StorageNotChosenReason reason) {
        if(LOG.isDebugEnabled()){
            genStorageIsNotChooseForReplication(storage, reason, null);
        }
    }

    public static void logStorageIsNotChooseForReplication(DatanodeStorageInfo storage,
                                                            StorageNotChosenReason reason, String reasonDetails) {
        if(LOG.isDebugEnabled()){
            genStorageIsNotChooseForReplication(storage, reason, reasonDetails);
        }
    }

    @VisibleForTesting
    static void genStorageIsNotChooseForReplication(DatanodeStorageInfo storage,
                                                            StorageNotChosenReason reason, String reasonDetails){
        // build the error message for later use.
        debugLoggingBuilder.get()
                .append("\n  Storage ").append((storage==null)?"None":storage)
                .append(" is not chosen since ").append(reason.getText());
        if (reasonDetails != null) {
            debugLoggingBuilder.get().append(" ").append(reasonDetails);
        }
        debugLoggingBuilder.get().append(".");
        final HashMap<StorageNotChosenReason, Integer> reasonMap =
                REASONS_SUMMARY.get();
        Integer base = reasonMap.get(reason);
        if (base == null) {
            base = 0;
        }
        reasonMap.put(reason, base + 1);
    }

    @VisibleForTesting
    static String getStorageNotChosenReason(BlockInfo block){
        StringBuilder blockInfoPrefix = new StringBuilder("Block ").append(block);
        final HashMap<StorageNotChosenReason, Integer> reasonMap =
                REASONS_SUMMARY.get();
        if(reasonMap.isEmpty()){
            return blockInfoPrefix.append(" successfully chosen storage.").toString();
        }else{
            blockInfoPrefix.append(" has no chosen storage. Reason: [\n") ;
            debugLoggingBuilder.get().append("\n]");
            StringBuilder reasonMapResult = new StringBuilder();
            reasonMapResult.append("Reason statistics: ").append(reasonMap);
            return blockInfoPrefix.append(debugLoggingBuilder.get()).append("\n")
                    .append(reasonMapResult).toString();
        }
    }
}
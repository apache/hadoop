/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.BackgroundService;
import org.apache.hadoop.utils.BackgroundTask;
import org.apache.hadoop.utils.BackgroundTaskQueue;
import org.apache.hadoop.utils.BackgroundTaskResult;
import org.apache.hadoop.utils.BackgroundTaskResult.EmptyTaskResult;
import org.apache.hadoop.utils.db.BatchOperation;
import org.apache.hadoop.utils.db.DBStore;
import org.apache.hadoop.utils.db.Table;

import com.google.common.annotations.VisibleForTesting;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the background service to delete keys. Scan the metadata of om
 * periodically to get the keys from DeletedTable and ask scm to delete
 * metadata accordingly, if scm returns success for keys, then clean up those
 * keys.
 */
public class KeyDeletingService extends BackgroundService {
  private static final Logger LOG =
      LoggerFactory.getLogger(KeyDeletingService.class);

  // The thread pool size for key deleting service.
  private final static int KEY_DELETING_CORE_POOL_SIZE = 2;

  private final ScmBlockLocationProtocol scmClient;
  private final KeyManager manager;
  private final int keyLimitPerTask;
  private final AtomicLong deletedKeyCount;
  private final AtomicLong runCount;

  public KeyDeletingService(ScmBlockLocationProtocol scmClient,
      KeyManager manager, long serviceInterval,
      long serviceTimeout, Configuration conf) {
    super("KeyDeletingService", serviceInterval, TimeUnit.MILLISECONDS,
        KEY_DELETING_CORE_POOL_SIZE, serviceTimeout);
    this.scmClient = scmClient;
    this.manager = manager;
    this.keyLimitPerTask = conf.getInt(OZONE_KEY_DELETING_LIMIT_PER_TASK,
        OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT);
    this.deletedKeyCount = new AtomicLong(0);
    this.runCount = new AtomicLong(0);
  }

  /**
   * Returns the number of times this Background service has run.
   *
   * @return Long, run count.
   */
  @VisibleForTesting
  public AtomicLong getRunCount() {
    return runCount;
  }

  /**
   * Returns the number of keys deleted by the background service.
   *
   * @return Long count.
   */
  @VisibleForTesting
  public AtomicLong getDeletedKeyCount() {
    return deletedKeyCount;
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new KeyDeletingTask());
    return queue;
  }

  /**
   * A key deleting task scans OM DB and looking for a certain number of
   * pending-deletion keys, sends these keys along with their associated blocks
   * to SCM for deletion. Once SCM confirms keys are deleted (once SCM persisted
   * the blocks info in its deletedBlockLog), it removes these keys from the
   * DB.
   */
  private class KeyDeletingTask implements
      BackgroundTask<BackgroundTaskResult> {

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      runCount.incrementAndGet();
      try {
        long startTime = Time.monotonicNow();
        List<BlockGroup> keyBlocksList = manager
            .getPendingDeletionKeys(keyLimitPerTask);
        if (keyBlocksList != null && keyBlocksList.size() > 0) {
          List<DeleteBlockGroupResult> results =
              scmClient.deleteKeyBlocks(keyBlocksList);
          if (results != null) {
            int delCount = deleteAllKeys(results);
            LOG.debug("Number of keys deleted: {}, elapsed time: {}ms",
                delCount, Time.monotonicNow() - startTime);
            deletedKeyCount.addAndGet(delCount);
          }
        }
      } catch (IOException e) {
        LOG.error("Error while running delete keys background task. Will " +
            "retry at next run.", e);
      }
      // By desing, no one cares about the results of this call back.
      return EmptyTaskResult.newResult();
    }

    /**
     * Deletes all the keys that SCM has acknowledged and queued for delete.
     *
     * @param results DeleteBlockGroups returned by SCM.
     * @throws RocksDBException on Error.
     * @throws IOException      on Error
     */
    private int deleteAllKeys(List<DeleteBlockGroupResult> results)
        throws RocksDBException, IOException {
      Table deletedTable = manager.getMetadataManager().getDeletedTable();

      DBStore store = manager.getMetadataManager().getStore();

      // Put all keys to delete in a single transaction and call for delete.
      int deletedCount = 0;
      try (BatchOperation writeBatch = store.initBatchOperation()) {
        for (DeleteBlockGroupResult result : results) {
          if (result.isSuccess()) {
            // Purge key from OM DB.
            deletedTable.deleteWithBatch(writeBatch,
                result.getObjectKey());
            LOG.debug("Key {} deleted from OM DB", result.getObjectKey());
            deletedCount++;
          }
        }
        // Write a single transaction for delete.
        store.commitBatchOperation(writeBatch);
      }
      return deletedCount;
    }
  }
}

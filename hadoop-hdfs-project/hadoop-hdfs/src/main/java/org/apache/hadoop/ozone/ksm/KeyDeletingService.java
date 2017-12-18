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
package org.apache.hadoop.ozone.ksm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.common.DeleteBlockGroupResult;
import org.apache.hadoop.ozone.common.BlockGroup;
import org.apache.hadoop.scm.protocol.ScmBlockLocationProtocol;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.utils.BackgroundService;
import org.apache.hadoop.utils.BackgroundTask;
import org.apache.hadoop.utils.BackgroundTaskQueue;
import org.apache.hadoop.utils.BackgroundTaskResult;
import org.apache.hadoop.utils.BackgroundTaskResult.EmptyTaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.ozone.ksm.KSMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK;
import static org.apache.hadoop.ozone.ksm.KSMConfigKeys.OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT;

/**
 * This is the background service to delete keys.
 * Scan the metadata of ksm periodically to get
 * the keys with prefix "#deleting" and ask scm to
 * delete metadata accordingly, if scm returns
 * success for keys, then clean up those keys.
 */
public class KeyDeletingService extends BackgroundService {

  private static final Logger LOG =
      LoggerFactory.getLogger(KeyDeletingService.class);

  // The thread pool size for key deleting service.
  private final static int KEY_DELETING_CORE_POOL_SIZE = 2;

  private final ScmBlockLocationProtocol scmClient;
  private final KeyManager manager;
  private final int keyLimitPerTask;

  public KeyDeletingService(ScmBlockLocationProtocol scmClient,
      KeyManager manager, long serviceInterval,
      long serviceTimeout, Configuration conf) {
    super("KeyDeletingService", serviceInterval, TimeUnit.MILLISECONDS,
        KEY_DELETING_CORE_POOL_SIZE, serviceTimeout);
    this.scmClient = scmClient;
    this.manager = manager;
    this.keyLimitPerTask = conf.getInt(OZONE_KEY_DELETING_LIMIT_PER_TASK,
        OZONE_KEY_DELETING_LIMIT_PER_TASK_DEFAULT);
  }

  @Override
  public BackgroundTaskQueue getTasks() {
    BackgroundTaskQueue queue = new BackgroundTaskQueue();
    queue.add(new KeyDeletingTask());
    return queue;
  }

  /**
   * A key deleting task scans KSM DB and looking for a certain number
   * of pending-deletion keys, sends these keys along with their associated
   * blocks to SCM for deletion. Once SCM confirms keys are deleted (once
   * SCM persisted the blocks info in its deletedBlockLog), it removes
   * these keys from the DB.
   */
  private class KeyDeletingTask implements
      BackgroundTask<BackgroundTaskResult> {

    @Override
    public int getPriority() {
      return 0;
    }

    @Override
    public BackgroundTaskResult call() throws Exception {
      try {
        long startTime = Time.monotonicNow();
        List<BlockGroup> keyBlocksList = manager
            .getPendingDeletionKeys(keyLimitPerTask);
        if (keyBlocksList.size() > 0) {
          LOG.info("Found {} to-delete keys in KSM", keyBlocksList.size());
          List<DeleteBlockGroupResult> results =
              scmClient.deleteKeyBlocks(keyBlocksList);
          for (DeleteBlockGroupResult result : results) {
            if (result.isSuccess()) {
              try {
                // Purge key from KSM DB.
                manager.deletePendingDeletionKey(result.getObjectKey());
                LOG.debug("Key {} deleted from KSM DB", result.getObjectKey());
              } catch (IOException e) {
                // if a pending deletion key is failed to delete,
                // print a warning here and retain it in this state,
                // so that it can be attempt to delete next time.
                LOG.warn("Failed to delete pending-deletion key {}",
                    result.getObjectKey(), e);
              }
            } else {
              // Key deletion failed, retry in next interval.
              LOG.warn("Key {} deletion failed because some of the blocks"
                  + " were failed to delete, failed blocks: {}",
                  result.getObjectKey(),
                  String.join(",", result.getFailedBlocks()));
            }
          }

          if (!results.isEmpty()) {
            LOG.info("Number of key deleted from KSM DB: {},"
                + " task elapsed time: {}ms",
                results.size(), Time.monotonicNow() - startTime);
          }

          return results::size;
        } else {
          LOG.debug("No pending deletion key found in KSM");
        }
      } catch (IOException e) {
        LOG.error("Unable to get pending deletion keys, retry in"
            + " next interval", e);
      }
      return EmptyTaskResult.newResult();
    }
  }
}

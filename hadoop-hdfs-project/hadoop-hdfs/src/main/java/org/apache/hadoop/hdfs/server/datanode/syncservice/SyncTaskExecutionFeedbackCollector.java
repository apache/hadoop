/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.syncservice;

import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.apache.hadoop.hdfs.server.protocol.BlockSyncTaskExecutionFeedback;
import org.apache.hadoop.hdfs.server.protocol.BulkSyncTaskExecutionFeedback;

import java.util.List;

/**
 * DatanodeSyncTaskExecutionFeedbackCollector collects feedback for the
 * sync service tracker to determine what has happened and report statistics.
 */
public class SyncTaskExecutionFeedbackCollector {

  private List<BlockSyncTaskExecutionFeedback> collectedFeedback;

  public SyncTaskExecutionFeedbackCollector() {
    this.collectedFeedback = Lists.newArrayList();
  }

  public void addFeedback(BlockSyncTaskExecutionFeedback feedback) {
    synchronized (this) {
      collectedFeedback.add(feedback);
    }
  }

  public BulkSyncTaskExecutionFeedback packageFeedbackForHeartbeat() {
    List<BlockSyncTaskExecutionFeedback> feedbackForHeartbeat;
    synchronized (this) {
      feedbackForHeartbeat = collectedFeedback;
      collectedFeedback = Lists.newArrayList();
    }
    return new BulkSyncTaskExecutionFeedback(feedbackForHeartbeat);
  }
}

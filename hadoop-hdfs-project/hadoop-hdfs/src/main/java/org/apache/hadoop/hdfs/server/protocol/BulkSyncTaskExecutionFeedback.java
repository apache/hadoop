/*
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
package org.apache.hadoop.hdfs.server.protocol;

import java.util.Collection;

/**
 * Feedback for a collection of {@link BlockSyncTask}s.
 */
public class BulkSyncTaskExecutionFeedback {

  private Collection<BlockSyncTaskExecutionFeedback> feedbacks;

  public BulkSyncTaskExecutionFeedback(
      Collection<BlockSyncTaskExecutionFeedback> feedbacks) {
    this.feedbacks = feedbacks;
  }

  public Collection<BlockSyncTaskExecutionFeedback> getFeedbacks() {
    return feedbacks;
  }
}

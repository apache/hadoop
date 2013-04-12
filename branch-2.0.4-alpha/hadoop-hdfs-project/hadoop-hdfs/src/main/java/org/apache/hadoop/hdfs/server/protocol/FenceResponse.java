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
package org.apache.hadoop.hdfs.server.protocol;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Response to a journal fence request. See {@link JournalProtocol#fence}
 */
@InterfaceAudience.Private
public class FenceResponse {
  private final long previousEpoch;
  private final long lastTransactionId;
  private final boolean isInSync;
  
  public FenceResponse(long previousEpoch, long lastTransId, boolean inSync) {
    this.previousEpoch = previousEpoch;
    this.lastTransactionId = lastTransId;
    this.isInSync = inSync;
  }

  public boolean isInSync() {
    return isInSync;
  }

  public long getLastTransactionId() {
    return lastTransactionId;
  }

  public long getPreviousEpoch() {
    return previousEpoch;
  }
}

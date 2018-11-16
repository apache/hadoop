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
package org.apache.hadoop.hdds.scm.block;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;

import java.util.ArrayList;
import java.util.List;

/**
 * Pending Deletes in the block space.
 */
public class PendingDeleteStatusList {

  private List<PendingDeleteStatus> pendingDeleteStatuses;
  private DatanodeDetails datanodeDetails;

  public PendingDeleteStatusList(DatanodeDetails datanodeDetails) {
    this.datanodeDetails = datanodeDetails;
    pendingDeleteStatuses = new ArrayList<>();
  }

  public void addPendingDeleteStatus(long dnDeleteTransactionId,
      long scmDeleteTransactionId, long containerId) {
    pendingDeleteStatuses.add(
        new PendingDeleteStatus(dnDeleteTransactionId, scmDeleteTransactionId,
            containerId));
  }

  /**
   * Status of pending deletes.
   */
  public static class PendingDeleteStatus {
    private long dnDeleteTransactionId;
    private long scmDeleteTransactionId;
    private long containerId;

    public PendingDeleteStatus(long dnDeleteTransactionId,
        long scmDeleteTransactionId, long containerId) {
      this.dnDeleteTransactionId = dnDeleteTransactionId;
      this.scmDeleteTransactionId = scmDeleteTransactionId;
      this.containerId = containerId;
    }

    public long getDnDeleteTransactionId() {
      return dnDeleteTransactionId;
    }

    public long getScmDeleteTransactionId() {
      return scmDeleteTransactionId;
    }

    public long getContainerId() {
      return containerId;
    }

  }

  public List<PendingDeleteStatus> getPendingDeleteStatuses() {
    return pendingDeleteStatuses;
  }

  public int getNumPendingDeletes() {
    return pendingDeleteStatuses.size();
  }

  public DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }
}

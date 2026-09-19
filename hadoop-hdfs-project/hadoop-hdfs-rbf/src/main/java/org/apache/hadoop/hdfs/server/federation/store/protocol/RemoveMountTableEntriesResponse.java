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
package org.apache.hadoop.hdfs.server.federation.store.protocol;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreSerializer;

/**
 * API response for removing multiple mount table paths in the state store.
 */
public abstract class RemoveMountTableEntriesResponse {

  public static RemoveMountTableEntriesResponse newInstance() throws IOException {
    return StateStoreSerializer.newRecord(RemoveMountTableEntriesResponse.class);
  }

  @Public
  @Unstable
  public abstract boolean getStatus();

  @Public
  @Unstable
  public abstract List<EntryFailure> getFailedEntries();

  @Public
  @Unstable
  public abstract void setStatus(boolean result);

  @Public
  @Unstable
  public abstract void setFailedEntries(List<EntryFailure> failedEntries);

  /**
   * A mount table that failed to be removed, as well as the reason why.
   */
  @Public
  @Unstable
  public static class EntryFailure {
    private final String srcPath;
    private final FailureReason reason;

    public EntryFailure(String srcPath,
        FailureReason reason) {
      this.srcPath = srcPath;
      this.reason = reason;
    }

    public String getSrcPath() {
      return srcPath;
    }

    public FailureReason getReason() {
      return reason;
    }
  }

  /**
   * Reason a mount table path failed to be removed.
   */
  @Public
  @Unstable
  public enum FailureReason {
    UNKNOWN_FAILURE,
    NONEXISTENT_MOUNT_POINT,
    DRIVER_FAILURE,
    ACCESS_DENIED
  }
}
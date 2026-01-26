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

package org.apache.hadoop.fs.azurebfs.services;

/**
 * A class to store the Result of an AbfsClient rename operation, signifying the
 * AbfsRestOperation result and the rename recovery.
 */
public class AbfsClientRenameResult {

  /** Abfs Rest Operation. */
  private final AbfsRestOperation op;
  /** Flag indicating recovery took place. */
  private final boolean renameRecovered;
  /** Abfs storage tracking metadata is in an incomplete state. */
  private final boolean isIncompleteMetadataState;

  /**
   * Constructing an ABFS rename operation result.
   * @param op The AbfsRestOperation.
   * @param renameRecovered Did rename recovery took place?
   * @param isIncompleteMetadataState Did the rename failed due to incomplete
   *                                 metadata state and had to be retried?
   */
  public AbfsClientRenameResult(
      AbfsRestOperation op,
      boolean renameRecovered,
      boolean isIncompleteMetadataState) {
    this.op = op;
    this.renameRecovered = renameRecovered;
    this.isIncompleteMetadataState = isIncompleteMetadataState;
  }

  public AbfsRestOperation getOp() {
    return op;
  }

  public boolean isRenameRecovered() {
    return renameRecovered;
  }

  public boolean isIncompleteMetadataState() {
    return isIncompleteMetadataState;
  }

  @Override
  public String toString() {
    return "AbfsClientRenameResult{"
            + "op="
            + op
            + ", renameRecovered="
            + renameRecovered
            + ", isIncompleteMetadataState="
            + isIncompleteMetadataState
            + '}';
  }
}

/*
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

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.Closeable;
import java.io.IOException;

/**
 * This represents state which may be passed to bulk IO operations
 * to enable them to store information about the state of the ongoing
 * operation across invocations.
 * <p>
 * A bulk operation state <i>MUST</i> only be be used for the single store
 * from which it was created, and <i>MUST</i>only for the duration of a single
 * bulk update operation.
 * <p>
 * Passing in the state is to allow the stores to maintain state about
 * updates they have already made to their store during this single operation:
 * a cache of what has happened. It is not a list of operations to be applied.
 * If a list of operations to perform is built up (e.g. during rename)
 * that is the duty of the caller, not this state.
 * <p>
 * After the operation has completed, it <i>MUST</i> be closed so
 * as to guarantee that all state is released.
 */
public class BulkOperationState implements Closeable {

  private final OperationType operation;

  /**
   * Constructor.
   * @param operation the type of the operation.
   */
  public BulkOperationState(final OperationType operation) {
    this.operation = operation;
  }

  /**
   * Get the operation type.
   * @return the operation type.
   */
  public OperationType getOperation() {
    return operation;
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * Enumeration of operations which can be performed in bulk.
   * This can be used by the stores however they want.
   * One special aspect: renames are to be done through a {@link RenameTracker}.
   * Callers will be blocked from initiating a rename through
   * {@code S3Guard#initiateBulkWrite()}
   */
  public enum OperationType {
    /** Writing data. */
    Put,
    /**
     * Rename: add and delete.
     * After the rename, the tree under the destination path
     * can be tagged as authoritative.
     */
    Rename,
    /** Pruning: deleting entries and updating parents. */
    Prune,
    /** Commit operation. */
    Commit,
    /** Deletion operation. */
    Delete,
    /** FSCK operation. */
    Fsck,
    /**
     * Bulk directory tree import.
     * After an import, the entire tree under the path has been
     * enumerated and should be tagged as authoritative.
     */
    Import,
    /**
     * Listing update.
     */
    Listing,
    /**
     * Mkdir operation.
     */
    Mkdir,
    /**
     * Multipart upload operation.
     */
    Upload
  }
}

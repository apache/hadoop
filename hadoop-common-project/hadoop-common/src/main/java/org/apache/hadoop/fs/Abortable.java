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

package org.apache.hadoop.fs;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 *  Abort data being written to  a stream, so that close() does
 *  not write the data. It is implemented by output streams in
 *  some object stores, and passed through {@link FSDataOutputStream}.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public interface Abortable {

  /**
   * Abort the active operation without the output becoming visible.
   *
   * This is to provide ability to cancel the write on stream; once
   * a stream is aborted, the write MUST NOT become visible.
   *
   * @throws UnsupportedOperationException if the operation is not supported.
   * @return the result.
   */
  AbortableResult abort();

  /**
   * Interface for the result of aborts; allows subclasses to extend
   * (IOStatistics etc) or for future enhancements if ever needed.
   */
  interface AbortableResult {

    /**
     * Was the stream already closed/aborted?
     * @return true if a close/abort operation had already
     * taken place.
     */
    boolean alreadyClosed();

    /**
     * Any exception caught during cleanup operations,
     * exceptions whose raising/catching does not change
     * the semantics of the abort.
     * @return an exception or null.
     */
    IOException anyCleanupException();
  }
}

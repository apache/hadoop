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

package org.apache.hadoop.hdfs;

import org.apache.hadoop.classification.InterfaceAudience;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

/**
 * The exception last seen by the {@link DataStreamer} or
 * {@link DFSOutputStream}.
 */
@InterfaceAudience.Private
class ExceptionLastSeen {
  private IOException thrown;

  /** Get the last seen exception. */
  synchronized protected IOException get() {
    return thrown;
  }

  /**
   * Set the last seen exception.
   * @param t the exception.
   */
  synchronized void set(Throwable t) {
    assert t != null;
    this.thrown = t instanceof IOException ?
        (IOException) t : new IOException(t);
  }

  /** Clear the last seen exception. */
  synchronized void clear() {
    thrown = null;
  }

  /**
   * Check if there already is an exception. Throw the exception if exist.
   *
   * @param resetToNull set to true to reset exception to null after calling
   *                    this function.
   * @throws IOException on existing IOException.
   */
  synchronized void check(boolean resetToNull) throws IOException {
    if (thrown != null) {
      final IOException e = thrown;
      if (resetToNull) {
        thrown = null;
      }
      throw e;
    }
  }

  synchronized void throwException4Close() throws IOException {
    check(false);
    throw new ClosedChannelException();
  }
}

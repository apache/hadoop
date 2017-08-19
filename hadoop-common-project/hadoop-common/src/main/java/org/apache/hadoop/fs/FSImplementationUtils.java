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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import static org.apache.hadoop.fs.FSExceptionMessages.*;

/**
 * Utility classes to help implementing filesystems and streams.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class FSImplementationUtils {


  /**
   * Class to manage close() logic.
   * A simple wrapper around an atomic boolean to guard against
   * calling operations when closed; {@link #checkOpen()}
   * will throw an exception when closed ... it should be
   * used in methods which require the stream/filesystem to be
   * open.
   */
  public static class CloseChecker {
    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public CloseChecker() {
    }

    /**
     *
     * @return true if the close() call can continue; false
     * if the state has been reached.
     */
    public boolean enterClose() {
      return closed.compareAndSet(false, true);
    }

    /**
     * Check for the stream being open.
     * @throws IOException if the stream is closed.
     */
    public void checkOpen() throws IOException {
      if (isClosed()) {
        throw new IOException(STREAM_IS_CLOSED);
      }
    }

    /**
     * Is the stream closed?
     * @return true if the stream is closed.
     */
    public boolean isClosed() {
      return closed.get();
    }

    public boolean isOpen() {
      return !isClosed();
    }


  }
}

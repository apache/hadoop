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

package org.apache.hadoop.fs.store;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;

import static org.apache.hadoop.fs.FSExceptionMessages.*;
import static org.apache.hadoop.fs.StreamCapabilities.StreamCapability.*;

/**
 * Utility classes to help implementing filesystems and streams.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class StoreImplementationUtils {

  private StoreImplementationUtils() {
  }

  /**
   * Check the supplied capabilities for being those required for full
   * {@code Syncable.hsync()} and {@code Syncable.hflush()} functionality.
   * @param capability capability string.
   * @return true if either refers to one of the Syncable operations.
   */
  public static boolean supportsSyncable(String capability) {
    return capability.equalsIgnoreCase(HSYNC.getValue()) ||
        capability.equalsIgnoreCase((HFLUSH.getValue()));
  }

  /**
   * Class to manage {@code close()} logic.
   * A simple wrapper around an atomic boolean to guard against
   * calling operations when closed; {@link #checkOpen()}
   * will throw an exception when closed ... it should be
   * used in methods which require the stream/filesystem to be
   * open.
   *
   * The {@link #enterClose()} call can be used to ensure that
   * a stream is closed at most once.
   * It should be the first operation in the {@code close} method,
   * with the caller exiting immediately if the stream is already closed.
   * <pre>
   * public void close() throws IOException {
   *   if (!closed.enterClose()) {
   *     return;
   *   }
   *   ... close operations
   * }
   * </pre>
   */
  public static class CloseChecker {

    /** Closed flag. */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Path; if not empty then a {@link PathIOException} will be raised
     * containing this path.
     */
    private final String path;

    /**
     * Constructor.
     * @param path path to use in exception text.
     */
    public CloseChecker(String path) {
      this.path = path;
    }

    /**
     * Instantiate.
     * @param path path to use in exception messages.
     */
    public CloseChecker(Path path) {
      this.path = path.toUri().toString();
    }

    /**
     * Constructor without a path.
     */
    public CloseChecker() {
      this("");
    }

    /**
     * Enter the close call, non-reentrantly
     * @return true if the close() call can continue; false
     * if the state has been reached.
     */
    public boolean enterClose() {
      return closed.compareAndSet(false, true);
    }

    /**
     * Check for the stream being open, throwing an
     * exception if it is not.
     * @throws IOException if the stream is closed.
     * @throws PathIOException if the stream is closed and this checker
     * was constructed with a path.
     */
    public void checkOpen() throws IOException {
      if (isClosed()) {
        if (StringUtils.isNotEmpty(path)) {
          throw new PathIOException(path, STREAM_IS_CLOSED);
        } else {
          throw new IOException(STREAM_IS_CLOSED);
        }
      }
    }

    /**
     * Is the stream closed?
     * @return true if the stream is closed.
     */
    public boolean isClosed() {
      return closed.get();
    }

  }
}

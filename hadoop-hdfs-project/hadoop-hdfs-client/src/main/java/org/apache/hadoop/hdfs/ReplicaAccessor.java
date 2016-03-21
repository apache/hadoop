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
import org.apache.hadoop.classification.InterfaceStability;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The public API for ReplicaAccessor objects.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class ReplicaAccessor {
  /**
   * Read bytes from the replica.
   *
   * @param pos    The position in the replica to start reading at.
   *                 Must not be negative.
   * @param buf    The byte array to read into.
   * @param off    The offset within buf to start reading into.
   * @param len    The maximum length to read.
   *
   * @return       The number of bytes read.  If the read extends past the end
   *                  of the replica, a short read count will be returned.  We
   *                  will should return -1 if EOF is reached and no bytes
   *                  can be returned.  We will never return a short read
   *                  count unless EOF is reached.
   */
  public abstract int read(long pos, byte[] buf, int off, int len)
      throws IOException;

  /**
   * Read bytes from the replica.
   *
   * @param pos    The position in the replica to start reading at.
   *                 Must not be negative.
   * @param buf    The byte buffer to read into.  The amount to read will be
   *                 dictated by the remaining bytes between the current
   *                 position and the limit.  The ByteBuffer may or may not be
   *                 direct.
   *
   * @return       The number of bytes read.  If the read extends past the end
   *                 of the replica, a short read count will be returned.  We
   *                 should return -1 if EOF is reached and no bytes can be
   *                 returned.  We will never return a short read count unless
   *                 EOF is reached.
   */
  public abstract int read(long pos, ByteBuffer buf) throws IOException;

  /**
   * Release the resources associated with the ReplicaAccessor.
   *
   * It is recommended that implementations never throw an IOException.  The
   * method is declared as throwing IOException in order to remain compatible
   * with java.io.Closeable.  If an exception is thrown, the ReplicaAccessor
   * must still be closed when the function returns in order to prevent a
   * resource leak.
   */
  public abstract void close() throws IOException;

  /**
   * Return true if bytes read via this accessor should count towards the
   * local byte count statistics.
   */
  public abstract boolean isLocal();

  /**
   * Return true if bytes read via this accessor should count towards the
   * short-circuit byte count statistics.
   */
  public abstract boolean isShortCircuit();

  /**
   * Return the network distance between local machine and the remote machine.
   */
  public int getNetworkDistance() {
    return isLocal() ? 0 : Integer.MAX_VALUE;
  }
}

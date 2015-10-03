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
package org.apache.hadoop.hdfs.client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.crypto.CryptoOutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DFSOutputStream;

import com.google.common.base.Preconditions;

/**
 * The Hdfs implementation of {@link FSDataOutputStream}.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HdfsDataOutputStream extends FSDataOutputStream {
  public HdfsDataOutputStream(DFSOutputStream out, FileSystem.Statistics stats,
      long startPosition) throws IOException {
    super(out, stats, startPosition);
  }

  public HdfsDataOutputStream(DFSOutputStream out, FileSystem.Statistics stats)
      throws IOException {
    this(out, stats, 0L);
  }

  public HdfsDataOutputStream(CryptoOutputStream out,
      FileSystem.Statistics stats, long startPosition) throws IOException {
    super(out, stats, startPosition);
    Preconditions.checkArgument(
        out.getWrappedStream() instanceof DFSOutputStream,
        "CryptoOutputStream should wrap a DFSOutputStream");
  }

  public HdfsDataOutputStream(CryptoOutputStream out,
      FileSystem.Statistics stats) throws IOException {
    this(out, stats, 0L);
  }

  /**
   * Get the actual number of replicas of the current block.
   *
   * This can be different from the designated replication factor of the file
   * because the namenode does not maintain replication for the blocks which are
   * currently being written to. Depending on the configuration, the client may
   * continue to write to a block even if a few datanodes in the write pipeline
   * have failed, or the client may add a new datanodes once a datanode has
   * failed.
   *
   * @return the number of valid replicas of the current block
   */
  public synchronized int getCurrentBlockReplication() throws IOException {
    OutputStream wrappedStream = getWrappedStream();
    if (wrappedStream instanceof CryptoOutputStream) {
      wrappedStream = ((CryptoOutputStream) wrappedStream).getWrappedStream();
    }
    return ((DFSOutputStream) wrappedStream).getCurrentBlockReplication();
  }

  /**
   * Sync buffered data to DataNodes (flush to disk devices).
   *
   * @param syncFlags
   *          Indicate the detailed semantic and actions of the hsync.
   * @throws IOException
   * @see FSDataOutputStream#hsync()
   */
  public void hsync(EnumSet<SyncFlag> syncFlags) throws IOException {
    OutputStream wrappedStream = getWrappedStream();
    if (wrappedStream instanceof CryptoOutputStream) {
      wrappedStream.flush();
      wrappedStream = ((CryptoOutputStream) wrappedStream).getWrappedStream();
    }
    ((DFSOutputStream) wrappedStream).hsync(syncFlags);
  }

  public enum SyncFlag {

    /**
     * When doing sync to DataNodes, also update the metadata (block length) in
     * the NameNode.
     */
    UPDATE_LENGTH,

    /**
     * Sync the data to DataNode, close the current block, and allocate a new
     * block
     */
    END_BLOCK
  }
}

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
package org.apache.hadoop.hdfs.server.datanode.fsdataset;

import java.io.Closeable;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.IOException;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.DataChecksum;

/**
 * Contains the output streams for the data and checksum of a replica.
 */
public class ReplicaOutputStreams implements Closeable {
  private final OutputStream dataOut;
  private final OutputStream checksumOut;
  private final DataChecksum checksum;
  private final boolean isTransientStorage;

  /**
   * Create an object with a data output stream, a checksum output stream
   * and a checksum.
   */
  public ReplicaOutputStreams(OutputStream dataOut, OutputStream checksumOut,
      DataChecksum checksum, boolean isTransientStorage) {
    this.dataOut = dataOut;
    this.checksumOut = checksumOut;
    this.checksum = checksum;
    this.isTransientStorage = isTransientStorage;
  }

  /** @return the data output stream. */
  public OutputStream getDataOut() {
    return dataOut;
  }

  /** @return the checksum output stream. */
  public OutputStream getChecksumOut() {
    return checksumOut;
  }

  /** @return the checksum. */
  public DataChecksum getChecksum() {
    return checksum;
  }

  /** @return is writing to a transient storage? */
  public boolean isTransientStorage() {
    return isTransientStorage;
  }

  @Override
  public void close() {
    IOUtils.closeStream(dataOut);
    IOUtils.closeStream(checksumOut);
  }

  /**
   * Sync the data stream if it supports it.
   */
  public void syncDataOut() throws IOException {
    if (dataOut instanceof FileOutputStream) {
      ((FileOutputStream)dataOut).getChannel().force(true);
    }
  }
  
  /**
   * Sync the checksum stream if it supports it.
   */
  public void syncChecksumOut() throws IOException {
    if (checksumOut instanceof FileOutputStream) {
      ((FileOutputStream)checksumOut).getChannel().force(true);
    }
  }

}

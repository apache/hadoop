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
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.IOException;

import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.datanode.FileIoProvider;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIOException;
import org.apache.hadoop.util.DataChecksum;
import org.slf4j.Logger;

/**
 * Contains the output streams for the data and checksum of a replica.
 */
public class ReplicaOutputStreams implements Closeable {
  public static final Logger LOG = DataNode.LOG;

  private FileDescriptor outFd = null;
  /** Stream to block. */
  private OutputStream dataOut;
  /** Stream to checksum. */
  private final OutputStream checksumOut;
  private final DataChecksum checksum;
  private final FsVolumeSpi volume;
  private final FileIoProvider fileIoProvider;

  /**
   * Create an object with a data output stream, a checksum output stream
   * and a checksum.
   */
  public ReplicaOutputStreams(
      OutputStream dataOut, OutputStream checksumOut, DataChecksum checksum,
      FsVolumeSpi volume, FileIoProvider fileIoProvider) {

    this.dataOut = dataOut;
    this.checksum = checksum;
    this.checksumOut = checksumOut;
    this.volume = volume;
    this.fileIoProvider = fileIoProvider;

    try {
      if (this.dataOut instanceof FileOutputStream) {
        this.outFd = ((FileOutputStream)this.dataOut).getFD();
      } else {
        LOG.debug("Could not get file descriptor for outputstream of class " +
            this.dataOut.getClass());
      }
    } catch (IOException e) {
      LOG.warn("Could not get file descriptor for outputstream of class " +
          this.dataOut.getClass());
    }
  }

  public FileDescriptor getOutFd() {
    return outFd;
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
    return volume.isTransientStorage();
  }

  @Override
  public void close() {
    IOUtils.closeStream(dataOut);
    IOUtils.closeStream(checksumOut);
  }

  public void closeDataStream() throws IOException {
    dataOut.close();
    dataOut = null;
  }

  /**
   * Sync the data stream if it supports it.
   */
  public void syncDataOut() throws IOException {
    if (dataOut instanceof FileOutputStream) {
      fileIoProvider.sync(volume, (FileOutputStream) dataOut);
    }
  }
  
  /**
   * Sync the checksum stream if it supports it.
   */
  public void syncChecksumOut() throws IOException {
    if (checksumOut instanceof FileOutputStream) {
      fileIoProvider.sync(volume, (FileOutputStream) checksumOut);
    }
  }

  /**
   * Flush the data stream if it supports it.
   */
  public void flushDataOut() throws IOException {
    if (dataOut != null) {
      fileIoProvider.flush(volume, dataOut);
    }
  }

  /**
   * Flush the checksum stream if it supports it.
   */
  public void flushChecksumOut() throws IOException {
    if (checksumOut != null) {
      fileIoProvider.flush(volume, checksumOut);
    }
  }

  public void writeDataToDisk(byte[] b, int off, int len)
      throws IOException {
    dataOut.write(b, off, len);
  }

  public void syncFileRangeIfPossible(long offset, long nbytes,
      int flags) throws NativeIOException {
    fileIoProvider.syncFileRange(
        volume, outFd, offset, nbytes, flags);
  }

  public void dropCacheBehindWrites(String identifier,
      long offset, long len, int flags) throws NativeIOException {
    fileIoProvider.posixFadvise(
        volume, identifier, outFd, offset, len, flags);
  }
}

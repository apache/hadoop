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
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.io.nativeio.NativeIOException;
import org.apache.hadoop.util.DataChecksum;
import org.apache.hadoop.util.Time;
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
  private final boolean isTransientStorage;
  private final long slowLogThresholdMs;

  /**
   * Create an object with a data output stream, a checksum output stream
   * and a checksum.
   */
  public ReplicaOutputStreams(OutputStream dataOut,
      OutputStream checksumOut, DataChecksum checksum,
      boolean isTransientStorage, long slowLogThresholdMs) {
    this.dataOut = dataOut;
    this.checksum = checksum;
    this.slowLogThresholdMs = slowLogThresholdMs;
    this.isTransientStorage = isTransientStorage;
    this.checksumOut = checksumOut;

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
    return isTransientStorage;
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
      sync((FileOutputStream)dataOut);
    }
  }
  
  /**
   * Sync the checksum stream if it supports it.
   */
  public void syncChecksumOut() throws IOException {
    if (checksumOut instanceof FileOutputStream) {
      sync((FileOutputStream)checksumOut);
    }
  }

  /**
   * Flush the data stream if it supports it.
   */
  public void flushDataOut() throws IOException {
    flush(dataOut);
  }

  /**
   * Flush the checksum stream if it supports it.
   */
  public void flushChecksumOut() throws IOException {
    flush(checksumOut);
  }

  private void flush(OutputStream dos) throws IOException {
    long begin = Time.monotonicNow();
    dos.flush();
    long duration = Time.monotonicNow() - begin;
    LOG.trace("ReplicaOutputStreams#flush takes {} ms.", duration);
    if (duration > slowLogThresholdMs) {
      LOG.warn("Slow flush took {} ms (threshold={} ms)", duration,
          slowLogThresholdMs);
    }
  }

  private void sync(FileOutputStream fos) throws IOException {
    long begin = Time.monotonicNow();
    fos.getChannel().force(true);
    long duration = Time.monotonicNow() - begin;
    LOG.trace("ReplicaOutputStreams#sync takes {} ms.", duration);
    if (duration > slowLogThresholdMs) {
      LOG.warn("Slow fsync took {} ms (threshold={} ms)", duration,
          slowLogThresholdMs);
    }
  }

  public long writeToDisk(byte[] b, int off, int len) throws IOException {
    long begin = Time.monotonicNow();
    dataOut.write(b, off, len);
    long duration = Time.monotonicNow() - begin;
    LOG.trace("DatanodeIO#writeToDisk takes {} ms.", duration);
    if (duration > slowLogThresholdMs) {
      LOG.warn("Slow BlockReceiver write data to disk cost: {} ms " +
          "(threshold={} ms)", duration, slowLogThresholdMs);
    }
    return duration;
  }

  public void syncFileRangeIfPossible(long offset, long nbytes,
      int flags) throws NativeIOException {
    assert this.outFd != null : "null outFd!";
    NativeIO.POSIX.syncFileRangeIfPossible(outFd, offset, nbytes, flags);
  }

  public void dropCacheBehindWrites(String identifier,
      long offset, long len, int flags) throws NativeIOException {
    assert this.outFd != null : "null outFd!";
    NativeIO.POSIX.getCacheManipulator().posixFadviseIfPossible(
        identifier, outFd, offset, len, flags);
  }
}

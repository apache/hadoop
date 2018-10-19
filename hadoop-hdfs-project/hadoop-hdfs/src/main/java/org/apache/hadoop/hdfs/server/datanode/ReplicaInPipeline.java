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
package org.apache.hadoop.hdfs.server.datanode;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hdfs.server.datanode.fsdataset.ReplicaOutputStreams;
import org.apache.hadoop.util.DataChecksum;

/** 
 * This defines the interface of a replica in Pipeline that's being written to
 */
public interface ReplicaInPipeline extends Replica {
  /**
   * Set the number of bytes received
   * @param bytesReceived number of bytes received
   */
  void setNumBytes(long bytesReceived);

  /**
   * Get the number of bytes acked
   * @return the number of bytes acked
   */
  long getBytesAcked();

  /**
   * Set the number bytes that have acked
   * @param bytesAcked number bytes acked
   */
  void setBytesAcked(long bytesAcked);

  /**
   * Release any disk space reserved for this replica.
   */
  public void releaseAllBytesReserved();

  /**
   * store the checksum for the last chunk along with the data length
   * @param dataLength number of bytes on disk
   * @param lastChecksum - checksum bytes for the last chunk
   */
  public void setLastChecksumAndDataLen(long dataLength, byte[] lastChecksum);
  
  /**
   * gets the last chunk checksum and the length of the block corresponding
   * to that checksum
   */
  public ChunkChecksum getLastChecksumAndDataLen();
  
  /**
   * Create output streams for writing to this replica,
   * one for block file and one for CRC file
   *
   * @param isCreate if it is for creation
   * @param requestedChecksum the checksum the writer would prefer to use
   * @return output streams for writing
   * @throws IOException if any error occurs
   */
  public ReplicaOutputStreams createStreams(boolean isCreate,
      DataChecksum requestedChecksum) throws IOException;

  /**
   * Create an output stream to write restart metadata in case of datanode
   * shutting down for quick restart.
   *
   * @return output stream for writing.
   * @throws IOException if any error occurs
   */
  public OutputStream createRestartMetaStream() throws IOException;
  
  ReplicaInfo getReplicaInfo();
  
  /**
   * Set the thread that is writing to this replica
   * @param writer a thread writing to this replica
   */
  void setWriter(Thread writer);
  
  void interruptThread();
  
  /**
   * Attempt to set the writer to a new value.
   */
  boolean attemptToSetWriter(Thread prevWriter, Thread newWriter);

  /**
   * Interrupt the writing thread and wait until it dies.
   * @throws IOException the waiting is interrupted
   */
  void stopWriter(long xceiverStopTimeout) throws IOException;

  /**
   * Causes the current thread to wait until a minimum length is reached, the
   * thread is interrupted, or the specified waiting time elapses.
   *
   * @param minLength The minimum length to achieve
   * @param time the maximum time to wait
   * @param unit the time unit of the time argument
   * @throws IOException if the current thread is interrupted or the minimum
   *           length is not achieved within the time allowed.
   */
  void waitForMinLength(long minLength, long time, TimeUnit unit)
      throws IOException;
}

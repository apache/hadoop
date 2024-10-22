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

import org.apache.hadoop.classification.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.protocol.DatanodeID;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Used for injecting faults in DFSClient and DFSOutputStream tests.
 * Calls into this are a no-op in production code. 
 */
@VisibleForTesting
@InterfaceAudience.Private
public class DataNodeFaultInjector {
  private static DataNodeFaultInjector instance = new DataNodeFaultInjector();

  public static DataNodeFaultInjector get() {
    return instance;
  }

  public static void set(DataNodeFaultInjector injector) {
    instance = injector;
  }

  public void getHdfsBlocksMetadata() {}

  public void writeBlockAfterFlush() throws IOException {}

  public void sendShortCircuitShmResponse() throws IOException {}

  public boolean dropHeartbeatPacket() {
    return false;
  }

  public void stopSendingPacketDownstream(final String mirrAddr)
      throws IOException {
  }

  /**
   * Used as a hook to intercept the latency of sending packet.
   */
  public void logDelaySendingPacketDownstream(
      final String mirrAddr,
      final long delayMs) throws IOException {
  }

  public void delaySendingAckToUpstream(final String upstreamAddr)
      throws IOException {
  }

  /**
   * Used as a hook to delay sending the response of the last packet.
   */
  public void delayAckLastPacket() throws IOException {
  }

  /**
   * Used as a hook to delay writing a packet to disk.
   */
  public void delayWriteToDisk() {}

  /**
   * Used as a hook to delay writing a packet to os cache.
   */
  public void delayWriteToOsCache() {}

  /**
   * Used as a hook to intercept the latency of sending ack.
   */
  public void logDelaySendingAckToUpstream(
      final String upstreamAddr,
      final long delayMs)
      throws IOException {
  }

  public void noRegistration() throws IOException { }

  public void failTransfer(DatanodeID sourceDNId) throws IOException { }

  public void failMirrorConnection() throws IOException { }

  public void failPipeline(ReplicaInPipeline replicaInfo,
      String mirrorAddr) throws IOException { }

  public void startOfferService() throws Exception {}

  public void endOfferService() throws Exception {}

  public void throwTooManyOpenFiles() throws FileNotFoundException {
  }

  /**
   * Used as a hook to inject failure in erasure coding reconstruction
   * process.
   */
  public void stripedBlockReconstruction() throws IOException {}

  /**
   * Used as a hook to inject failure in erasure coding checksum reconstruction
   * process.
   */
  public void stripedBlockChecksumReconstruction() throws IOException {}

  /**
   * Used as a hook to inject latency when read block
   * in erasure coding reconstruction process.
   */
  public void delayBlockReader() {}

  /**
   * Used as a hook to inject intercept when free the block reader buffer.
   */
  public void interceptFreeBlockReaderBuffer() {}

  /**
   * Used as a hook to inject intercept When finish reading from block.
   */
  public void interceptBlockReader() {}

  /**
   * Used as a hook to inject intercept when BPOfferService hold lock.
   */
  public void delayWhenOfferServiceHoldLock() {}

  /**
   * Used as a hook to inject intercept when re-register.
   */
  public void blockUtilSendFullBlockReport() {}

  /**
   * Just delay a while.
   */
  public void delay() {}

  /**
   * Used as a hook to inject data pollution
   * into an erasure coding reconstruction.
   */
  public void badDecoding(ByteBuffer[] outputs) {}

  public void markSlow(String dnAddr, int[] replies) {}

  /**
   * Just delay delete replica a while.
   */
  public void delayDeleteReplica() {}

  /**
   * Just delay run diff record a while.
   */
  public void delayDiffRecord() {}

  /**
   * Just delay getMetaDataInputStream a while.
   */
  public void delayGetMetaDataInputStream() {}

  /**
   * Used in {@link DirectoryScanner#reconcile()} to wait until a storage is removed,
   * leaving a stale copy of {@link DirectoryScanner#diffs}.
   */
  public void waitUntilStorageRemoved() {}
}

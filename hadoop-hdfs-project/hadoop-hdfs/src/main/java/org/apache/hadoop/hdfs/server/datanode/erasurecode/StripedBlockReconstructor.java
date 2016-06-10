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
package org.apache.hadoop.hdfs.server.datanode.erasurecode;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * StripedBlockReconstructor reconstruct one or more missed striped block in
 * the striped block group, the minimum number of live striped blocks should
 * be no less than data block number.
 */
@InterfaceAudience.Private
class StripedBlockReconstructor extends StripedReconstructor
    implements Runnable {

  private StripedWriter stripedWriter;

  StripedBlockReconstructor(ErasureCodingWorker worker,
      StripedReconstructionInfo stripedReconInfo) {
    super(worker, stripedReconInfo);

    stripedWriter = new StripedWriter(this, getDatanode(),
        getConf(), stripedReconInfo);
  }

  boolean hasValidTargets() {
    return stripedWriter.hasValidTargets();
  }

  @Override
  public void run() {
    getDatanode().incrementXmitsInProgress();
    try {
      getStripedReader().init();

      stripedWriter.init();

      reconstruct();

      stripedWriter.endTargetBlocks();

      // Currently we don't check the acks for packets, this is similar as
      // block replication.
    } catch (Throwable e) {
      LOG.warn("Failed to reconstruct striped block: {}", getBlockGroup(), e);
      getDatanode().getMetrics().incrECFailedReconstructionTasks();
    } finally {
      getDatanode().decrementXmitsInProgress();
      getDatanode().getMetrics().incrECReconstructionTasks();
      getStripedReader().close();
      stripedWriter.close();
    }
  }

  void reconstruct() throws IOException {
    while (getPositionInBlock() < getMaxTargetLength()) {
      long remaining = getMaxTargetLength() - getPositionInBlock();
      final int toReconstructLen =
          (int) Math.min(getStripedReader().getBufferSize(), remaining);

      // step1: read from minimum source DNs required for reconstruction.
      // The returned success list is the source DNs we do real read from
      getStripedReader().readMinimumSources(toReconstructLen);

      // step2: decode to reconstruct targets
      reconstructTargets(toReconstructLen);

      // step3: transfer data
      if (stripedWriter.transferData2Targets() == 0) {
        String error = "Transfer failed for all targets.";
        throw new IOException(error);
      }

      updatePositionInBlock(toReconstructLen);

      clearBuffers();
    }
  }

  private void reconstructTargets(int toReconstructLen) {
    initDecoderIfNecessary();

    ByteBuffer[] inputs = getStripedReader().getInputBuffers(toReconstructLen);

    int[] erasedIndices = stripedWriter.getRealTargetIndices();
    ByteBuffer[] outputs = stripedWriter.getRealTargetBuffers(toReconstructLen);

    getDecoder().decode(inputs, erasedIndices, outputs);

    stripedWriter.updateRealTargetBuffers(toReconstructLen);
  }

  /**
   * Clear all associated buffers.
   */
  private void clearBuffers() {
    getStripedReader().clearBuffers();

    stripedWriter.clearBuffers();
  }
}

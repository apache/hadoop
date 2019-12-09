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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.util.CrcComposer;

/**
 * Computes striped composite CRCs over reconstructed chunk CRCs.
 */
@InterfaceAudience.Private
public class StripedBlockChecksumCompositeCrcReconstructor
    extends StripedBlockChecksumReconstructor {
  private final int ecPolicyCellSize;

  private byte[] digestValue;
  private CrcComposer digester;

  public StripedBlockChecksumCompositeCrcReconstructor(
      ErasureCodingWorker worker,
      StripedReconstructionInfo stripedReconInfo,
      DataOutputBuffer checksumWriter,
      long requestedBlockLength) throws IOException {
    super(worker, stripedReconInfo, checksumWriter, requestedBlockLength);
    this.ecPolicyCellSize = stripedReconInfo.getEcPolicy().getCellSize();
  }

  @Override
  public Object getDigestObject() {
    return digestValue;
  }

  @Override
  void prepareDigester() throws IOException {
    digester = CrcComposer.newStripedCrcComposer(
        getChecksum().getChecksumType(),
        getChecksum().getBytesPerChecksum(),
        ecPolicyCellSize);
  }

  @Override
  void updateDigester(byte[] checksumBytes, int dataBytesPerChecksum)
      throws IOException {
    if (digester == null) {
      throw new IOException(String.format(
          "Called updatedDigester with checksumBytes.length=%d, "
          + "dataBytesPerChecksum=%d but digester is null",
          checksumBytes.length, dataBytesPerChecksum));
    }
    digester.update(
        checksumBytes, 0, checksumBytes.length, dataBytesPerChecksum);
  }

  @Override
  void commitDigest() throws IOException {
    if (digester == null) {
      throw new IOException("Called commitDigest() but digester is null");
    }
    digestValue = digester.digest();
    getChecksumWriter().write(digestValue, 0, digestValue.length);
  }
}

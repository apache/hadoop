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
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ErasureCoderOptions;
import org.apache.hadoop.io.erasurecode.rawcoder.util.DumpUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;

/**
 * A raw erasure encoder in RS code scheme in pure Java in case native one
 * isn't available in some environment. Please always use native implementations
 * when possible. This new Java coder is about 5X faster than the one originated
 * from HDFS-RAID, and also compatible with the native/ISA-L coder.
 */
@InterfaceAudience.Private
public class RSRawEncoder extends RawErasureEncoder {
  // relevant to schema and won't change during encode calls.
  private byte[] encodeMatrix;
  /**
   * Array of input tables generated from coding coefficients previously.
   * Must be of size 32*k*rows
   */
  private byte[] gfTables;

  public RSRawEncoder(ErasureCoderOptions coderOptions) {
    super(coderOptions);

    if (getNumAllUnits() >= RSUtil.GF.getFieldSize()) {
      throw new HadoopIllegalArgumentException(
          "Invalid numDataUnits and numParityUnits");
    }

    encodeMatrix = new byte[getNumAllUnits() * getNumDataUnits()];
    RSUtil.genCauchyMatrix(encodeMatrix, getNumAllUnits(), getNumDataUnits());
    if (allowVerboseDump()) {
      DumpUtil.dumpMatrix(encodeMatrix, getNumDataUnits(), getNumAllUnits());
    }
    gfTables = new byte[getNumAllUnits() * getNumDataUnits() * 32];
    RSUtil.initTables(getNumDataUnits(), getNumParityUnits(), encodeMatrix,
        getNumDataUnits() * getNumDataUnits(), gfTables);
    if (allowVerboseDump()) {
      System.out.println(DumpUtil.bytesToHex(gfTables, -1));
    }
  }

  @Override
  protected void doEncode(ByteBufferEncodingState encodingState) {
    CoderUtil.resetOutputBuffers(encodingState.outputs,
        encodingState.encodeLength);
    RSUtil.encodeData(gfTables, encodingState.inputs, encodingState.outputs);
  }

  @Override
  protected void doEncode(ByteArrayEncodingState encodingState) {
    CoderUtil.resetOutputBuffers(encodingState.outputs,
        encodingState.outputOffsets,
        encodingState.encodeLength);
    RSUtil.encodeData(gfTables, encodingState.encodeLength,
        encodingState.inputs,
        encodingState.inputOffsets, encodingState.outputs,
        encodingState.outputOffsets);
  }
}

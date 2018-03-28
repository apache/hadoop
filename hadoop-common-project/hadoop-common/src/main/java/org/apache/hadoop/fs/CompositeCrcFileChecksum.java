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
package org.apache.hadoop.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.util.CrcUtil;
import org.apache.hadoop.util.DataChecksum;

/** Composite CRC. */
@InterfaceAudience.LimitedPrivate({"HDFS"})
@InterfaceStability.Unstable
public class CompositeCrcFileChecksum extends FileChecksum {
  public static final int LENGTH = Integer.SIZE / Byte.SIZE;

  private int crc;
  private DataChecksum.Type crcType;
  private int bytesPerCrc;

  /** Create a CompositeCrcFileChecksum. */
  public CompositeCrcFileChecksum(
      int crc, DataChecksum.Type crcType, int bytesPerCrc) {
    this.crc = crc;
    this.crcType = crcType;
    this.bytesPerCrc = bytesPerCrc;
  }

  @Override
  public String getAlgorithmName() {
    return "COMPOSITE-" + crcType.name();
  }

  @Override
  public int getLength() {
    return LENGTH;
  }

  @Override
  public byte[] getBytes() {
    return CrcUtil.intToBytes(crc);
  }

  @Override
  public ChecksumOpt getChecksumOpt() {
    return new ChecksumOpt(crcType, bytesPerCrc);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    crc = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(crc);
  }

  @Override
  public String toString() {
    return getAlgorithmName() + ":" + String.format("0x%08x", crc);
  }
}

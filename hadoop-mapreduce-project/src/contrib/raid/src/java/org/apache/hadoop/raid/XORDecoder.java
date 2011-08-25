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

package org.apache.hadoop.raid;

import java.io.OutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

public class XORDecoder extends Decoder {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.XORDecoder");

  public XORDecoder(
    Configuration conf, int stripeSize) {
    super(conf, stripeSize, 1);
  }

  @Override
  protected void fixErasedBlock(
      FileSystem fs, Path srcFile, FileSystem parityFs, Path parityFile,
      long blockSize, long errorOffset, long bytesToSkip, long limit,
      OutputStream out) throws IOException {
    LOG.info("Fixing block at " + srcFile + ":" + errorOffset +
             ", skipping " + bytesToSkip + ", limit " + limit);
    FileStatus srcStat = fs.getFileStatus(srcFile);
    ArrayList<FSDataInputStream> xorinputs = new ArrayList<FSDataInputStream>();

    FSDataInputStream parityFileIn = parityFs.open(parityFile);
    parityFileIn.seek(parityOffset(errorOffset, blockSize));
    xorinputs.add(parityFileIn);

    long errorBlockOffset = (errorOffset / blockSize) * blockSize;
    long[] srcOffsets = stripeOffsets(errorOffset, blockSize);
    for (int i = 0; i < srcOffsets.length; i++) {
      if (srcOffsets[i] == errorBlockOffset) {
        LOG.info("Skipping block at " + srcFile + ":" + errorBlockOffset);
        continue;
      }
      if (srcOffsets[i] < srcStat.getLen()) {
        FSDataInputStream in = fs.open(srcFile);
        in.seek(srcOffsets[i]);
        xorinputs.add(in);
      }
    }
    FSDataInputStream[] inputs = xorinputs.toArray(
                                    new FSDataInputStream[]{null});
    ParityInputStream recovered =
      new ParityInputStream(inputs, limit, readBufs[0], writeBufs[0]);
    recovered.skip(bytesToSkip);
    recovered.drain(out, null);
  }

  protected long[] stripeOffsets(long errorOffset, long blockSize) {
    long[] offsets = new long[stripeSize];
    long stripeIdx = errorOffset / (blockSize * stripeSize);
    long startOffsetOfStripe = stripeIdx * stripeSize * blockSize;
    for (int i = 0; i < stripeSize; i++) {
      offsets[i] = startOffsetOfStripe + i * blockSize;
    }
    return offsets;
  }

  protected long parityOffset(long errorOffset, long blockSize) {
    long stripeIdx = errorOffset / (blockSize * stripeSize);
    return stripeIdx * blockSize;
  }

}

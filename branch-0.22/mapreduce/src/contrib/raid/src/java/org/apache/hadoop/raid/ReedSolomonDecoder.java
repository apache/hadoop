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
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;

public class ReedSolomonDecoder extends Decoder {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.ReedSolomonDecoder");
  private ErasureCode reedSolomonCode;

  public ReedSolomonDecoder(
    Configuration conf, int stripeSize, int paritySize) {
    super(conf, stripeSize, paritySize);
    this.reedSolomonCode = new ReedSolomonCode(stripeSize, paritySize);
  }

  @Override
  protected void fixErasedBlock(
      FileSystem fs, Path srcFile,
      FileSystem parityFs, Path parityFile,
      long blockSize, long errorOffset, long bytesToSkip, long limit,
      OutputStream out) throws IOException {
    FSDataInputStream[] inputs = new FSDataInputStream[stripeSize + paritySize];
    int[] erasedLocations = buildInputs(fs, srcFile, parityFs, parityFile,
                                        errorOffset, inputs);
    int blockIdxInStripe = ((int)(errorOffset/blockSize)) % stripeSize;
    int erasedLocationToFix = paritySize + blockIdxInStripe;
    writeFixedBlock(inputs, erasedLocations, erasedLocationToFix,
                    bytesToSkip, limit, out);
  }

  protected int[] buildInputs(FileSystem fs, Path srcFile,
                              FileSystem parityFs, Path parityFile,
                              long errorOffset, FSDataInputStream[] inputs)
      throws IOException {
    LOG.info("Building inputs to recover block starting at " + errorOffset);
    FileStatus srcStat = fs.getFileStatus(srcFile);
    long blockSize = srcStat.getBlockSize();
    long blockIdx = (int)(errorOffset / blockSize);
    long stripeIdx = blockIdx / stripeSize;
    LOG.info("FileSize = " + srcStat.getLen() + ", blockSize = " + blockSize +
             ", blockIdx = " + blockIdx + ", stripeIdx = " + stripeIdx);
    ArrayList<Integer> erasedLocations = new ArrayList<Integer>();
    // First open streams to the parity blocks.
    for (int i = 0; i < paritySize; i++) {
      long offset = blockSize * (stripeIdx * paritySize + i);
      FSDataInputStream in = parityFs.open(
        parityFile, conf.getInt("io.file.buffer.size", 64 * 1024));
      in.seek(offset);
      LOG.info("Adding " + parityFile + ":" + offset + " as input " + i);
      inputs[i] = in;
    }
    // Now open streams to the data blocks.
    for (int i = paritySize; i < paritySize + stripeSize; i++) {
      long offset = blockSize * (stripeIdx * stripeSize + i - paritySize);
      if (offset == errorOffset) {
        LOG.info(srcFile + ":" + offset +
            " is known to have error, adding zeros as input " + i);
        inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
            offset + blockSize));
        erasedLocations.add(i);
      } else if (offset > srcStat.getLen()) {
        LOG.info(srcFile + ":" + offset +
                 " is past file size, adding zeros as input " + i);
        inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
            offset + blockSize));
      } else {
        FSDataInputStream in = fs.open(
          srcFile, conf.getInt("io.file.buffer.size", 64 * 1024));
        in.seek(offset);
        LOG.info("Adding " + srcFile + ":" + offset + " as input " + i);
        inputs[i] = in;
      }
    }
    if (erasedLocations.size() > paritySize) {
      String msg = "Too many erased locations: " + erasedLocations.size();
      LOG.error(msg);
      throw new IOException(msg);
    }
    int[] locs = new int[erasedLocations.size()];
    for (int i = 0; i < locs.length; i++) {
      locs[i] = erasedLocations.get(i);
    }
    return locs;
  }

  /**
   * Decode the inputs provided and write to the output.
   * @param inputs array of inputs.
   * @param erasedLocations indexes in the inputs which are known to be erased.
   * @param erasedLocationToFix index in the inputs which needs to be fixed.
   * @param skipBytes number of bytes to skip before writing to output.
   * @param limit maximum number of bytes to be written/skipped.
   * @param out the output.
   * @throws IOException
   */
  void writeFixedBlock(
          FSDataInputStream[] inputs,
          int[] erasedLocations,
          int erasedLocationToFix,
          long skipBytes,
          long limit,
          OutputStream out) throws IOException {

    LOG.info("Need to write " + (limit - skipBytes) +
             " bytes for erased location index " + erasedLocationToFix);
    int[] tmp = new int[inputs.length];
    int[] decoded = new int[erasedLocations.length];
    long toDiscard = skipBytes;
    // Loop while the number of skipped + written bytes is less than the max.
    for (long written = 0; skipBytes + written < limit; ) {
      erasedLocations = readFromInputs(inputs, erasedLocations, limit);
      if (decoded.length != erasedLocations.length) {
        decoded = new int[erasedLocations.length];
      }

      int toWrite = (int)Math.min((long)bufSize, limit - (skipBytes + written));
      if (toDiscard >= toWrite) {
        toDiscard -= toWrite;
        continue;
      }

      // Decoded bufSize amount of data.
      for (int i = 0; i < bufSize; i++) {
        performDecode(readBufs, writeBufs, i, tmp, erasedLocations, decoded);
      }

      for (int i = 0; i < erasedLocations.length; i++) {
        if (erasedLocations[i] == erasedLocationToFix) {
          toWrite -= toDiscard;
          out.write(writeBufs[i], (int)toDiscard, toWrite);
          toDiscard = 0;
          written += toWrite;
          LOG.debug("Wrote " + toWrite + " bytes for erased location index " +
                    erasedLocationToFix);
          break;
        }
      }
    }
  }

  int[] readFromInputs(
          FSDataInputStream[] inputs,
          int[] erasedLocations,
          long limit) throws IOException {
    // For every input, read some data = bufSize.
    for (int i = 0; i < inputs.length; i++) {
      long curPos = inputs[i].getPos();
      try {
        RaidUtils.readTillEnd(inputs[i], readBufs[i], true);
        continue;
      } catch (BlockMissingException e) {
        LOG.error("Encountered BlockMissingException in stream " + i);
      } catch (ChecksumException e) {
        LOG.error("Encountered ChecksumException in stream " + i);
      }

      // Found a new erased location.
      if (erasedLocations.length == paritySize) {
        String msg = "Too many read errors";
        LOG.error(msg);
        throw new IOException(msg);
      }

      // Add this stream to the set of erased locations.
      int[] newErasedLocations = new int[erasedLocations.length + 1];
      for (int j = 0; j < erasedLocations.length; j++) {
        newErasedLocations[j] = erasedLocations[j];
      }
      newErasedLocations[newErasedLocations.length - 1] = i;
      erasedLocations = newErasedLocations;

      LOG.info("Using zeros for stream " + i);
      inputs[i] = new FSDataInputStream(
        new RaidUtils.ZeroInputStream(curPos + limit));
      inputs[i].seek(curPos);
      RaidUtils.readTillEnd(inputs[i], readBufs[i], true);
    }
    return erasedLocations;
  }

  void performDecode(byte[][] readBufs, byte[][] writeBufs,
                     int idx, int[] inputs,
                     int[] erasedLocations, int[] decoded) {
    for (int i = 0; i < decoded.length; i++) {
      decoded[i] = 0;
    }
    for (int i = 0; i < inputs.length; i++) {
      inputs[i] = readBufs[i][idx] & 0x000000FF;
    }
    reedSolomonCode.decode(inputs, erasedLocations, decoded);
    for (int i = 0; i < decoded.length; i++) {
      writeBufs[i][idx] = (byte)decoded[i];
    }
  }

}

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

package org.apache.hadoop.fs.slive;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.slive.DataWriter.GenerateOutput;
import org.apache.hadoop.fs.slive.OperationOutput.OutputType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Operation which selects a random file and a random number of bytes to create
 * that file with (from the write size option) and selects a random block size
 * (from the block size option) and a random replication amount (from the
 * replication option) and attempts to create a file with those options.
 * 
 * This operation will capture statistics on success for bytes written, time
 * taken (milliseconds), and success count and on failure it will capture the
 * number of failures and the time taken (milliseconds) to fail.
 */
class CreateOp extends Operation {

  private static final Logger LOG = LoggerFactory.getLogger(CreateOp.class);

  private static int DEF_IO_BUFFER_SIZE = 4096;

  private static final String IO_BUF_CONFIG = ("io.file.buffer.size");

  CreateOp(ConfigExtractor cfg, Random rnd) {
    super(CreateOp.class.getSimpleName(), cfg, rnd);
  }

  /**
   * Returns the block size to use (aligned to nearest BYTES_PER_CHECKSUM if
   * configuration says a value exists) - this will avoid the warnings caused by
   * this not occurring and the file will not be created if it is not correct...
   * 
   * @return long
   */
  private long determineBlockSize() {
    Range<Long> blockSizeRange = getConfig().getBlockSize();
    long blockSize = Range.betweenPositive(getRandom(), blockSizeRange);
    Long byteChecksum = getConfig().getByteCheckSum();
    if (byteChecksum == null) {
      return blockSize;
    }
    // adjust to nearest multiple
    long full = (blockSize / byteChecksum) * byteChecksum;
    long toFull = blockSize - full;
    if (toFull >= (byteChecksum / 2)) {
      full += byteChecksum;
    }
    // adjust if over extended
    if (full > blockSizeRange.getUpper()) {
      full = blockSizeRange.getUpper();
    }
    if (full < blockSizeRange.getLower()) {
      full = blockSizeRange.getLower();
    }
    return full;
  }

  /**
   * Gets the replication amount
   * 
   * @return short
   */
  private short determineReplication() {
    Range<Short> replicationAmountRange = getConfig().getReplication();
    Range<Long> repRange = new Range<Long>(replicationAmountRange.getLower()
        .longValue(), replicationAmountRange.getUpper().longValue());
    short replicationAmount = (short) Range.betweenPositive(getRandom(),
        repRange);
    return replicationAmount;
  }

  /**
   * Gets the output buffering size to use
   * 
   * @return int
   */
  private int getBufferSize() {
    return getConfig().getConfig().getInt(IO_BUF_CONFIG, DEF_IO_BUFFER_SIZE);
  }

  /**
   * Gets the file to create
   * 
   * @return Path
   */
  protected Path getCreateFile() {
    Path fn = getFinder().getFile();
    return fn;
  }

  @Override // Operation
  List<OperationOutput> run(FileSystem fs) {
    List<OperationOutput> out = super.run(fs);
    FSDataOutputStream os = null;
    try {
      Path fn = getCreateFile();
      Range<Long> writeSizeRange = getConfig().getWriteSize();
      long writeSize = 0;
      long blockSize = determineBlockSize();
      short replicationAmount = determineReplication();
      if (getConfig().shouldWriteUseBlockSize()) {
        writeSizeRange = getConfig().getBlockSize();
      }
      writeSize = Range.betweenPositive(getRandom(), writeSizeRange);
      long bytesWritten = 0;
      long timeTaken = 0;
      int bufSize = getBufferSize();
      boolean overWrite = false;
      DataWriter writer = new DataWriter(getRandom());
      LOG.info("Attempting to create file at " + fn + " of size "
          + Helper.toByteInfo(writeSize) + " using blocksize "
          + Helper.toByteInfo(blockSize) + " and replication amount "
          + replicationAmount);
      {
        // open & create
        long startTime = Timer.now();
        os = fs.create(fn, overWrite, bufSize, replicationAmount, blockSize);
        timeTaken += Timer.elapsed(startTime);
        // write the given length
        GenerateOutput stats = writer.writeSegment(writeSize, os);
        bytesWritten += stats.getBytesWritten();
        timeTaken += stats.getTimeTaken();
        // capture close time
        startTime = Timer.now();
        os.close();
        os = null;
        timeTaken += Timer.elapsed(startTime);
      }
      LOG.info("Created file at " + fn + " of size "
          + Helper.toByteInfo(bytesWritten) + " bytes using blocksize "
          + Helper.toByteInfo(blockSize) + " and replication amount "
          + replicationAmount + " in " + timeTaken + " milliseconds");
      // collect all the stats
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.OK_TIME_TAKEN, timeTaken));
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.BYTES_WRITTEN, bytesWritten));
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.SUCCESSES, 1L));
    } catch (IOException e) {
      out.add(new OperationOutput(OutputType.LONG, getType(),
          ReportWriter.FAILURES, 1L));
      LOG.warn("Error with creating", e);
    } finally {
      if (os != null) {
        try {
          os.close();
        } catch (IOException e) {
          LOG.warn("Error closing create stream", e);
        }
      }
    }
    return out;
  }
}

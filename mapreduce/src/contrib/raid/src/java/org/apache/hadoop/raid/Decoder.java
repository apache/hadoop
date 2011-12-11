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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

/**
 * Represents a generic decoder that can be used to read a file with
 * corrupt blocks by using the parity file.
 * This is an abstract class, concrete subclasses need to implement
 * fixErasedBlock.
 */
public abstract class Decoder {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.Decoder");
  protected Configuration conf;
  protected int stripeSize;
  protected int paritySize;
  protected Random rand;
  protected int bufSize;
  protected byte[][] readBufs;
  protected byte[][] writeBufs;

  Decoder(Configuration conf, int stripeSize, int paritySize) {
    this.conf = conf;
    this.stripeSize = stripeSize;
    this.paritySize = paritySize;
    this.rand = new Random();
    this.bufSize = conf.getInt("raid.decoder.bufsize", 1024 * 1024);
    this.readBufs = new byte[stripeSize + paritySize][];
    this.writeBufs = new byte[paritySize][];
    allocateBuffers();
  }

  private void allocateBuffers() {
    for (int i = 0; i < stripeSize + paritySize; i++) {
      readBufs[i] = new byte[bufSize];
    }
    for (int i = 0; i < paritySize; i++) {
      writeBufs[i] = new byte[bufSize];
    }
  }

  private void configureBuffers(long blockSize) {
    if ((long)bufSize > blockSize) {
      bufSize = (int)blockSize;
      allocateBuffers();
    } else if (blockSize % bufSize != 0) {
      bufSize = (int)(blockSize / 256L); // heuristic.
      if (bufSize == 0) {
        bufSize = 1024;
      }
      bufSize = Math.min(bufSize, 1024 * 1024);
      allocateBuffers();
    }
  }

  /**
   * The interface to generate a decoded file using the good portion of the
   * source file and the parity file.
   * @param fs The filesystem containing the source file.
   * @param srcFile The damaged source file.
   * @param parityFs The filesystem containing the parity file. This could be
   *        different from fs in case the parity file is part of a HAR archive.
   * @param parityFile The parity file.
   * @param errorOffset Known location of error in the source file. There could
   *        be additional errors in the source file that are discovered during
   *        the decode process.
   * @param decodedFile The decoded file. This will have the exact same contents
   *        as the source file on success.
   */
  public void decodeFile(
    FileSystem fs, Path srcFile, FileSystem parityFs, Path parityFile,
    long errorOffset, Path decodedFile) throws IOException {

    LOG.info("Create " + decodedFile + " for error at " +
            srcFile + ":" + errorOffset);
    FileStatus srcStat = fs.getFileStatus(srcFile);
    long blockSize = srcStat.getBlockSize();
    configureBuffers(blockSize);
    // Move the offset to the start of the block.
    errorOffset = (errorOffset / blockSize) * blockSize;

    // Create the decoded file.
    FSDataOutputStream out = fs.create(
      decodedFile, false, conf.getInt("io.file.buffer.size", 64 * 1024),
      srcStat.getReplication(), srcStat.getBlockSize());

    // Open the source file.
    FSDataInputStream in = fs.open(
      srcFile, conf.getInt("io.file.buffer.size", 64 * 1024));

    // Start copying data block-by-block.
    for (long offset = 0; offset < srcStat.getLen(); offset += blockSize) {
      long limit = Math.min(blockSize, srcStat.getLen() - offset);
      long bytesAlreadyCopied = 0;
      if (offset != errorOffset) {
        try {
          in = fs.open(
            srcFile, conf.getInt("io.file.buffer.size", 64 * 1024));
          in.seek(offset);
          RaidUtils.copyBytes(in, out, readBufs[0], limit);
          assert(out.getPos() == offset +limit);
          LOG.info("Copied till " + out.getPos() + " from " + srcFile);
          continue;
        } catch (BlockMissingException e) {
          LOG.info("Encountered BME at " + srcFile + ":" + offset);
          bytesAlreadyCopied = out.getPos() - offset;
        } catch (ChecksumException e) {
          LOG.info("Encountered CE at " + srcFile + ":" + offset);
          bytesAlreadyCopied = out.getPos() - offset;
        }
      }
      // If we are here offset == errorOffset or we got an exception.
      // Recover the block starting at offset.
      fixErasedBlock(fs, srcFile, parityFs, parityFile, blockSize, offset,
        bytesAlreadyCopied, limit, out);
    }
    out.close();

    try {
      fs.setOwner(decodedFile, srcStat.getOwner(), srcStat.getGroup());
      fs.setPermission(decodedFile, srcStat.getPermission());
      fs.setTimes(decodedFile, srcStat.getModificationTime(),
                  srcStat.getAccessTime());
    } catch (Exception exc) {
      LOG.info("Didn't manage to copy meta information because of " + exc +
               " Ignoring...");
    }

  }

  /**
   * Recovers a corrupt block to local file.
   *
   * @param srcFs The filesystem containing the source file.
   * @param srcPath The damaged source file.
   * @param parityPath The filesystem containing the parity file. This could be
   *        different from fs in case the parity file is part of a HAR archive.
   * @param parityFile The parity file.
   * @param blockSize The block size of the file.
   * @param blockOffset Known location of error in the source file. There could
   *        be additional errors in the source file that are discovered during
   *        the decode process.
   * @param localBlockFile The file to write the block to.
   * @param limit The maximum number of bytes to be written out.
   *              This is to prevent writing beyond the end of the file.
   */
  public void recoverBlockToFile(
    FileSystem srcFs, Path srcPath, FileSystem parityFs, Path parityPath,
    long blockSize, long blockOffset, File localBlockFile, long limit)
    throws IOException {
    OutputStream out = new FileOutputStream(localBlockFile);
    fixErasedBlock(srcFs, srcPath, parityFs, parityPath,
                  blockSize, blockOffset, 0, limit, out);
    out.close();
  }

  /**
   * Implementation-specific mechanism of writing a fixed block.
   * @param fs The filesystem containing the source file.
   * @param srcFile The damaged source file.
   * @param parityFs The filesystem containing the parity file. This could be
   *        different from fs in case the parity file is part of a HAR archive.
   * @param parityFile The parity file.
   * @param blockSize The maximum size of a block.
   * @param errorOffset Known location of error in the source file. There could
   *        be additional errors in the source file that are discovered during
   *        the decode process.
   * @param bytesToSkip After the block is generated, these many bytes should be
   *       skipped before writing to the output. This is needed because the
   *       output may have a portion of the block written from the source file
   *       before a new corruption is discovered in the block.
   * @param limit The maximum number of bytes to be written out, including
   *       bytesToSkip. This is to prevent writing beyond the end of the file.
   * @param out The output.
   */
  protected abstract void fixErasedBlock(
      FileSystem fs, Path srcFile, FileSystem parityFs, Path parityFile,
      long blockSize, long errorOffset, long bytesToSkip, long limit,
      OutputStream out) throws IOException;
}

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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.apache.hadoop.fs.slive.Constants.BYTES_PER_LONG;

/**
 * Class which handles generating data (creating and appending) along with
 * ensuring the correct data is written out for the given path name so that it
 * can be later verified
 */
class DataWriter {
  /**
   * Header size in bytes
   */
  private static final int HEADER_LENGTH = (BYTES_PER_LONG * 2);

  private int bufferSize;
  private Random rnd;

  /**
   * Class used to hold the number of bytes written and time taken for write
   * operations for callers to use
   */
  static class GenerateOutput {

    private long bytes;
    private long time;

    GenerateOutput(long bytesWritten, long timeTaken) {
      this.bytes = bytesWritten;
      this.time = timeTaken;
    }

    long getBytesWritten() {
      return bytes;
    }

    long getTimeTaken() {
      return time;
    }

    public String toString() {
      return "Wrote " + getBytesWritten() + " bytes " + " which took "
          + getTimeTaken() + " milliseconds";
    }
  }

  /**
   * Class used to hold a byte buffer and offset position for generating data
   */
  private static class GenerateResult {
    private long offset;
    private ByteBuffer buffer;

    GenerateResult(long offset, ByteBuffer buffer) {
      this.offset = offset;
      this.buffer = buffer;
    }

    long getOffset() {
      return offset;
    }

    ByteBuffer getBuffer() {
      return buffer;
    }
  }

  /**
   * What a header write output returns need the hash value to use and the time
   * taken to perform the write + bytes written
   */
  private static class WriteInfo {
    private long hashValue;
    private long bytesWritten;
    private long timeTaken;

    WriteInfo(long hashValue, long bytesWritten, long timeTaken) {
      this.hashValue = hashValue;
      this.bytesWritten = bytesWritten;
      this.timeTaken = timeTaken;
    }

    long getHashValue() {
      return hashValue;
    }

    long getTimeTaken() {
      return timeTaken;
    }

    long getBytesWritten() {
      return bytesWritten;
    }
  }

  /**
   * Inits with given buffer size (must be greater than bytes per long and a
   * multiple of bytes per long)
   * 
   * @param rnd
   *          random number generator to use for hash value creation
   * 
   * @param bufferSize
   *          size which must be greater than BYTES_PER_LONG and which also must
   *          be a multiple of BYTES_PER_LONG
   */
  DataWriter(Random rnd, int bufferSize) {
    if (bufferSize < BYTES_PER_LONG) {
      throw new IllegalArgumentException(
          "Buffer size must be greater than or equal to " + BYTES_PER_LONG);
    }
    if ((bufferSize % BYTES_PER_LONG) != 0) {
      throw new IllegalArgumentException("Buffer size must be a multiple of "
          + BYTES_PER_LONG);
    }
    this.bufferSize = bufferSize;
    this.rnd = rnd;
  }

  /**
   * Inits with default buffer size
   */
  DataWriter(Random rnd) {
    this(rnd, Constants.BUFFERSIZE);
  }

  /**
   * Generates a partial segment which is less than bytes per long size
   * 
   * @param byteAm
   *          the number of bytes to generate (less than bytes per long)
   * @param offset
   *          the staring offset
   * @param hasher
   *          hasher to use for generating data given an offset
   * 
   * @return GenerateResult containing new offset and byte buffer
   */
  private GenerateResult generatePartialSegment(int byteAm, long offset,
      DataHasher hasher) {
    if (byteAm > BYTES_PER_LONG) {
      throw new IllegalArgumentException(
          "Partial bytes must be less or equal to " + BYTES_PER_LONG);
    }
    if (byteAm <= 0) {
      throw new IllegalArgumentException(
          "Partial bytes must be greater than zero and not " + byteAm);
    }
    ByteBuffer buf = ByteBuffer.wrap(new byte[BYTES_PER_LONG]);
    buf.putLong(hasher.generate(offset));
    ByteBuffer allBytes = ByteBuffer.wrap(new byte[byteAm]);
    buf.rewind();
    for (int i = 0; i < byteAm; ++i) {
      allBytes.put(buf.get());
    }
    allBytes.rewind();
    return new GenerateResult(offset, allBytes);
  }

  /**
   * Generates a full segment (aligned to bytes per long) of the given byte
   * amount size
   * 
   * @param byteAm
   *          long aligned size
   * @param startOffset
   *          starting hash offset
   * @param hasher
   *          hasher to use for generating data given an offset
   * @return GenerateResult containing new offset and byte buffer
   */
  private GenerateResult generateFullSegment(int byteAm, long startOffset,
      DataHasher hasher) {
    if (byteAm <= 0) {
      throw new IllegalArgumentException(
          "Byte amount must be greater than zero and not " + byteAm);
    }
    if ((byteAm % BYTES_PER_LONG) != 0) {
      throw new IllegalArgumentException("Byte amount " + byteAm
          + " must be a multiple of " + BYTES_PER_LONG);
    }
    // generate all the segments
    ByteBuffer allBytes = ByteBuffer.wrap(new byte[byteAm]);
    long offset = startOffset;
    ByteBuffer buf = ByteBuffer.wrap(new byte[BYTES_PER_LONG]);
    for (long i = 0; i < byteAm; i += BYTES_PER_LONG) {
      buf.rewind();
      buf.putLong(hasher.generate(offset));
      buf.rewind();
      allBytes.put(buf);
      offset += BYTES_PER_LONG;
    }
    allBytes.rewind();
    return new GenerateResult(offset, allBytes);
  }

  /**
   * Writes a set of bytes to the output stream, for full segments it will write
   * out the complete segment but for partial segments, ie when the last
   * position does not fill up a full long then a partial set will be written
   * out containing the needed bytes from the expected full segment
   * 
   * @param byteAm
   *          the amount of bytes to write
   * @param startPos
   *          a BYTES_PER_LONG aligned start position
   * @param hasher
   *          hasher to use for generating data given an offset
   * @param out
   *          the output stream to write to
   * @return how many bytes were written
   * @throws IOException
   */
  private GenerateOutput writePieces(long byteAm, long startPos,
      DataHasher hasher, OutputStream out) throws IOException {
    if (byteAm <= 0) {
      return new GenerateOutput(0, 0);
    }
    if (startPos < 0) {
      startPos = 0;
    }
    int leftOver = (int) (byteAm % bufferSize);
    long fullPieces = byteAm / bufferSize;
    long offset = startPos;
    long bytesWritten = 0;
    long timeTaken = 0;
    // write the full pieces that fit in the buffer size
    for (long i = 0; i < fullPieces; ++i) {
      GenerateResult genData = generateFullSegment(bufferSize, offset, hasher);
      offset = genData.getOffset();
      ByteBuffer gBuf = genData.getBuffer();
      {
        byte[] buf = gBuf.array();
        long startTime = Timer.now();
        out.write(buf);
        if (Constants.FLUSH_WRITES) {
          out.flush();
        }
        timeTaken += Timer.elapsed(startTime);
        bytesWritten += buf.length;
      }
    }
    if (leftOver > 0) {
      ByteBuffer leftOverBuf = ByteBuffer.wrap(new byte[leftOver]);
      int bytesLeft = leftOver % BYTES_PER_LONG;
      leftOver = leftOver - bytesLeft;
      // collect the piece which do not fit in the buffer size but is
      // also greater or eq than BYTES_PER_LONG and a multiple of it
      if (leftOver > 0) {
        GenerateResult genData = generateFullSegment(leftOver, offset, hasher);
        offset = genData.getOffset();
        leftOverBuf.put(genData.getBuffer());
      }
      // collect any single partial byte segment
      if (bytesLeft > 0) {
        GenerateResult genData = generatePartialSegment(bytesLeft, offset,
            hasher);
        offset = genData.getOffset();
        leftOverBuf.put(genData.getBuffer());
      }
      // do the write of both
      leftOverBuf.rewind();
      {
        byte[] buf = leftOverBuf.array();
        long startTime = Timer.now();
        out.write(buf);
        if (Constants.FLUSH_WRITES) {
          out.flush();
        }
        timeTaken += Timer.elapsed(startTime);
        bytesWritten += buf.length;
      }
    }
    return new GenerateOutput(bytesWritten, timeTaken);
  }

  /**
   * Writes to a stream the given number of bytes specified
   * 
   * @param byteAm
   *          the file size in number of bytes to write
   * 
   * @param out
   *          the outputstream to write to
   * 
   * @return the number of bytes written + time taken
   * 
   * @throws IOException
   */
  GenerateOutput writeSegment(long byteAm, OutputStream out)
      throws IOException {
    long headerLen = getHeaderLength();
    if (byteAm < headerLen) {
      // not enough bytes to write even the header
      return new GenerateOutput(0, 0);
    }
    // adjust for header length
    byteAm -= headerLen;
    if (byteAm < 0) {
      byteAm = 0;
    }
    WriteInfo header = writeHeader(out, byteAm);
    DataHasher hasher = new DataHasher(header.getHashValue());
    GenerateOutput pRes = writePieces(byteAm, 0, hasher, out);
    long bytesWritten = pRes.getBytesWritten() + header.getBytesWritten();
    long timeTaken = header.getTimeTaken() + pRes.getTimeTaken();
    return new GenerateOutput(bytesWritten, timeTaken);
  }

  /**
   * Gets the header length
   * 
   * @return int
   */
  static int getHeaderLength() {
    return HEADER_LENGTH;
  }

  /**
   * Writes a header to the given output stream
   * 
   * @param os
   *          output stream to write to
   * 
   * @param fileSize
   *          the file size to write
   * 
   * @return WriteInfo
   * 
   * @throws IOException
   *           if a write failure occurs
   */
  WriteInfo writeHeader(OutputStream os, long fileSize) throws IOException {
    int headerLen = getHeaderLength();
    ByteBuffer buf = ByteBuffer.wrap(new byte[headerLen]);
    long hash = rnd.nextLong();
    buf.putLong(hash);
    buf.putLong(fileSize);
    buf.rewind();
    byte[] headerData = buf.array();
    long elapsed = 0;
    {
      long startTime = Timer.now();
      os.write(headerData);
      if (Constants.FLUSH_WRITES) {
        os.flush();
      }
      elapsed += Timer.elapsed(startTime);
    }
    return new WriteInfo(hash, headerLen, elapsed);
  }
}

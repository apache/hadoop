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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Class which reads in and verifies bytes that have been read in
 */
class DataVerifier {
  private static final int BYTES_PER_LONG = Constants.BYTES_PER_LONG;

  private int bufferSize;

  /**
   * The output from verification includes the number of chunks that were the
   * same as expected and the number of segments that were different than what
   * was expected and the number of total bytes read
   */
  static class VerifyOutput {
    private long same;
    private long different;
    private long read;
    private long readTime;

    VerifyOutput(long sameChunks, long differentChunks, long readBytes,
        long readTime) {
      this.same = sameChunks;
      this.different = differentChunks;
      this.read = readBytes;
      this.readTime = readTime;
    }

    long getReadTime() {
      return this.readTime;
    }

    long getBytesRead() {
      return this.read;
    }

    long getChunksSame() {
      return same;
    }

    long getChunksDifferent() {
      return different;
    }

    public String toString() {
      return "Bytes read = " + getBytesRead() + " same = " + getChunksSame()
          + " different = " + getChunksDifferent() + " in " + getReadTime()
          + " milliseconds";
    }

  }

  /**
   * Class used to hold the result of a read on a header
   */
  private static class ReadInfo {
    private long byteAm;
    private long hash;
    private long timeTaken;
    private long bytesRead;

    ReadInfo(long byteAm, long hash, long timeTaken, long bytesRead) {
      this.byteAm = byteAm;
      this.hash = hash;
      this.timeTaken = timeTaken;
      this.bytesRead = bytesRead;
    }

    long getByteAm() {
      return byteAm;
    }

    long getHashValue() {
      return hash;
    }

    long getTimeTaken() {
      return timeTaken;
    }

    long getBytesRead() {
      return bytesRead;
    }

  }

  /**
   * Storage class used to hold the chunks same and different for buffered reads
   * and the resultant verification
   */
  private static class VerifyInfo {

    VerifyInfo(long same, long different) {
      this.same = same;
      this.different = different;
    }

    long getSame() {
      return same;
    }

    long getDifferent() {
      return different;
    }

    private long same;
    private long different;
  }

  /**
   * Inits with given buffer size (must be greater than bytes per long and a
   * multiple of bytes per long)
   * 
   * @param bufferSize
   *          size which must be greater than BYTES_PER_LONG and which also must
   *          be a multiple of BYTES_PER_LONG
   */
  DataVerifier(int bufferSize) {
    if (bufferSize < BYTES_PER_LONG) {
      throw new IllegalArgumentException(
          "Buffer size must be greater than or equal to " + BYTES_PER_LONG);
    }
    if ((bufferSize % BYTES_PER_LONG) != 0) {
      throw new IllegalArgumentException("Buffer size must be a multiple of "
          + BYTES_PER_LONG);
    }
    this.bufferSize = bufferSize;
  }

  /**
   * Inits with the default buffer size
   */
  DataVerifier() {
    this(Constants.BUFFERSIZE);
  }

  /**
   * Verifies a buffer of a given size using the given start hash offset
   * 
   * @param buf
   *          the buffer to verify
   * @param size
   *          the number of bytes to be used in that buffer
   * @param startOffset
   *          the start hash offset
   * @param hasher
   *          the hasher to use for calculating expected values
   * 
   * @return ResumeBytes a set of data about the next offset and chunks analyzed
   */
  private VerifyInfo verifyBuffer(ByteBuffer buf, int size, long startOffset,
      DataHasher hasher) {
    ByteBuffer cmpBuf = ByteBuffer.wrap(new byte[BYTES_PER_LONG]);
    long hashOffset = startOffset;
    long chunksSame = 0;
    long chunksDifferent = 0;
    for (long i = 0; i < size; ++i) {
      cmpBuf.put(buf.get());
      if (!cmpBuf.hasRemaining()) {
        cmpBuf.rewind();
        long receivedData = cmpBuf.getLong();
        cmpBuf.rewind();
        long expected = hasher.generate(hashOffset);
        hashOffset += BYTES_PER_LONG;
        if (receivedData == expected) {
          ++chunksSame;
        } else {
          ++chunksDifferent;
        }
      }
    }
    // any left over??
    if (cmpBuf.hasRemaining() && cmpBuf.position() != 0) {
      // partial capture
      // zero fill and compare with zero filled
      int curSize = cmpBuf.position();
      while (cmpBuf.hasRemaining()) {
        cmpBuf.put((byte) 0);
      }
      long expected = hasher.generate(hashOffset);
      ByteBuffer tempBuf = ByteBuffer.wrap(new byte[BYTES_PER_LONG]);
      tempBuf.putLong(expected);
      tempBuf.position(curSize);
      while (tempBuf.hasRemaining()) {
        tempBuf.put((byte) 0);
      }
      cmpBuf.rewind();
      tempBuf.rewind();
      if (cmpBuf.equals(tempBuf)) {
        ++chunksSame;
      } else {
        ++chunksDifferent;
      }
    }
    return new VerifyInfo(chunksSame, chunksDifferent);
  }

  /**
   * Determines the offset to use given a byte counter
   * 
   * @param byteRead
   * 
   * @return offset position
   */
  private long determineOffset(long byteRead) {
    if (byteRead < 0) {
      byteRead = 0;
    }
    return (byteRead / BYTES_PER_LONG) * BYTES_PER_LONG;
  }

  /**
   * Verifies a given number of bytes from a file - less number of bytes may be
   * read if a header can not be read in due to the byte limit
   * 
   * @param byteAm
   *          the byte amount to limit to (should be less than or equal to file
   *          size)
   * 
   * @param in
   *          the input stream to read from
   * 
   * @return VerifyOutput with data about reads
   * 
   * @throws IOException
   *           if a read failure occurs
   * 
   * @throws BadFileException
   *           if a header can not be read or end of file is reached
   *           unexpectedly
   */
  VerifyOutput verifyFile(long byteAm, DataInputStream in)
      throws IOException, BadFileException {
    return verifyBytes(byteAm, 0, in);
  }

  /**
   * Verifies a given number of bytes from a file - less number of bytes may be
   * read if a header can not be read in due to the byte limit
   * 
   * @param byteAm
   *          the byte amount to limit to (should be less than or equal to file
   *          size)
   * 
   * @param bytesRead
   *          the starting byte location
   * 
   * @param in
   *          the input stream to read from
   * 
   * @return VerifyOutput with data about reads
   * 
   * @throws IOException
   *           if a read failure occurs
   * 
   * @throws BadFileException
   *           if a header can not be read or end of file is reached
   *           unexpectedly
   */
  private VerifyOutput verifyBytes(long byteAm, long bytesRead,
      DataInputStream in) throws IOException, BadFileException {
    if (byteAm <= 0) {
      return new VerifyOutput(0, 0, 0, 0);
    }
    long chunksSame = 0;
    long chunksDifferent = 0;
    long readTime = 0;
    long bytesLeft = byteAm;
    long bufLeft = 0;
    long bufRead = 0;
    long seqNum = 0;
    DataHasher hasher = null;
    ByteBuffer readBuf = ByteBuffer.wrap(new byte[bufferSize]);
    while (bytesLeft > 0) {
      if (bufLeft <= 0) {
        if (bytesLeft < DataWriter.getHeaderLength()) {
          // no bytes left to read a header
          break;
        }
        // time to read a new header
        ReadInfo header = null;
        try {
          header = readHeader(in);
        } catch (EOFException e) {
          // eof ok on header reads
          // but not on data readers
          break;
        }
        ++seqNum;
        hasher = new DataHasher(header.getHashValue());
        bufLeft = header.getByteAm();
        readTime += header.getTimeTaken();
        bytesRead += header.getBytesRead();
        bytesLeft -= header.getBytesRead();
        bufRead = 0;
        // number of bytes to read greater than how many we want to read
        if (bufLeft > bytesLeft) {
          bufLeft = bytesLeft;
        }
        // does the buffer amount have anything??
        if (bufLeft <= 0) {
          continue;
        }
      }
      // figure out the buffer size to read
      int bufSize = bufferSize;
      if (bytesLeft < bufSize) {
        bufSize = (int) bytesLeft;
      }
      if (bufLeft < bufSize) {
        bufSize = (int) bufLeft;
      }
      // read it in
      try {
        readBuf.rewind();
        long startTime = Timer.now();
        in.readFully(readBuf.array(), 0, bufSize);
        readTime += Timer.elapsed(startTime);
      } catch (EOFException e) {
        throw new BadFileException(
            "Could not read the number of expected data bytes " + bufSize
                + " due to unexpected end of file during sequence " + seqNum, e);
      }
      // update the counters
      bytesRead += bufSize;
      bytesLeft -= bufSize;
      bufLeft -= bufSize;
      // verify what we read
      readBuf.rewind();
      // figure out the expected hash offset start point
      long vOffset = determineOffset(bufRead);
      // now update for new position
      bufRead += bufSize;
      // verify
      VerifyInfo verifyRes = verifyBuffer(readBuf, bufSize, vOffset, hasher);
      // update the verification counters
      chunksSame += verifyRes.getSame();
      chunksDifferent += verifyRes.getDifferent();
    }
    return new VerifyOutput(chunksSame, chunksDifferent, bytesRead, readTime);
  }


  /**
   * Reads a header from the given input stream
   * 
   * @param in
   *          input stream to read from
   * 
   * @return ReadInfo
   * 
   * @throws IOException
   *           if a read error occurs or EOF occurs
   * 
   * @throws BadFileException
   *           if end of file occurs or the byte amount read is invalid
   */
  ReadInfo readHeader(DataInputStream in) throws IOException,
      BadFileException {
    int headerLen = DataWriter.getHeaderLength();
    ByteBuffer headerBuf = ByteBuffer.wrap(new byte[headerLen]);
    long elapsed = 0;
    {
      long startTime = Timer.now();
      in.readFully(headerBuf.array());
      elapsed += Timer.elapsed(startTime);
    }
    headerBuf.rewind();
    long hashValue = headerBuf.getLong();
    long byteAvailable = headerBuf.getLong();
    if (byteAvailable < 0) {
      throw new BadFileException("Invalid negative amount " + byteAvailable
          + " determined for header data amount");
    }
    return new ReadInfo(byteAvailable, hashValue, elapsed, headerLen);
  }
}

/*
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

package org.apache.hadoop.io.compress.zlib;

import java.io.IOException;
import java.util.zip.Checksum;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DoNotPool;
import org.apache.hadoop.util.DataChecksum;

/**
 * A {@link Decompressor} based on the popular gzip compressed file format.
 * http://www.gzip.org/
 *
 */
@DoNotPool
public class BuiltInGzipDecompressor implements Decompressor {
  private static final int GZIP_MAGIC_ID = 0x8b1f;  // if read as LE short int
  private static final int GZIP_DEFLATE_METHOD = 8;
  private static final int GZIP_FLAGBIT_HEADER_CRC  = 0x02;
  private static final int GZIP_FLAGBIT_EXTRA_FIELD = 0x04;
  private static final int GZIP_FLAGBIT_FILENAME    = 0x08;
  private static final int GZIP_FLAGBIT_COMMENT     = 0x10;
  private static final int GZIP_FLAGBITS_RESERVED   = 0xe0;

  // 'true' (nowrap) => Inflater will handle raw deflate stream only
  private Inflater inflater = new Inflater(true);

  private byte[] userBuf = null;
  private int userBufOff = 0;
  private int userBufLen = 0;

  private byte[] localBuf = new byte[256];
  private int localBufOff = 0;

  private int headerBytesRead = 0;
  private int trailerBytesRead = 0;
  private int numExtraFieldBytesRemaining = -1;
  private Checksum crc = DataChecksum.newCrc32();
  private boolean hasExtraField = false;
  private boolean hasFilename = false;
  private boolean hasComment = false;
  private boolean hasHeaderCRC = false;

  private GzipStateLabel state;

  /**
   * The current state of the gzip decoder, external to the Inflater context.
   * (Technically, the private variables localBuf through hasHeaderCRC are
   * also part of the state, so this enum is merely the label for it.)
   */
  private static enum GzipStateLabel {
    /**
     * Immediately prior to or (strictly) within the 10-byte basic gzip header.
     */
    HEADER_BASIC,
    /**
     * Immediately prior to or within the optional "extra field."
     */
    HEADER_EXTRA_FIELD,
    /**
     * Immediately prior to or within the optional filename field.
     */
    HEADER_FILENAME,
    /**
     * Immediately prior to or within the optional comment field.
     */
    HEADER_COMMENT,
    /**
     * Immediately prior to or within the optional 2-byte header CRC value.
     */
    HEADER_CRC,
    /**
     * Immediately prior to or within the main compressed (deflate) data stream.
     */
    DEFLATE_STREAM,
    /**
     * Immediately prior to or (strictly) within the 4-byte uncompressed CRC.
     */
    TRAILER_CRC,
    /**
     * Immediately prior to or (strictly) within the 4-byte uncompressed size.
     */
    TRAILER_SIZE,
    /**
     * Immediately after the trailer (and potentially prior to the next gzip
     * member/substream header), without reset() having been called.
     */
    FINISHED;
  }

  /**
   * Creates a new (pure Java) gzip decompressor.
   */
  public BuiltInGzipDecompressor() {
    state = GzipStateLabel.HEADER_BASIC;
    crc.reset();
    // FIXME? Inflater docs say:  'it is also necessary to provide an extra
    //        "dummy" byte as input. This is required by the ZLIB native
    //        library in order to support certain optimizations.'  However,
    //        this does not appear to be true, and in any case, it's not
    //        entirely clear where the byte should go or what its value
    //        should be.  Perhaps it suffices to have some deflated bytes
    //        in the first buffer load?  (But how else would one do it?)
  }

  @Override
  public synchronized boolean needsInput() {
    if (state == GzipStateLabel.DEFLATE_STREAM) {  // most common case
      return inflater.needsInput();
    }
    // see userBufLen comment at top of decompress(); currently no need to
    // verify userBufLen <= 0
    return (state != GzipStateLabel.FINISHED);
  }

  /** {@inheritDoc} */
  /*
   * In our case, the input data includes both gzip header/trailer bytes (which
   * we handle in executeState()) and deflate-stream bytes (which we hand off
   * to Inflater).
   *
   * NOTE:  This code assumes the data passed in via b[] remains unmodified
   *        until _we_ signal that it's safe to modify it (via needsInput()).
   *        The alternative would require an additional buffer-copy even for
   *        the bulk deflate stream, which is a performance hit we don't want
   *        to absorb.  (Decompressor now documents this requirement.)
   */
  @Override
  public synchronized void setInput(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    userBuf = b;
    userBufOff = off;
    userBufLen = len;  // note:  might be zero
  }

  /**
   * Decompress the data (gzip header, deflate stream, gzip trailer) in the
   * provided buffer.
   *
   * @return the number of decompressed bytes placed into b
   */
  /* From the caller's perspective, this is where the state machine lives.
   * The code is written such that we never return from decompress() with
   * data remaining in userBuf unless we're in FINISHED state and there was
   * data beyond the current gzip member (e.g., we're within a concatenated
   * gzip stream).  If this ever changes, {@link #needsInput()} will also
   * need to be modified (i.e., uncomment the userBufLen condition).
   *
   * The actual deflate-stream processing (decompression) is handled by
   * Java's Inflater class.  Unlike the gzip header/trailer code (execute*
   * methods below), the deflate stream is never copied; Inflater operates
   * directly on the user's buffer.
   */
  @Override
  public synchronized int decompress(byte[] b, int off, int len)
  throws IOException {
    int numAvailBytes = 0;

    if (state != GzipStateLabel.DEFLATE_STREAM) {
      executeHeaderState();

      if (userBufLen <= 0) {
        return numAvailBytes;
      }
    }

    // "executeDeflateStreamState()"
    if (state == GzipStateLabel.DEFLATE_STREAM) {
      // hand off user data (or what's left of it) to Inflater--but note that
      // Inflater may not have consumed all of previous bufferload (e.g., if
      // data highly compressed or output buffer very small), in which case
      // userBufLen will be zero
      if (userBufLen > 0) {
        inflater.setInput(userBuf, userBufOff, userBufLen);
        userBufOff += userBufLen;
        userBufLen = 0;
      }

      // now decompress it into b[]
      try {
        numAvailBytes = inflater.inflate(b, off, len);
      } catch (DataFormatException dfe) {
        throw new IOException(dfe.getMessage());
      }
      crc.update(b, off, numAvailBytes);  // CRC-32 is on _uncompressed_ data
      if (inflater.finished()) {
        state = GzipStateLabel.TRAILER_CRC;
        int bytesRemaining = inflater.getRemaining();
        assert (bytesRemaining >= 0) :
          "logic error: Inflater finished; byte-count is inconsistent";
          // could save a copy of userBufLen at call to inflater.setInput() and
          // verify that bytesRemaining <= origUserBufLen, but would have to
          // be a (class) member variable...seems excessive for a sanity check
        userBufOff -= bytesRemaining;
        userBufLen = bytesRemaining;   // or "+=", but guaranteed 0 coming in
      } else {
        return numAvailBytes;  // minor optimization
      }
    }

    executeTrailerState();

    return numAvailBytes;
  }

  /**
   * Parse the gzip header (assuming we're in the appropriate state).
   * In order to deal with degenerate cases (e.g., user buffer is one byte
   * long), we copy (some) header bytes to another buffer.  (Filename,
   * comment, and extra-field bytes are simply skipped.)</p>
   *
   * See http://www.ietf.org/rfc/rfc1952.txt for the gzip spec.  Note that
   * no version of gzip to date (at least through 1.4.0, 2010-01-20) supports
   * the FHCRC header-CRC16 flagbit; instead, the implementation treats it
   * as a multi-file continuation flag (which it also doesn't support). :-(
   * Sun's JDK v6 (1.6) supports the header CRC, however, and so do we.
   */
  private void executeHeaderState() throws IOException {

    // this can happen because DecompressorStream's decompress() is written
    // to call decompress() first, setInput() second:
    if (userBufLen <= 0) {
      return;
    }

    // "basic"/required header:  somewhere in first 10 bytes
    if (state == GzipStateLabel.HEADER_BASIC) {
      int n = Math.min(userBufLen, 10-localBufOff);  // (or 10-headerBytesRead)
      checkAndCopyBytesToLocal(n);  // modifies userBufLen, etc.
      if (localBufOff >= 10) {      // should be strictly ==
        processBasicHeader();       // sig, compression method, flagbits
        localBufOff = 0;            // no further need for basic header
        state = GzipStateLabel.HEADER_EXTRA_FIELD;
      }
    }

    if (userBufLen <= 0) {
      return;
    }

    // optional header stuff (extra field, filename, comment, header CRC)

    if (state == GzipStateLabel.HEADER_EXTRA_FIELD) {
      if (hasExtraField) {
        // 2 substates:  waiting for 2 bytes => get numExtraFieldBytesRemaining,
        // or already have 2 bytes & waiting to finish skipping specified length
        if (numExtraFieldBytesRemaining < 0) {
          int n = Math.min(userBufLen, 2-localBufOff);
          checkAndCopyBytesToLocal(n);
          if (localBufOff >= 2) {
            numExtraFieldBytesRemaining = readUShortLE(localBuf, 0);
            localBufOff = 0;
          }
        }
        if (numExtraFieldBytesRemaining > 0 && userBufLen > 0) {
          int n = Math.min(userBufLen, numExtraFieldBytesRemaining);
          checkAndSkipBytes(n);     // modifies userBufLen, etc.
          numExtraFieldBytesRemaining -= n;
        }
        if (numExtraFieldBytesRemaining == 0) {
          state = GzipStateLabel.HEADER_FILENAME;
        }
      } else {
        state = GzipStateLabel.HEADER_FILENAME;
      }
    }

    if (userBufLen <= 0) {
      return;
    }

    if (state == GzipStateLabel.HEADER_FILENAME) {
      if (hasFilename) {
        boolean doneWithFilename = checkAndSkipBytesUntilNull();
        if (!doneWithFilename) {
          return;  // exit early:  used up entire buffer without hitting NULL
        }
      }
      state = GzipStateLabel.HEADER_COMMENT;
    }

    if (userBufLen <= 0) {
      return;
    }

    if (state == GzipStateLabel.HEADER_COMMENT) {
      if (hasComment) {
        boolean doneWithComment = checkAndSkipBytesUntilNull();
        if (!doneWithComment) {
          return;  // exit early:  used up entire buffer
        }
      }
      state = GzipStateLabel.HEADER_CRC;
    }

    if (userBufLen <= 0) {
      return;
    }

    if (state == GzipStateLabel.HEADER_CRC) {
      if (hasHeaderCRC) {
        assert (localBufOff < 2);
        int n = Math.min(userBufLen, 2-localBufOff);
        copyBytesToLocal(n);
        if (localBufOff >= 2) {
          long headerCRC = readUShortLE(localBuf, 0);
          if (headerCRC != (crc.getValue() & 0xffff)) {
            throw new IOException("gzip header CRC failure");
          }
          localBufOff = 0;
          crc.reset();
          state = GzipStateLabel.DEFLATE_STREAM;
        }
      } else {
        crc.reset();   // will reuse for CRC-32 of uncompressed data
        state = GzipStateLabel.DEFLATE_STREAM;  // switching to Inflater now
      }
    }
  }

  /**
   * Parse the gzip trailer (assuming we're in the appropriate state).
   * In order to deal with degenerate cases (e.g., user buffer is one byte
   * long), we copy trailer bytes (all 8 of 'em) to a local buffer.</p>
   *
   * See http://www.ietf.org/rfc/rfc1952.txt for the gzip spec.
   */
  private void executeTrailerState() throws IOException {

    if (userBufLen <= 0) {
      return;
    }

    // verify that the CRC-32 of the decompressed stream matches the value
    // stored in the gzip trailer
    if (state == GzipStateLabel.TRAILER_CRC) {
      // localBuf was empty before we handed off to Inflater, so we handle this
      // exactly like header fields
      assert (localBufOff < 4);  // initially 0, but may need multiple calls
      int n = Math.min(userBufLen, 4-localBufOff);
      copyBytesToLocal(n);
      if (localBufOff >= 4) {
        long streamCRC = readUIntLE(localBuf, 0);
        if (streamCRC != crc.getValue()) {
          throw new IOException("gzip stream CRC failure");
        }
        localBufOff = 0;
        crc.reset();
        state = GzipStateLabel.TRAILER_SIZE;
      }
    }

    if (userBufLen <= 0) {
      return;
    }

    // verify that the mod-2^32 decompressed stream size matches the value
    // stored in the gzip trailer
    if (state == GzipStateLabel.TRAILER_SIZE) {
      assert (localBufOff < 4);  // initially 0, but may need multiple calls
      int n = Math.min(userBufLen, 4-localBufOff);
      copyBytesToLocal(n);       // modifies userBufLen, etc.
      if (localBufOff >= 4) {    // should be strictly ==
        long inputSize = readUIntLE(localBuf, 0);
        if (inputSize != (inflater.getBytesWritten() & 0xffffffffL)) {
          throw new IOException(
            "stored gzip size doesn't match decompressed size");
        }
        localBufOff = 0;
        state = GzipStateLabel.FINISHED;
      }
    }

    if (state == GzipStateLabel.FINISHED) {
      return;
    }
  }

  /**
   * Returns the total number of compressed bytes input so far, including
   * gzip header/trailer bytes.</p>
   *
   * @return the total (non-negative) number of compressed bytes read so far
   */
  public synchronized long getBytesRead() {
    return headerBytesRead + inflater.getBytesRead() + trailerBytesRead;
  }

  /**
   * Returns the number of bytes remaining in the input buffer; normally
   * called when finished() is true to determine amount of post-gzip-stream
   * data.  Note that, other than the finished state with concatenated data
   * after the end of the current gzip stream, this will never return a
   * non-zero value unless called after {@link #setInput(byte[] b, int off,
   * int len)} and before {@link #decompress(byte[] b, int off, int len)}.
   * (That is, after {@link #decompress(byte[] b, int off, int len)} it
   * always returns zero, except in finished state with concatenated data.)</p>
   *
   * @return the total (non-negative) number of unprocessed bytes in input
   */
  @Override
  public synchronized int getRemaining() {
    return userBufLen;
  }

  @Override
  public synchronized boolean needsDictionary() {
    return inflater.needsDictionary();
  }

  @Override
  public synchronized void setDictionary(byte[] b, int off, int len) {
    inflater.setDictionary(b, off, len);
  }

  /**
   * Returns true if the end of the gzip substream (single "member") has been
   * reached.</p>
   */
  @Override
  public synchronized boolean finished() {
    return (state == GzipStateLabel.FINISHED);
  }

  /**
   * Resets everything, including the input buffer, regardless of whether the
   * current gzip substream is finished.</p>
   */
  @Override
  public synchronized void reset() {
    // could optionally emit INFO message if state != GzipStateLabel.FINISHED
    inflater.reset();
    state = GzipStateLabel.HEADER_BASIC;
    crc.reset();
    userBufOff = userBufLen = 0;
    localBufOff = 0;
    headerBytesRead = 0;
    trailerBytesRead = 0;
    numExtraFieldBytesRemaining = -1;
    hasExtraField = false;
    hasFilename = false;
    hasComment = false;
    hasHeaderCRC = false;
  }

  @Override
  public synchronized void end() {
    inflater.end();
  }

  /**
   * Check ID bytes (throw if necessary), compression method (throw if not 8),
   * and flag bits (set hasExtraField, hasFilename, hasComment, hasHeaderCRC).
   * Ignore MTIME, XFL, OS.  Caller must ensure we have at least 10 bytes (at
   * the start of localBuf).</p>
   */
  /*
   * Flag bits (remainder are reserved and must be zero):
   *   bit 0   FTEXT
   *   bit 1   FHCRC   (never implemented in gzip, at least through version
   *                   1.4.0; instead interpreted as "continuation of multi-
   *                   part gzip file," which is unsupported through 1.4.0)
   *   bit 2   FEXTRA
   *   bit 3   FNAME
   *   bit 4   FCOMMENT
   *  [bit 5   encrypted]
   */
  private void processBasicHeader() throws IOException {
    if (readUShortLE(localBuf, 0) != GZIP_MAGIC_ID) {
      throw new IOException("not a gzip file");
    }
    if (readUByte(localBuf, 2) != GZIP_DEFLATE_METHOD) {
      throw new IOException("gzip data not compressed with deflate method");
    }
    int flg = readUByte(localBuf, 3);
    if ((flg & GZIP_FLAGBITS_RESERVED) != 0) {
      throw new IOException("unknown gzip format (reserved flagbits set)");
    }
    hasExtraField = ((flg & GZIP_FLAGBIT_EXTRA_FIELD) != 0);
    hasFilename   = ((flg & GZIP_FLAGBIT_FILENAME)    != 0);
    hasComment    = ((flg & GZIP_FLAGBIT_COMMENT)     != 0);
    hasHeaderCRC  = ((flg & GZIP_FLAGBIT_HEADER_CRC)  != 0);
  }

  private void checkAndCopyBytesToLocal(int len) {
    System.arraycopy(userBuf, userBufOff, localBuf, localBufOff, len);
    localBufOff += len;
    // alternatively, could call checkAndSkipBytes(len) for rest...
    crc.update(userBuf, userBufOff, len);
    userBufOff += len;
    userBufLen -= len;
    headerBytesRead += len;
  }

  private void checkAndSkipBytes(int len) {
    crc.update(userBuf, userBufOff, len);
    userBufOff += len;
    userBufLen -= len;
    headerBytesRead += len;
  }

  // returns true if saw NULL, false if ran out of buffer first; called _only_
  // during gzip-header processing (not trailer)
  // (caller can check before/after state of userBufLen to compute num bytes)
  private boolean checkAndSkipBytesUntilNull() {
    boolean hitNull = false;
    if (userBufLen > 0) {
      do {
        hitNull = (userBuf[userBufOff] == 0);
        crc.update(userBuf[userBufOff]);
        ++userBufOff;
        --userBufLen;
        ++headerBytesRead;
      } while (userBufLen > 0 && !hitNull);
    }
    return hitNull;
  }

  // this one doesn't update the CRC and does support trailer processing but
  // otherwise is same as its "checkAnd" sibling
  private void copyBytesToLocal(int len) {
    System.arraycopy(userBuf, userBufOff, localBuf, localBufOff, len);
    localBufOff += len;
    userBufOff += len;
    userBufLen -= len;
    if (state == GzipStateLabel.TRAILER_CRC ||
        state == GzipStateLabel.TRAILER_SIZE) {
      trailerBytesRead += len;
    } else {
      headerBytesRead += len;
    }
  }

  private int readUByte(byte[] b, int off) {
    return ((int)b[off] & 0xff);
  }

  // caller is responsible for not overrunning buffer
  private int readUShortLE(byte[] b, int off) {
    return ((((b[off+1] & 0xff) << 8) |
             ((b[off]   & 0xff)     )) & 0xffff);
  }

  // caller is responsible for not overrunning buffer
  private long readUIntLE(byte[] b, int off) {
    return ((((long)(b[off+3] & 0xff) << 24) |
             ((long)(b[off+2] & 0xff) << 16) |
             ((long)(b[off+1] & 0xff) <<  8) |
             ((long)(b[off]   & 0xff)      )) & 0xffffffffL);
  }

}

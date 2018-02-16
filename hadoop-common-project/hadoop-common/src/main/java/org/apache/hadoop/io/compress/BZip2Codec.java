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

package org.apache.hadoop.io.compress;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.io.Charsets;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.bzip2.BZip2Constants;
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.apache.hadoop.io.compress.bzip2.CBZip2OutputStream;
import org.apache.hadoop.io.compress.bzip2.Bzip2Factory;

/**
 * This class provides output and input streams for bzip2 compression
 * and decompression.  It uses the native bzip2 library on the system
 * if possible, else it uses a pure-Java implementation of the bzip2
 * algorithm.  The configuration parameter
 * io.compression.codec.bzip2.library can be used to control this
 * behavior.
 *
 * In the pure-Java mode, the Compressor and Decompressor interfaces
 * are not implemented.  Therefore, in that mode, those methods of
 * CompressionCodec which have a Compressor or Decompressor type
 * argument, throw UnsupportedOperationException.
 *
 * Currently, support for splittability is available only in the
 * pure-Java mode; therefore, if a SplitCompressionInputStream is
 * requested, the pure-Java implementation is used, regardless of the
 * setting of the configuration parameter mentioned above.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class BZip2Codec implements Configurable, SplittableCompressionCodec {

  private static final String HEADER = "BZ";
  private static final int HEADER_LEN = HEADER.length();
  private static final String SUB_HEADER = "h9";
  private static final int SUB_HEADER_LEN = SUB_HEADER.length();

  private Configuration conf;
  
  /**
   * Set the configuration to be used by this object.
   *
   * @param conf the configuration object.
   */
  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }
  
  /**
   * Return the configuration used by this object.
   *
   * @return the configuration object used by this objec.
   */
  @Override
  public Configuration getConf() {
    return conf;
  }
  
  /**
  * Creates a new instance of BZip2Codec.
  */
  public BZip2Codec() { }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream}.
   *
   * @param out        the location for the final output stream
   * @return a stream the user can write uncompressed data to, to have it 
   *         compressed
   * @throws IOException
   */
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out)
      throws IOException {
    return CompressionCodec.Util.
        createOutputStreamWithCodecPool(this, conf, out);
  }

  /**
   * Create a {@link CompressionOutputStream} that will write to the given
   * {@link OutputStream} with the given {@link Compressor}.
   *
   * @param out        the location for the final output stream
   * @param compressor compressor to use
   * @return a stream the user can write uncompressed data to, to have it 
   *         compressed
   * @throws IOException
   */
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out,
      Compressor compressor) throws IOException {
    return Bzip2Factory.isNativeBzip2Loaded(conf) ?
      new CompressorStream(out, compressor, 
                           conf.getInt("io.file.buffer.size", 4*1024)) :
      new BZip2CompressionOutputStream(out);
  }

  /**
   * Get the type of {@link Compressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of compressor needed by this codec.
   */
  @Override
  public Class<? extends Compressor> getCompressorType() {
    return Bzip2Factory.getBzip2CompressorType(conf);
  }

  /**
   * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
   *
   * @return a new compressor for use by this codec
   */
  @Override
  public Compressor createCompressor() {
    return Bzip2Factory.getBzip2Compressor(conf);
  }

  /**
   * Create a {@link CompressionInputStream} that will read from the given
   * input stream and return a stream for uncompressed data.
   *
   * @param in the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  @Override
  public CompressionInputStream createInputStream(InputStream in)
      throws IOException {
    return CompressionCodec.Util.
        createInputStreamWithCodecPool(this, conf, in);
  }

  /**
   * Create a {@link CompressionInputStream} that will read from the given
   * {@link InputStream} with the given {@link Decompressor}, and return a 
   * stream for uncompressed data.
   *
   * @param in           the stream to read compressed bytes from
   * @param decompressor decompressor to use
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  @Override
  public CompressionInputStream createInputStream(InputStream in,
      Decompressor decompressor) throws IOException {
    return Bzip2Factory.isNativeBzip2Loaded(conf) ? 
      new DecompressorStream(in, decompressor,
                             conf.getInt("io.file.buffer.size", 4*1024)) :
      new BZip2CompressionInputStream(in);
  }

  /**
   * Creates CompressionInputStream to be used to read off uncompressed data
   * in one of the two reading modes. i.e. Continuous or Blocked reading modes
   *
   * @param seekableIn The InputStream
   * @param start The start offset into the compressed stream
   * @param end The end offset into the compressed stream
   * @param readMode Controls whether progress is reported continuously or
   *                 only at block boundaries.
   *
   * @return CompressionInputStream for BZip2 aligned at block boundaries
   */
  public SplitCompressionInputStream createInputStream(InputStream seekableIn,
      Decompressor decompressor, long start, long end, READ_MODE readMode)
      throws IOException {

    if (!(seekableIn instanceof Seekable)) {
      throw new IOException("seekableIn must be an instance of " +
          Seekable.class.getName());
    }

    ((Seekable)seekableIn).seek(start);
    return new BZip2CompressionInputStream(seekableIn, start, end, readMode);
  }

  /**
   * Get the type of {@link Decompressor} needed by this {@link CompressionCodec}.
   *
   * @return the type of decompressor needed by this codec.
   */
  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return Bzip2Factory.getBzip2DecompressorType(conf);
  }

  /**
   * Create a new {@link Decompressor} for use by this {@link CompressionCodec}.
   *
   * @return a new decompressor for use by this codec
   */
  @Override
  public Decompressor createDecompressor() {
    return Bzip2Factory.getBzip2Decompressor(conf);
  }

  /**
  * .bz2 is recognized as the default extension for compressed BZip2 files
  *
  * @return A String telling the default bzip2 file extension
  */
  @Override
  public String getDefaultExtension() {
    return ".bz2";
  }

  private static class BZip2CompressionOutputStream extends
      CompressionOutputStream {

    // class data starts here//
    private CBZip2OutputStream output;
    private boolean needsReset; 
    // class data ends here//

    public BZip2CompressionOutputStream(OutputStream out)
        throws IOException {
      super(out);
      needsReset = true;
    }

    private void writeStreamHeader() throws IOException {
      if (super.out != null) {
        // The compressed bzip2 stream should start with the
        // identifying characters BZ. Caller of CBZip2OutputStream
        // i.e. this class must write these characters.
        out.write(HEADER.getBytes(Charsets.UTF_8));
      }
    }

    public void finish() throws IOException {
      if (needsReset) {
        // In the case that nothing is written to this stream, we still need to
        // write out the header before closing, otherwise the stream won't be
        // recognized by BZip2CompressionInputStream.
        internalReset();
      }
      this.output.finish();
      needsReset = true;
    }

    private void internalReset() throws IOException {
      if (needsReset) {
        needsReset = false;
        writeStreamHeader();
        this.output = new CBZip2OutputStream(out);
      }
    }    
    
    public void resetState() throws IOException {
      // Cannot write to out at this point because out might not be ready
      // yet, as in SequenceFile.Writer implementation.
      needsReset = true;
    }

    public void write(int b) throws IOException {
      if (needsReset) {
        internalReset();
      }
      this.output.write(b);
    }

    public void write(byte[] b, int off, int len) throws IOException {
      if (needsReset) {
        internalReset();
      }
      this.output.write(b, off, len);
    }

    public void close() throws IOException {
      try {
        super.close();
      } finally {
        output.close();
      }
    }

  }// end of class BZip2CompressionOutputStream

  /**
   * This class is capable to de-compress BZip2 data in two modes;
   * CONTINOUS and BYBLOCK.  BYBLOCK mode makes it possible to
   * do decompression starting any arbitrary position in the stream.
   *
   * So this facility can easily be used to parallelize decompression
   * of a large BZip2 file for performance reasons.  (It is exactly
   * done so for Hadoop framework.  See LineRecordReader for an
   * example).  So one can break the file (of course logically) into
   * chunks for parallel processing.  These "splits" should be like
   * default Hadoop splits (e.g as in FileInputFormat getSplit metod).
   * So this code is designed and tested for FileInputFormat's way
   * of splitting only.
   */

  private static class BZip2CompressionInputStream extends
      SplitCompressionInputStream {

    // class data starts here//
    private CBZip2InputStream input;
    boolean needsReset;
    private BufferedInputStream bufferedIn;
    private boolean isHeaderStripped = false;
    private boolean isSubHeaderStripped = false;
    private READ_MODE readMode = READ_MODE.CONTINUOUS;
    private long startingPos = 0L;

    // Following state machine handles different states of compressed stream
    // position
    // HOLD : Don't advertise compressed stream position
    // ADVERTISE : Read 1 more character and advertise stream position
    // See more comments about it before updatePos method.
    private enum POS_ADVERTISEMENT_STATE_MACHINE {
      HOLD, ADVERTISE
    };

    POS_ADVERTISEMENT_STATE_MACHINE posSM = POS_ADVERTISEMENT_STATE_MACHINE.HOLD;
    long compressedStreamPosition = 0;

    // class data ends here//

    public BZip2CompressionInputStream(InputStream in) throws IOException {
      this(in, 0L, Long.MAX_VALUE, READ_MODE.CONTINUOUS);
    }

    public BZip2CompressionInputStream(InputStream in, long start, long end,
        READ_MODE readMode) throws IOException {
      super(in, start, end);
      needsReset = false;
      bufferedIn = new BufferedInputStream(super.in);
      this.startingPos = super.getPos();
      this.readMode = readMode;
      long numSkipped = 0;
      if (this.startingPos == 0) {
        // We only strip header if it is start of file
        bufferedIn = readStreamHeader();
      } else if (this.readMode == READ_MODE.BYBLOCK  &&
          this.startingPos <= HEADER_LEN + SUB_HEADER_LEN) {
        // When we're in BYBLOCK mode and the start position is >=0
        // and < HEADER_LEN + SUB_HEADER_LEN, we should skip to after
        // start of the first bz2 block to avoid duplicated records
        numSkipped = HEADER_LEN + SUB_HEADER_LEN + 1 - this.startingPos;
        long skipBytes = numSkipped;
        while (skipBytes > 0) {
          long s = bufferedIn.skip(skipBytes);
          if (s > 0) {
            skipBytes -= s;
          } else {
            if (bufferedIn.read() == -1) {
              break; // end of the split
            } else {
              skipBytes--;
            }
          }
        }
      }
      input = new CBZip2InputStream(bufferedIn, readMode);
      if (this.isHeaderStripped) {
        input.updateReportedByteCount(HEADER_LEN);
      }

      if (this.isSubHeaderStripped) {
        input.updateReportedByteCount(SUB_HEADER_LEN);
      }

      if (numSkipped > 0) {
        input.updateReportedByteCount((int) numSkipped);
      }

      // To avoid dropped records, not advertising a new byte position
      // when we are in BYBLOCK mode and the start position is 0
      if (!(this.readMode == READ_MODE.BYBLOCK && this.startingPos == 0)) {
        this.updatePos(false);
      }
    }

    private BufferedInputStream readStreamHeader() throws IOException {
      // We are flexible enough to allow the compressed stream not to
      // start with the header of BZ. So it works fine either we have
      // the header or not.
      if (super.in != null) {
        bufferedIn.mark(HEADER_LEN);
        byte[] headerBytes = new byte[HEADER_LEN];
        int actualRead = bufferedIn.read(headerBytes, 0, HEADER_LEN);
        if (actualRead != -1) {
          String header = new String(headerBytes, Charsets.UTF_8);
          if (header.compareTo(HEADER) != 0) {
            bufferedIn.reset();
          } else {
            this.isHeaderStripped = true;
            // In case of BYBLOCK mode, we also want to strip off
            // remaining two character of the header.
            if (this.readMode == READ_MODE.BYBLOCK) {
              actualRead = bufferedIn.read(headerBytes, 0,
                  SUB_HEADER_LEN);
              if (actualRead != -1) {
                this.isSubHeaderStripped = true;
              }
            }
          }
        }
      }

      if (bufferedIn == null) {
        throw new IOException("Failed to read bzip2 stream.");
      }

      return bufferedIn;

    }// end of method

    public void close() throws IOException {
      if (!needsReset) {
        try {
          input.close();
          needsReset = true;
        } finally {
          super.close();
        }
      }
    }

    /**
    * This method updates compressed stream position exactly when the
    * client of this code has read off at least one byte passed any BZip2
    * end of block marker.
    *
    * This mechanism is very helpful to deal with data level record
    * boundaries. Please see constructor and next methods of
    * org.apache.hadoop.mapred.LineRecordReader as an example usage of this
    * feature.  We elaborate it with an example in the following:
    *
    * Assume two different scenarios of the BZip2 compressed stream, where
    * [m] represent end of block, \n is line delimiter and . represent compressed
    * data.
    *
    * ............[m]......\n.......
    *
    * ..........\n[m]......\n.......
    *
    * Assume that end is right after [m].  In the first case the reading
    * will stop at \n and there is no need to read one more line.  (To see the
    * reason of reading one more line in the next() method is explained in LineRecordReader.)
    * While in the second example LineRecordReader needs to read one more line
    * (till the second \n).  Now since BZip2Codecs only update position
    * at least one byte passed a maker, so it is straight forward to differentiate
    * between the two cases mentioned.
    *
    */

    public int read(byte[] b, int off, int len) throws IOException {
      if (needsReset) {
        internalReset();
      }

      int result = 0;
      result = this.input.read(b, off, len);
      if (result == BZip2Constants.END_OF_BLOCK) {
        this.posSM = POS_ADVERTISEMENT_STATE_MACHINE.ADVERTISE;
      }

      if (this.posSM == POS_ADVERTISEMENT_STATE_MACHINE.ADVERTISE) {
        result = this.input.read(b, off, off + 1);
        // This is the precise time to update compressed stream position
        // to the client of this code.
        this.updatePos(true);
        this.posSM = POS_ADVERTISEMENT_STATE_MACHINE.HOLD;
      }

      return result;

    }

    public int read() throws IOException {
      byte b[] = new byte[1];
      int result = this.read(b, 0, 1);
      return (result < 0) ? result : (b[0] & 0xff);
    }

    private void internalReset() throws IOException {
      if (needsReset) {
        needsReset = false;
        BufferedInputStream bufferedIn = readStreamHeader();
        input = new CBZip2InputStream(bufferedIn, this.readMode);
      }
    }    
    
    public void resetState() throws IOException {
      // Cannot read from bufferedIn at this point because bufferedIn
      // might not be ready
      // yet, as in SequenceFile.Reader implementation.
      needsReset = true;
    }

    public long getPos() {
      return this.compressedStreamPosition;
      }

    /*
     * As the comments before read method tell that
     * compressed stream is advertised when at least
     * one byte passed EOB have been read off.  But
     * there is an exception to this rule.  When we
     * construct the stream we advertise the position
     * exactly at EOB.  In the following method
     * shouldAddOn boolean captures this exception.
     *
     */
    private void updatePos(boolean shouldAddOn) {
      int addOn = shouldAddOn ? 1 : 0;
      this.compressedStreamPosition = this.startingPos
          + this.input.getProcessedByteCount() + addOn;
    }

  }// end of BZip2CompressionInputStream

}

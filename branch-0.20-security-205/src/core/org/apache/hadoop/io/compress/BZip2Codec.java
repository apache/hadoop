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

import org.apache.hadoop.io.compress.bzip2.BZip2DummyCompressor;
import org.apache.hadoop.io.compress.bzip2.BZip2DummyDecompressor;
import org.apache.hadoop.io.compress.bzip2.CBZip2InputStream;
import org.apache.hadoop.io.compress.bzip2.CBZip2OutputStream;

/**
 * This class provides CompressionOutputStream and CompressionInputStream for
 * compression and decompression. Currently we dont have an implementation of
 * the Compressor and Decompressor interfaces, so those methods of
 * CompressionCodec which have a Compressor or Decompressor type argument, throw
 * UnsupportedOperationException.
 */
public class BZip2Codec implements
    org.apache.hadoop.io.compress.CompressionCodec {

  private static final String HEADER = "BZ";
  private static final int HEADER_LEN = HEADER.length();

  /**
  * Creates a new instance of BZip2Codec
  */
  public BZip2Codec() {
  }

  /**
  * Creates CompressionOutputStream for BZip2
  *
  * @param out
  *            The output Stream
  * @return The BZip2 CompressionOutputStream
  * @throws java.io.IOException
  *             Throws IO exception
  */
  public CompressionOutputStream createOutputStream(OutputStream out)
      throws IOException {
    return new BZip2CompressionOutputStream(out);
  }

  /**
   * This functionality is currently not supported.
   *
   * @throws java.lang.UnsupportedOperationException
   *             Throws UnsupportedOperationException
   */
  public CompressionOutputStream createOutputStream(OutputStream out,
      Compressor compressor) throws IOException {
    return createOutputStream(out);
  }

  /**
  * This functionality is currently not supported.
  *
  * @throws java.lang.UnsupportedOperationException
  *             Throws UnsupportedOperationException
  */
  public Class<? extends org.apache.hadoop.io.compress.Compressor> getCompressorType() {
    return BZip2DummyCompressor.class;
  }

  /**
  * This functionality is currently not supported.
  *
  * @throws java.lang.UnsupportedOperationException
  *             Throws UnsupportedOperationException
  */
  public Compressor createCompressor() {
    return new BZip2DummyCompressor();
  }

  /**
  * Creates CompressionInputStream to be used to read off uncompressed data.
  *
  * @param in
  *            The InputStream
  * @return Returns CompressionInputStream for BZip2
  * @throws java.io.IOException
  *             Throws IOException
  */
  public CompressionInputStream createInputStream(InputStream in)
      throws IOException {
    return new BZip2CompressionInputStream(in);
  }

  /**
  * This functionality is currently not supported.
  *
  * @throws java.lang.UnsupportedOperationException
  *             Throws UnsupportedOperationException
  */
  public CompressionInputStream createInputStream(InputStream in,
      Decompressor decompressor) throws IOException {
    return createInputStream(in);
  }

  /**
  * This functionality is currently not supported.
  *
  * @throws java.lang.UnsupportedOperationException
  *             Throws UnsupportedOperationException
  */
  public Class<? extends org.apache.hadoop.io.compress.Decompressor> getDecompressorType() {
    return BZip2DummyDecompressor.class;
  }

  /**
  * This functionality is currently not supported.
  *
  * @throws java.lang.UnsupportedOperationException
  *             Throws UnsupportedOperationException
  */
  public Decompressor createDecompressor() {
    return new BZip2DummyDecompressor();
  }

  /**
  * .bz2 is recognized as the default extension for compressed BZip2 files
  *
  * @return A String telling the default bzip2 file extension
  */
  public String getDefaultExtension() {
    return ".bz2";
  }

  private static class BZip2CompressionOutputStream extends CompressionOutputStream {

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
        out.write(HEADER.getBytes());
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
      if (needsReset) {
        // In the case that nothing is written to this stream, we still need to
        // write out the header before closing, otherwise the stream won't be
        // recognized by BZip2CompressionInputStream.
        internalReset();
      }
      this.output.flush();
      this.output.close();
      needsReset = true;
    }

  }// end of class BZip2CompressionOutputStream

  private static class BZip2CompressionInputStream extends CompressionInputStream {

    // class data starts here//
    private CBZip2InputStream input;
    boolean needsReset;
    // class data ends here//

    public BZip2CompressionInputStream(InputStream in) throws IOException {

      super(in);
      needsReset = true;
    }

    private BufferedInputStream readStreamHeader() throws IOException {
      // We are flexible enough to allow the compressed stream not to
      // start with the header of BZ. So it works fine either we have
      // the header or not.
      BufferedInputStream bufferedIn = null;
      if (super.in != null) {
        bufferedIn = new BufferedInputStream(super.in);
        bufferedIn.mark(HEADER_LEN);
        byte[] headerBytes = new byte[HEADER_LEN];
        int actualRead = bufferedIn.read(headerBytes, 0, HEADER_LEN);
        if (actualRead != -1) {
          String header = new String(headerBytes);
          if (header.compareTo(HEADER) != 0) {
            bufferedIn.reset();
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
        input.close();
        needsReset = true;
      }
    }

    public int read(byte[] b, int off, int len) throws IOException {
      if (needsReset) {
        internalReset();
      }
      return this.input.read(b, off, len);

    }

    private void internalReset() throws IOException {
      if (needsReset) {
        needsReset = false;
        BufferedInputStream bufferedIn = readStreamHeader();
        input = new CBZip2InputStream(bufferedIn);
      }
    }    
    
    public void resetState() throws IOException {
      // Cannot read from bufferedIn at this point because bufferedIn might not be ready
      // yet, as in SequenceFile.Reader implementation.
      needsReset = true;
    }

    public int read() throws IOException {
      if (needsReset) {
        internalReset();
      }
      return this.input.read();
    }

  }// end of BZip2CompressionInputStream

}

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
    throw new UnsupportedOperationException();
  }

  /**
  * This functionality is currently not supported.
  *
  * @throws java.lang.UnsupportedOperationException
  *             Throws UnsupportedOperationException
  */
  public Class<org.apache.hadoop.io.compress.Compressor> getCompressorType() {
    throw new UnsupportedOperationException();
  }

  /**
  * This functionality is currently not supported.
  *
  * @throws java.lang.UnsupportedOperationException
  *             Throws UnsupportedOperationException
  */
  public Compressor createCompressor() {
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();

  }

  /**
  * This functionality is currently not supported.
  *
  * @throws java.lang.UnsupportedOperationException
  *             Throws UnsupportedOperationException
  */
  public Class<org.apache.hadoop.io.compress.Decompressor> getDecompressorType() {
    throw new UnsupportedOperationException();
  }

  /**
  * This functionality is currently not supported.
  *
  * @throws java.lang.UnsupportedOperationException
  *             Throws UnsupportedOperationException
  */
  public Decompressor createDecompressor() {
    throw new UnsupportedOperationException();
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

    // class data ends here//

    public BZip2CompressionOutputStream(OutputStream out)
        throws IOException {
      super(out);
      writeStreamHeader();
      this.output = new CBZip2OutputStream(out);
    }

    private void writeStreamHeader() throws IOException {
      if (super.out != null) {
        // The compressed bzip2 stream should start with the
        // identifying characters BZ. Caller of CBZip2OutputStream
        // i.e. this class must write these characters.
        out.write(HEADER.getBytes());
      }
    }

    public void write(byte[] b, int off, int len) throws IOException {
      this.output.write(b, off, len);

    }

    public void finish() throws IOException {
      this.output.flush();
    }

    public void resetState() throws IOException {

    }

    public void write(int b) throws IOException {
      this.output.write(b);
    }

    public void close() throws IOException {
      this.output.flush();
      this.output.close();
    }

    protected void finalize() throws IOException {
      if (this.output != null) {
        this.close();
      }
    }

  }// end of class BZip2CompressionOutputStream

  private static class BZip2CompressionInputStream extends CompressionInputStream {

    // class data starts here//
    private CBZip2InputStream input;

    // class data ends here//

    public BZip2CompressionInputStream(InputStream in) throws IOException {

      super(in);
      BufferedInputStream bufferedIn = readStreamHeader();
      input = new CBZip2InputStream(bufferedIn);
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
      this.input.close();
    }

    public int read(byte[] b, int off, int len) throws IOException {

      return this.input.read(b, off, len);

    }

    public void resetState() throws IOException {

    }

    public int read() throws IOException {
      return this.input.read();

    }

    protected void finalize() throws IOException {
      if (this.input != null) {
        this.close();
      }

    }

  }// end of BZip2CompressionInputStream

}

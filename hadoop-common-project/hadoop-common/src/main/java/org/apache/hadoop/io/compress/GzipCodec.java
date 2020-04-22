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

package org.apache.hadoop.io.compress;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.zlib.BuiltInGzipDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;

/**
 * This class creates gzip compressors/decompressors. 
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class GzipCodec extends DefaultCodec {
  /**
   * A bridge that wraps around a DeflaterOutputStream to make it
   * a CompressionOutputStream.
   */
  @InterfaceStability.Evolving
  protected static class GzipOutputStream extends CompressorStream {

    private static class ResetableGZIPOutputStream extends GZIPOutputStream {
      /**
       * Fixed ten-byte gzip header. See {@link GZIPOutputStream}'s source for
       * details.
       */
      private static final byte[] GZIP_HEADER = new byte[] {
          0x1f, (byte) 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 };

      private boolean reset = false;

      public ResetableGZIPOutputStream(OutputStream out) throws IOException {
        super(out);
      }

      public synchronized void resetState() throws IOException {
        reset = true;
      }

      @Override
      public synchronized void write(byte[] buf, int off, int len)
          throws IOException {
        if (reset) {
          def.reset();
          crc.reset();
          out.write(GZIP_HEADER);
          reset = false;
        }
        super.write(buf, off, len);
      }

      @Override
      public synchronized void close() throws IOException {
        reset = false;
        super.close();
      }

    }

    public GzipOutputStream(OutputStream out) throws IOException {
      super(new ResetableGZIPOutputStream(out));
    }

    /**
     * Allow children types to put a different type in here.
     * @param out the Deflater stream to use
     */
    protected GzipOutputStream(CompressorStream out) {
      super(out);
    }

    @Override
    public void close() throws IOException {
      out.close();
    }
    
    @Override
    public void flush() throws IOException {
      out.flush();
    }
    
    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }
    
    @Override
    public void write(byte[] data, int offset, int length) 
      throws IOException {
      out.write(data, offset, length);
    }
    
    @Override
    public void finish() throws IOException {
      ((ResetableGZIPOutputStream) out).finish();
    }

    @Override
    public void resetState() throws IOException {
      ((ResetableGZIPOutputStream) out).resetState();
    }
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) 
    throws IOException {
    if (!ZlibFactory.isNativeZlibLoaded(conf)) {
      return new GzipOutputStream(out);
    }
    return CompressionCodec.Util.
        createOutputStreamWithCodecPool(this, conf, out);
  }
  
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, 
                                                    Compressor compressor) 
  throws IOException {
    return (compressor != null) ?
               new CompressorStream(out, compressor,
                                    conf.getInt(IO_FILE_BUFFER_SIZE_KEY,
                                            IO_FILE_BUFFER_SIZE_DEFAULT)) :
               createOutputStream(out);
  }

  @Override
  public Compressor createCompressor() {
    return (ZlibFactory.isNativeZlibLoaded(conf))
      ? new GzipZlibCompressor(conf)
      : null;
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return ZlibFactory.isNativeZlibLoaded(conf)
      ? GzipZlibCompressor.class
      : null;
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in)
      throws IOException {
    return CompressionCodec.Util.
        createInputStreamWithCodecPool(this, conf, in);
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in,
                                                  Decompressor decompressor)
  throws IOException {
    if (decompressor == null) {
      decompressor = createDecompressor();  // always succeeds (or throws)
    }
    return new DecompressorStream(in, decompressor,
                                  conf.getInt(IO_FILE_BUFFER_SIZE_KEY,
                                      IO_FILE_BUFFER_SIZE_DEFAULT));
  }

  @Override
  public Decompressor createDecompressor() {
    return (ZlibFactory.isNativeZlibLoaded(conf))
      ? new GzipZlibDecompressor()
      : new BuiltInGzipDecompressor();
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return ZlibFactory.isNativeZlibLoaded(conf)
      ? GzipZlibDecompressor.class
      : BuiltInGzipDecompressor.class;
  }
    
  @Override
  public DirectDecompressor createDirectDecompressor() {
    return ZlibFactory.isNativeZlibLoaded(conf) 
        ? new ZlibDecompressor.ZlibDirectDecompressor(
          ZlibDecompressor.CompressionHeader.AUTODETECT_GZIP_ZLIB, 0) : null;
  }

  @Override
  public String getDefaultExtension() {
    return CodecConstants.GZIP_CODEC_EXTENSION;
  }

  static final class GzipZlibCompressor extends ZlibCompressor {
    public GzipZlibCompressor() {
      super(ZlibCompressor.CompressionLevel.DEFAULT_COMPRESSION,
          ZlibCompressor.CompressionStrategy.DEFAULT_STRATEGY,
          ZlibCompressor.CompressionHeader.GZIP_FORMAT, 64*1024);
    }
    
    public GzipZlibCompressor(Configuration conf) {
      super(ZlibFactory.getCompressionLevel(conf),
           ZlibFactory.getCompressionStrategy(conf),
           ZlibCompressor.CompressionHeader.GZIP_FORMAT,
           64 * 1024);
    }
  }

  static final class GzipZlibDecompressor extends ZlibDecompressor {
    public GzipZlibDecompressor() {
      super(ZlibDecompressor.CompressionHeader.AUTODETECT_GZIP_ZLIB, 64*1024);
    }
  }

}

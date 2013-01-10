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

import java.io.*;
import java.util.zip.GZIPOutputStream;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.zlib.*;

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
      private static final int TRAILER_SIZE = 8;
      public static final String JVMVendor= System.getProperty("java.vendor");
      public static final String JVMVersion= System.getProperty("java.version");
      private static final boolean HAS_BROKEN_FINISH =
          (JVMVendor.contains("IBM") && JVMVersion.contains("1.6.0"));

      public ResetableGZIPOutputStream(OutputStream out) throws IOException {
        super(out);
      }

      public void resetState() throws IOException {
        def.reset();
      }

      /**
       * Override this method for HADOOP-8419.
       * Override because IBM implementation calls def.end() which
       * causes problem when reseting the stream for reuse.
       *
       */
      @Override
      public void finish() throws IOException {
        if (HAS_BROKEN_FINISH) {
          if (!def.finished()) {
            def.finish();
            while (!def.finished()) {
              int i = def.deflate(this.buf, 0, this.buf.length);
              if ((def.finished()) && (i <= this.buf.length - TRAILER_SIZE)) {
                writeTrailer(this.buf, i);
                i += TRAILER_SIZE;
                out.write(this.buf, 0, i);

                return;
              }
              if (i > 0) {
                out.write(this.buf, 0, i);
              }
            }

            byte[] arrayOfByte = new byte[TRAILER_SIZE];
            writeTrailer(arrayOfByte, 0);
            out.write(arrayOfByte);
          }
        } else {
          super.finish();
        }
      }

      /** re-implement for HADOOP-8419 because the relative method in jdk is invisible */
      private void writeTrailer(byte[] paramArrayOfByte, int paramInt)
        throws IOException {
        writeInt((int)this.crc.getValue(), paramArrayOfByte, paramInt);
        writeInt(this.def.getTotalIn(), paramArrayOfByte, paramInt + 4);
      }

      /** re-implement for HADOOP-8419 because the relative method in jdk is invisible */
      private void writeInt(int paramInt1, byte[] paramArrayOfByte, int paramInt2)
        throws IOException {
        writeShort(paramInt1 & 0xFFFF, paramArrayOfByte, paramInt2);
        writeShort(paramInt1 >> 16 & 0xFFFF, paramArrayOfByte, paramInt2 + 2);
      }

      /** re-implement for HADOOP-8419 because the relative method in jdk is invisible */
      private void writeShort(int paramInt1, byte[] paramArrayOfByte, int paramInt2)
        throws IOException {
        paramArrayOfByte[paramInt2] = (byte)(paramInt1 & 0xFF);
        paramArrayOfByte[(paramInt2 + 1)] = (byte)(paramInt1 >> 8 & 0xFF);
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
    return (ZlibFactory.isNativeZlibLoaded(conf)) ?
               new CompressorStream(out, createCompressor(),
                                    conf.getInt("io.file.buffer.size", 4*1024)) :
               new GzipOutputStream(out);
  }
  
  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, 
                                                    Compressor compressor) 
  throws IOException {
    return (compressor != null) ?
               new CompressorStream(out, compressor,
                                    conf.getInt("io.file.buffer.size", 
                                                4*1024)) :
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
    return createInputStream(in, null);
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in,
                                                  Decompressor decompressor)
  throws IOException {
    if (decompressor == null) {
      decompressor = createDecompressor();  // always succeeds (or throws)
    }
    return new DecompressorStream(in, decompressor,
                                  conf.getInt("io.file.buffer.size", 4*1024));
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
  public String getDefaultExtension() {
    return ".gz";
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

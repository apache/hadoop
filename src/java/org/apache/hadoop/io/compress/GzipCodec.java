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
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.zlib.*;

/**
 * This class creates gzip compressors/decompressors. 
 * @author Owen O'Malley
 */
public class GzipCodec extends DefaultCodec {
  /**
   * A bridge that wraps around a DeflaterOutputStream to make it 
   * a CompressionOutputStream.
   * @author Owen O'Malley
   */
  protected static class GzipOutputStream extends CompressorStream {

    private static class ResetableGZIPOutputStream extends GZIPOutputStream {
      
      public ResetableGZIPOutputStream(OutputStream out) throws IOException {
        super(out);
      }
      
      public void resetState() throws IOException {
        def.reset();
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
    
    public void close() throws IOException {
      out.close();
    }
    
    public void flush() throws IOException {
      out.flush();
    }
    
    public void write(int b) throws IOException {
      out.write(b);
    }
    
    public void write(byte[] data, int offset, int length) 
    throws IOException {
      out.write(data, offset, length);
    }
    
    public void finish() throws IOException {
      ((ResetableGZIPOutputStream) out).finish();
    }

    public void resetState() throws IOException {
      ((ResetableGZIPOutputStream) out).resetState();
    }
  }
  
  protected static class GzipInputStream extends DecompressorStream {
    
    private static class ResetableGZIPInputStream extends GZIPInputStream {

      public ResetableGZIPInputStream(InputStream in) throws IOException {
        super(in);
      }
      
      public void resetState() throws IOException {
        inf.reset();
      }
    }
    
    public GzipInputStream(InputStream in) throws IOException {
      super(new ResetableGZIPInputStream(in));
    }
    
    /**
     * Allow subclasses to directly set the inflater stream.
     */
    protected GzipInputStream(DecompressorStream in) {
      super(in);
    }

    public int available() throws IOException {
      return in.available(); 
    }

    public void close() throws IOException {
      in.close();
    }

    public int read() throws IOException {
      return in.read();
    }
    
    public int read(byte[] data, int offset, int len) throws IOException {
      return in.read(data, offset, len);
    }
    
    public long skip(long offset) throws IOException {
      return in.skip(offset);
    }
    
    public void resetState() throws IOException {
      ((ResetableGZIPInputStream) in).resetState();
    }
  }  
  
  /**
   * Create a stream compressor that will write to the given output stream.
   * @param out the location for the final output stream
   * @return a stream the user can write uncompressed data to
   */
  public CompressionOutputStream createOutputStream(OutputStream out) 
  throws IOException {
    CompressionOutputStream compOutStream = null;
    
    if (ZlibFactory.isNativeZlibLoaded()) {
      Compressor compressor = 
        new ZlibCompressor(ZlibCompressor.CompressionLevel.DEFAULT_COMPRESSION,
            ZlibCompressor.CompressionStrategy.DEFAULT_STRATEGY,
            ZlibCompressor.CompressionHeader.GZIP_FORMAT,
            64*1024); 
     
      compOutStream = new CompressorStream(out, compressor,
                        conf.getInt("io.file.buffer.size", 4*1024)); 
    } else {
      compOutStream = new GzipOutputStream(out);
    }
    
    return compOutStream;
  }
  
  /**
   * Create a stream decompressor that will read from the given input stream.
   * @param in the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   */
  public CompressionInputStream createInputStream(InputStream in) 
  throws IOException {
    CompressionInputStream compInStream = null;
    
    if (ZlibFactory.isNativeZlibLoaded()) {
      Decompressor decompressor =
        new ZlibDecompressor(ZlibDecompressor.CompressionHeader.AUTODETECT_GZIP_ZLIB,
            64*1-24);

      compInStream = new DecompressorStream(in, decompressor,
                        conf.getInt("io.file.buffer.size", 4*1024)); 
    } else {
      compInStream = new GzipInputStream(in);
    }
    
    return compInStream;
  }
  
  /**
   * Get the default filename extension for this kind of compression.
   * @return the extension including the '.'
   */
  public String getDefaultExtension() {
    return ".gz";
  }

}

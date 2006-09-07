/*
 * Copyright 2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

public class DefaultCodec implements CompressionCodec {

  /**
   * A bridge that wraps around a DeflaterOutputStream to make it 
   * a CompressionOutputStream.
   * @author Owen O'Malley
   */
  protected static class DefaultCompressionOutputStream 
  extends CompressionOutputStream {

    /**
     * A DeflaterOutputStream that provides a mechanism to 
     * reset the decompressor.
     * @author Owen O'Malley
     */
    private static class ResetableDeflaterOutputStream 
    extends DeflaterOutputStream {
      
      public ResetableDeflaterOutputStream(OutputStream out) {
        super(out);
      }
      
      public void resetState() throws IOException {
        def.reset();
      }
    }
    
    public DefaultCompressionOutputStream(OutputStream out) {
      super(new ResetableDeflaterOutputStream(out));
    }
    
    /**
     * Allow children types to put a different type in here (namely gzip).
     * @param out the Deflater stream to use
     */
    protected DefaultCompressionOutputStream(DeflaterOutputStream out) {
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
      ((DeflaterOutputStream) out).finish();
    }
    
    public void resetState() throws IOException {
      ((ResetableDeflaterOutputStream) out).resetState();
    }
  }
  
  protected static class DefaultCompressionInputStream 
  extends CompressionInputStream {
    
    /**
     * A InflaterStream that provides a mechanism to reset the decompressor.
     * @author Owen O'Malley
     */
    private static class ResetableInflaterInputStream 
    extends InflaterInputStream {
      public ResetableInflaterInputStream(InputStream in) {
        super(in);
      }
      
      public void resetState() throws IOException {
        inf.reset();
      }
    }
    
    public DefaultCompressionInputStream(InputStream in) {
      super(new ResetableInflaterInputStream(in));
    }
    
    /**
     * Allow subclasses to directly set the inflater stream
     */
    protected DefaultCompressionInputStream(InflaterInputStream in) {
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
      ((ResetableInflaterInputStream) in).resetState();
    }
    
  }
  
  /**
   * Create a stream compressor that will write to the given output stream.
   * @param out the location for the final output stream
   * @return a stream the user can write uncompressed data to
   */
  public CompressionOutputStream createOutputStream(OutputStream out) 
  throws IOException {
    return new DefaultCompressionOutputStream(out);
  }
  
  /**
   * Create a stream decompressor that will read from the given input stream.
   * @param in the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   */
  public CompressionInputStream createInputStream(InputStream in) 
  throws IOException {
    return new DefaultCompressionInputStream(in);
  }
  
  /**
   * Get the default filename extension for this kind of compression.
   * @return the extension including the '.'
   */
  public String getDefaultExtension() {
    return ".deflate";
  }

}

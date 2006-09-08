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
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.io.compress.DefaultCodec;

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
  protected static class GzipOutputStream extends DefaultCompressionOutputStream {
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
    protected GzipOutputStream(DefaultCompressionOutputStream out) {
      super(out);
    }
    

    public void resetState() throws IOException {
      ((ResetableGZIPOutputStream) out).resetState();
    }

  }
  
  protected static class GzipInputStream extends DefaultCompressionInputStream {
    
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
    protected GzipInputStream(DefaultCompressionInputStream in) {
      super(in);
    }
  }
  
  /**
   * Create a stream compressor that will write to the given output stream.
   * @param out the location for the final output stream
   * @return a stream the user can write uncompressed data to
   */
  public CompressionOutputStream createOutputStream(OutputStream out) 
  throws IOException {
    return new GzipOutputStream(out);
  }
  
  /**
   * Create a stream decompressor that will read from the given input stream.
   * @param in the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   */
  public CompressionInputStream createInputStream(InputStream in) 
  throws IOException {
    return new GzipInputStream(in);
  }
  
  /**
   * Get the default filename extension for this kind of compression.
   * @return the extension including the '.'
   */
  public String getDefaultExtension() {
    return ".gz";
  }

}

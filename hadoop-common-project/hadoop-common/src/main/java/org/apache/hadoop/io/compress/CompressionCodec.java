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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

/**
 * This class encapsulates a streaming compression/decompression pair.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface CompressionCodec {

  /**
   * Create a {@link CompressionOutputStream} that will write to the given 
   * {@link OutputStream}.
   * 
   * @param out the location for the final output stream
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  CompressionOutputStream createOutputStream(OutputStream out) 
  throws IOException;
  
  /**
   * Create a {@link CompressionOutputStream} that will write to the given 
   * {@link OutputStream} with the given {@link Compressor}.
   * 
   * @param out the location for the final output stream
   * @param compressor compressor to use
   * @return a stream the user can write uncompressed data to have it compressed
   * @throws IOException
   */
  CompressionOutputStream createOutputStream(OutputStream out, 
                                             Compressor compressor) 
  throws IOException;

  /**
   * Get the type of {@link Compressor} needed by this {@link CompressionCodec}.
   * 
   * @return the type of compressor needed by this codec.
   */
  Class<? extends Compressor> getCompressorType();
  
  /**
   * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
   * 
   * @return a new compressor for use by this codec
   */
  Compressor createCompressor();
  
  /**
   * Create a {@link CompressionInputStream} that will read from the given
   * input stream.
   * 
   * @param in the stream to read compressed bytes from
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  CompressionInputStream createInputStream(InputStream in) throws IOException;
  
  /**
   * Create a {@link CompressionInputStream} that will read from the given 
   * {@link InputStream} with the given {@link Decompressor}.
   * 
   * @param in the stream to read compressed bytes from
   * @param decompressor decompressor to use
   * @return a stream to read uncompressed bytes from
   * @throws IOException
   */
  CompressionInputStream createInputStream(InputStream in, 
                                           Decompressor decompressor) 
  throws IOException;


  /**
   * Get the type of {@link Decompressor} needed by this {@link CompressionCodec}.
   * 
   * @return the type of decompressor needed by this codec.
   */
  Class<? extends Decompressor> getDecompressorType();
  
  /**
   * Create a new {@link Decompressor} for use by this {@link CompressionCodec}.
   * 
   * @return a new decompressor for use by this codec
   */
  Decompressor createDecompressor();
  
  /**
   * Get the default filename extension for this kind of compression.
   * @return the extension including the '.'
   */
  String getDefaultExtension();

  static class Util {
    /**
     * Create an output stream with a codec taken from the global CodecPool.
     *
     * @param codec       The codec to use to create the output stream.
     * @param conf        The configuration to use if we need to create a new codec.
     * @param out         The output stream to wrap.
     * @return            The new output stream
     * @throws IOException
     */
    static CompressionOutputStream createOutputStreamWithCodecPool(
        CompressionCodec codec, Configuration conf, OutputStream out)
        throws IOException {
      Compressor compressor = CodecPool.getCompressor(codec, conf);
      CompressionOutputStream stream = null;
      try {
        stream = codec.createOutputStream(out, compressor);
      } finally {
        if (stream == null) {
          CodecPool.returnCompressor(compressor);
        } else {
          stream.setTrackedCompressor(compressor);
        }
      }
      return stream;
    }

    /**
     * Create an input stream with a codec taken from the global CodecPool.
     *
     * @param codec       The codec to use to create the input stream.
     * @param conf        The configuration to use if we need to create a new codec.
     * @param in          The input stream to wrap.
     * @return            The new input stream
     * @throws IOException
     */
    static CompressionInputStream createInputStreamWithCodecPool(
        CompressionCodec codec,  Configuration conf, InputStream in)
          throws IOException {
      Decompressor decompressor = CodecPool.getDecompressor(codec);
      CompressionInputStream stream = null;
      try {
        stream = codec.createInputStream(in, decompressor);
      } finally {
        if (stream == null) {
          CodecPool.returnDecompressor(decompressor);
        } else {
          stream.setTrackedDecompressor(decompressor);
        }
      }
      return stream;
    }
  }
}

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
package org.apache.hadoop.hdfs.server.namenode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * Simple container class that handles support for compressed fsimage files.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class FSImageCompression {

  /** Codec to use to save or load image, or null if the image is not compressed */
  private CompressionCodec imageCodec;

  /**
   * Create a "noop" compression - i.e. uncompressed
   */
  private FSImageCompression() {
  }

  /**
   * Create compression using a particular codec
   */
  private FSImageCompression(CompressionCodec codec) {
    imageCodec = codec;
  }

  public CompressionCodec getImageCodec() {
    return imageCodec;
  }

  /**
   * Create a "noop" compression - i.e. uncompressed
   */
  static FSImageCompression createNoopCompression() {
    return new FSImageCompression();
  }

  /**
   * Create a compression instance based on the user's configuration in the given
   * Configuration object.
   * @throws IOException if the specified codec is not available.
   */
  static FSImageCompression createCompression(Configuration conf)
    throws IOException {
    boolean compressImage = conf.getBoolean(
      DFSConfigKeys.DFS_IMAGE_COMPRESS_KEY,
      DFSConfigKeys.DFS_IMAGE_COMPRESS_DEFAULT);

    if (!compressImage) {
      return createNoopCompression();
    }

    String codecClassName = conf.get(
      DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_KEY,
      DFSConfigKeys.DFS_IMAGE_COMPRESSION_CODEC_DEFAULT);
    return createCompression(conf, codecClassName);
  }

  /**
   * Create a compression instance using the codec specified by
   * <code>codecClassName</code>
   */
  static FSImageCompression createCompression(Configuration conf,
                                                      String codecClassName)
    throws IOException {

    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    CompressionCodec codec = factory.getCodecByClassName(codecClassName);
    if (codec == null) {
      throw new IOException("Not a supported codec: " + codecClassName);
    }

    return new FSImageCompression(codec);
  }

  /**
   * Create a compression instance based on a header read from an input stream.
   * @throws IOException if the specified codec is not available or the
   * underlying IO fails.
   */
  static FSImageCompression readCompressionHeader(
    Configuration conf, DataInput in) throws IOException
  {
    boolean isCompressed = in.readBoolean();

    if (!isCompressed) {
      return createNoopCompression();
    } else {
      String codecClassName = Text.readString(in);
      return createCompression(conf, codecClassName);
    }
  }
  
  /**
   * Unwrap a compressed input stream by wrapping it with a decompressor based
   * on this codec. If this instance represents no compression, simply adds
   * buffering to the input stream.
   * @return a buffered stream that provides uncompressed data
   * @throws IOException If the decompressor cannot be instantiated or an IO
   * error occurs.
   */
  DataInputStream unwrapInputStream(InputStream is) throws IOException {
    if (imageCodec != null) {
      return new DataInputStream(imageCodec.createInputStream(is));
    } else {
      return new DataInputStream(new BufferedInputStream(is));
    }
  }

  /**
   * Write out a header to the given stream that indicates the chosen
   * compression codec, and return the same stream wrapped with that codec.
   * If no codec is specified, simply adds buffering to the stream, so that
   * the returned stream is always buffered.
   * 
   * @param os The stream to write header to and wrap. This stream should
   * be unbuffered.
   * @return A stream wrapped with the specified compressor, or buffering
   * if compression is not enabled.
   * @throws IOException if an IO error occurs or the compressor cannot be
   * instantiated
   */
  DataOutputStream writeHeaderAndWrapStream(OutputStream os)
  throws IOException {
    DataOutputStream dos = new DataOutputStream(os);

    dos.writeBoolean(imageCodec != null);

    if (imageCodec != null) {
      String codecClassName = imageCodec.getClass().getCanonicalName();
      Text.writeString(dos, codecClassName);

      return new DataOutputStream(imageCodec.createOutputStream(os));
    } else {
      // use a buffered output stream
      return new DataOutputStream(new BufferedOutputStream(os));
    }
  }

  @Override
  public String toString() {
    if (imageCodec != null) {
      return "codec " + imageCodec.getClass().getCanonicalName();
    } else {
      return "no compression";
    }
  }
}
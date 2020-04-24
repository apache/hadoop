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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

/**
 * This is a special codec which does not transform the output.
 * It can be declared as a codec in the option "io.compression.codecs",
 * and then it will declare that it supports the file extension
 * set in {@link #OPT_EXTENSION}.
 *
 * This allows decompression to be disabled on a job, even when there is
 * a registered/discoverable decompression codec for a file extension
 * -without having to change the standard codec binding mechanism.
 *
 * For example, to disable decompression for a gzipped files, set the
 * options
 * <pre>
 *   io.compression.codecs = org.apache.hadoop.io.compress.PassthroughCodec
 *   io.compress.passthrough.extension = .gz
 * </pre>
 *
 * <i>Note:</i> this is not a Splittable codec: it doesn't know the
 * capabilities of the passed in stream. It should be possible to
 * extend this in a subclass: the inner classes are marked as protected
 * to enable this. <i>Do not retrofit splitting to this class.</i>.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class PassthroughCodec
    implements Configurable, CompressionCodec {

  private static final Logger LOG =
      LoggerFactory.getLogger(PassthroughCodec.class);

  /**
   * Classname of the codec: {@value}.
   */
  public static final String CLASSNAME =
      "org.apache.hadoop.io.compress.PassthroughCodec";

  /**
   * Option to control the extension of the code: {@value}.
   */
  public static final String OPT_EXTENSION =
      "io.compress.passthrough.extension";

  /**
   * This default extension is here so that if no extension has been defined,
   * some value is still returned: {@value}..
   */
  public static final String DEFAULT_EXTENSION =
      CodecConstants.PASSTHROUGH_CODEC_EXTENSION;

  private Configuration conf;

  private String extension = DEFAULT_EXTENSION;

  public PassthroughCodec() {
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(final Configuration conf) {
    this.conf = conf;
    // update the default extension value at this point, adding
    // a dot prefix if needed.
    String ex = conf.getTrimmed(OPT_EXTENSION, DEFAULT_EXTENSION);
    extension = ex.startsWith(".") ? ex : ("." + ex);
  }

  @Override
  public String getDefaultExtension() {
    LOG.info("Registering fake codec for extension {}", extension);
    return extension;
  }

  @Override
  public CompressionOutputStream createOutputStream(final OutputStream out)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompressionOutputStream createOutputStream(final OutputStream out,
      final Compressor compressor) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Compressor createCompressor() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompressionInputStream createInputStream(final InputStream in)
      throws IOException {
    return createInputStream(in, null);
  }

  @Override
  public CompressionInputStream createInputStream(final InputStream in,
      final Decompressor decompressor) throws IOException {
    return new PassthroughDecompressorStream(in);
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return StubDecompressor.class;
  }

  @Override
  public Decompressor createDecompressor() {
    return new StubDecompressor();
  }

  /**
   * The decompressor.
   */
  protected static final class PassthroughDecompressorStream
      extends DecompressorStream {

    private final InputStream input;

    PassthroughDecompressorStream(final InputStream input)
        throws IOException {
      super(input);
      this.input = input;
    }

    @Override
    public int read(final byte[] b) throws IOException {
      return input.read(b);
    }

    @Override
    public int read() throws IOException {
      return input.read();
    }

    @Override
    public int read(final byte[] b, final int off, final int len)
        throws IOException {
      return input.read(b, off, len);
    }

    @Override
    public long skip(final long n) throws IOException {
      return input.skip(n);
    }

    @Override
    public int available() throws IOException {
      return input.available();
    }
  }

  /**
   * The decompressor is a no-op. It is not needed other than
   * to complete the methods offered by the interface.
   */
  protected static final class StubDecompressor implements Decompressor {

    @Override
    public void setInput(final byte[] b, final int off, final int len) {

    }

    @Override
    public boolean needsInput() {
      return false;
    }

    @Override
    public void setDictionary(final byte[] b, final int off, final int len) {

    }

    @Override
    public boolean needsDictionary() {
      return false;
    }

    @Override
    public boolean finished() {
      return false;
    }

    @Override
    public int decompress(final byte[] b, final int off, final int len)
        throws IOException {
      return 0;
    }

    @Override
    public int getRemaining() {
      return 0;
    }

    @Override
    public void reset() {

    }

    @Override
    public void end() {

    }
  }
}

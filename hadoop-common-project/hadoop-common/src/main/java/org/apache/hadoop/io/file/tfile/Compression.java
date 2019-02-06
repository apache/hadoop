/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.io.file.tfile;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeys.IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

/**
 * Compression related stuff.
 */
public final class Compression {
  static final Logger LOG = LoggerFactory.getLogger(Compression.class);

  /**
   * Prevent the instantiation of class.
   */
  private Compression() {
    // nothing
  }

  static class FinishOnFlushCompressionStream extends FilterOutputStream {
    public FinishOnFlushCompressionStream(CompressionOutputStream cout) {
      super(cout);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      CompressionOutputStream cout = (CompressionOutputStream) out;
      cout.finish();
      cout.flush();
      cout.resetState();
    }
  }

  /**
   * Compression algorithms.
   */
  public enum Algorithm {
    LZO(TFile.COMPRESSION_LZO) {
      private transient boolean checked = false;
      private transient ClassNotFoundException cnf;
      private transient boolean reinitCodecInTests;
      private static final String defaultClazz =
          "org.apache.hadoop.io.compress.LzoCodec";
      private transient String clazz;
      private transient CompressionCodec codec = null;

      private String getLzoCodecClass() {
        String extClazzConf = conf.get(CONF_LZO_CLASS);
        String extClazz = (extClazzConf != null) ?
            extClazzConf : System.getProperty(CONF_LZO_CLASS);
        return (extClazz != null) ? extClazz : defaultClazz;
      }

      @Override
      public synchronized boolean isSupported() {
        if (!checked || reinitCodecInTests) {
          checked = true;
          reinitCodecInTests = conf.getBoolean("test.reload.lzo.codec", false);
          clazz = getLzoCodecClass();
          try {
            LOG.info("Trying to load Lzo codec class: " + clazz);
            codec =
                (CompressionCodec) ReflectionUtils.newInstance(Class
                    .forName(clazz), conf);
          } catch (ClassNotFoundException e) {
            cnf = e;
          }
        }
        return codec != null;
      }

      @Override
      CompressionCodec getCodec() throws IOException {
        if (!isSupported()) {
          throw new IOException(String.format(
              "LZO codec %s=%s could not be loaded", CONF_LZO_CLASS, clazz),
                  cnf);
        }

        return codec;
      }

      @Override
      public synchronized InputStream createDecompressionStream(
          InputStream downStream, Decompressor decompressor,
          int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException(
              "LZO codec class not specified. Did you forget to set property "
                  + CONF_LZO_CLASS + "?");
        }
        InputStream bis1 = null;
        if (downStreamBufferSize > 0) {
          bis1 = new BufferedInputStream(downStream, downStreamBufferSize);
        } else {
          bis1 = downStream;
        }
        conf.setInt(IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY,
            IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_DEFAULT);
        CompressionInputStream cis =
            codec.createInputStream(bis1, decompressor);
        BufferedInputStream bis2 = new BufferedInputStream(cis, DATA_IBUF_SIZE);
        return bis2;
      }

      @Override
      public synchronized OutputStream createCompressionStream(
          OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        if (!isSupported()) {
          throw new IOException(
              "LZO codec class not specified. Did you forget to set property "
                  + CONF_LZO_CLASS + "?");
        }
        OutputStream bos1 = null;
        if (downStreamBufferSize > 0) {
          bos1 = new BufferedOutputStream(downStream, downStreamBufferSize);
        } else {
          bos1 = downStream;
        }
        conf.setInt(IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_KEY,
            IO_COMPRESSION_CODEC_LZO_BUFFERSIZE_DEFAULT);
        CompressionOutputStream cos =
            codec.createOutputStream(bos1, compressor);
        BufferedOutputStream bos2 =
            new BufferedOutputStream(new FinishOnFlushCompressionStream(cos),
                DATA_OBUF_SIZE);
        return bos2;
      }
    },

    GZ(TFile.COMPRESSION_GZ) {
      private transient DefaultCodec codec;

      @Override
      CompressionCodec getCodec() {
        if (codec == null) {
          codec = new DefaultCodec();
          codec.setConf(conf);
        }

        return codec;
      }

      @Override
      public synchronized InputStream createDecompressionStream(
          InputStream downStream, Decompressor decompressor,
          int downStreamBufferSize) throws IOException {
        // Set the internal buffer size to read from down stream.
        if (downStreamBufferSize > 0) {
          codec.getConf().setInt(IO_FILE_BUFFER_SIZE_KEY, downStreamBufferSize);
        }
        CompressionInputStream cis =
            codec.createInputStream(downStream, decompressor);
        BufferedInputStream bis2 = new BufferedInputStream(cis, DATA_IBUF_SIZE);
        return bis2;
      }

      @Override
      public synchronized OutputStream createCompressionStream(
          OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        OutputStream bos1 = null;
        if (downStreamBufferSize > 0) {
          bos1 = new BufferedOutputStream(downStream, downStreamBufferSize);
        } else {
          bos1 = downStream;
        }
        codec.getConf().setInt(IO_FILE_BUFFER_SIZE_KEY, 32 * 1024);
        CompressionOutputStream cos =
            codec.createOutputStream(bos1, compressor);
        BufferedOutputStream bos2 =
            new BufferedOutputStream(new FinishOnFlushCompressionStream(cos),
                DATA_OBUF_SIZE);
        return bos2;
      }

      @Override
      public boolean isSupported() {
        return true;
      }
    },

    NONE(TFile.COMPRESSION_NONE) {
      @Override
      CompressionCodec getCodec() {
        return null;
      }

      @Override
      public synchronized InputStream createDecompressionStream(
          InputStream downStream, Decompressor decompressor,
          int downStreamBufferSize) throws IOException {
        if (downStreamBufferSize > 0) {
          return new BufferedInputStream(downStream, downStreamBufferSize);
        }
        return downStream;
      }

      @Override
      public synchronized OutputStream createCompressionStream(
          OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        if (downStreamBufferSize > 0) {
          return new BufferedOutputStream(downStream, downStreamBufferSize);
        }

        return downStream;
      }

      @Override
      public boolean isSupported() {
        return true;
      }
    };

    // We require that all compression related settings are configured
    // statically in the Configuration object.
    protected static final Configuration conf = new Configuration();
    private final String compressName;
    // data input buffer size to absorb small reads from application.
    private static final int DATA_IBUF_SIZE = 1 * 1024;
    // data output buffer size to absorb small writes from application.
    private static final int DATA_OBUF_SIZE = 4 * 1024;
    public static final String CONF_LZO_CLASS =
        "io.compression.codec.lzo.class";

    Algorithm(String name) {
      this.compressName = name;
    }

    abstract CompressionCodec getCodec() throws IOException;

    public abstract InputStream createDecompressionStream(
        InputStream downStream, Decompressor decompressor,
        int downStreamBufferSize) throws IOException;

    public abstract OutputStream createCompressionStream(
        OutputStream downStream, Compressor compressor, int downStreamBufferSize)
        throws IOException;

    public abstract boolean isSupported();

    public Compressor getCompressor() throws IOException {
      CompressionCodec codec = getCodec();
      if (codec != null) {
        Compressor compressor = CodecPool.getCompressor(codec);
        if (compressor != null) {
          if (compressor.finished()) {
            // Somebody returns the compressor to CodecPool but is still using
            // it.
            LOG.warn("Compressor obtained from CodecPool already finished()");
          } else {
            if(LOG.isDebugEnabled()) {
              LOG.debug("Got a compressor: " + compressor.hashCode());
            }
          }
          /**
           * Following statement is necessary to get around bugs in 0.18 where a
           * compressor is referenced after returned back to the codec pool.
           */
          compressor.reset();
        }
        return compressor;
      }
      return null;
    }

    public void returnCompressor(Compressor compressor) {
      if (compressor != null) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Return a compressor: " + compressor.hashCode());
        }
        CodecPool.returnCompressor(compressor);
      }
    }

    public Decompressor getDecompressor() throws IOException {
      CompressionCodec codec = getCodec();
      if (codec != null) {
        Decompressor decompressor = CodecPool.getDecompressor(codec);
        if (decompressor != null) {
          if (decompressor.finished()) {
            // Somebody returns the decompressor to CodecPool but is still using
            // it.
            LOG.warn("Deompressor obtained from CodecPool already finished()");
          } else {
            if(LOG.isDebugEnabled()) {
              LOG.debug("Got a decompressor: " + decompressor.hashCode());
            }
          }
          /**
           * Following statement is necessary to get around bugs in 0.18 where a
           * decompressor is referenced after returned back to the codec pool.
           */
          decompressor.reset();
        }
        return decompressor;
      }

      return null;
    }

    public void returnDecompressor(Decompressor decompressor) {
      if (decompressor != null) {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Returned a decompressor: " + decompressor.hashCode());
        }
        CodecPool.returnDecompressor(decompressor);
      }
    }

    public String getName() {
      return compressName;
    }
  }

  public static Algorithm getCompressionAlgorithmByName(String compressName) {
    Algorithm[] algos = Algorithm.class.getEnumConstants();

    for (Algorithm a : algos) {
      if (a.getName().equals(compressName)) {
        return a;
      }
    }

    throw new IllegalArgumentException(
        "Unsupported compression algorithm name: " + compressName);
  }

  static String[] getSupportedAlgorithms() {
    Algorithm[] algos = Algorithm.class.getEnumConstants();

    ArrayList<String> ret = new ArrayList<String>();
    for (Algorithm a : algos) {
      if (a.isSupported()) {
        ret.add(a.getName());
      }
    }
    return ret.toArray(new String[ret.size()]);
  }
}

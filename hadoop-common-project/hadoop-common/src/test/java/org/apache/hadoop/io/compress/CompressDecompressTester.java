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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.lz4.Lz4Compressor;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.zlib.BuiltInZlibDeflater;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.log4j.Logger;
import org.junit.Assert;

import org.apache.hadoop.thirdparty.com.google.common.base.Joiner;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableSet;
import static org.junit.Assert.*;

public class CompressDecompressTester<T extends Compressor, E extends Decompressor> {

  private static final Logger logger = Logger
      .getLogger(CompressDecompressTester.class);

  private final byte[] originalRawData;

  private ImmutableList<TesterPair<T, E>> pairs = ImmutableList.of();
  private ImmutableList.Builder<TesterPair<T, E>> builder = ImmutableList.builder();     

  private ImmutableSet<CompressionTestStrategy> stateges = ImmutableSet.of();

  private PreAssertionTester<T, E> assertionDelegate;
  
  public CompressDecompressTester(byte[] originalRawData) {
    this.originalRawData = Arrays.copyOf(originalRawData,
        originalRawData.length);
    this.assertionDelegate = new PreAssertionTester<T, E>() {

      @Override
      public ImmutableList<TesterPair<T, E>> filterOnAssumeWhat(
          ImmutableList<TesterPair<T, E>> pairs) {
        ImmutableList.Builder<TesterPair<T, E>> builder = ImmutableList
            .builder();

        for (TesterPair<T, E> pair : pairs) {
          if (isAvailable(pair))
            builder.add(pair);
        }
        return builder.build();
      }
    };
  }

  public static <T extends Compressor, E extends Decompressor> CompressDecompressTester<T, E> of(
      byte[] rawData) {
    return new CompressDecompressTester<T, E>(rawData);
  }
  

  public CompressDecompressTester<T, E> withCompressDecompressPair(
      T compressor, E decompressor) {
    addPair(
        compressor,
        decompressor,
        Joiner.on("_").join(compressor.getClass().getCanonicalName(),
            decompressor.getClass().getCanonicalName()));
    return this;
  }
  
  public CompressDecompressTester<T, E> withTestCases(
      ImmutableSet<CompressionTestStrategy> stateges) {
    this.stateges = ImmutableSet.copyOf(stateges);
    return this;
  }

  private void addPair(T compressor, E decompressor, String name) {
    builder.add(new TesterPair<T, E>(name, compressor, decompressor));
  }

  public void test() throws Exception {
    pairs = builder.build();
    pairs = assertionDelegate.filterOnAssumeWhat(pairs);

    for (TesterPair<T, E> pair : pairs) {
      for (CompressionTestStrategy strategy : stateges) {
        strategy.getTesterStrategy().assertCompression(pair.getName(),
            pair.getCompressor(), pair.getDecompressor(),
            Arrays.copyOf(originalRawData, originalRawData.length));
      }
    }
    endAll(pairs);
  }

  private void endAll(ImmutableList<TesterPair<T, E>> pairs) {
    for (TesterPair<T, E> pair : pairs)
      pair.end();
  }

  interface PreAssertionTester<T extends Compressor, E extends Decompressor> {
    ImmutableList<TesterPair<T, E>> filterOnAssumeWhat(
        ImmutableList<TesterPair<T, E>> pairs);
  }

  public enum CompressionTestStrategy {

    COMPRESS_DECOMPRESS_ERRORS(new TesterCompressionStrategy() {
      private final Joiner joiner = Joiner.on("- ");

      @Override
      public void assertCompression(String name, Compressor compressor,
          Decompressor decompressor, byte[] rawData) {
        assertTrue(checkSetInputNullPointerException(compressor));
        assertTrue(checkSetInputNullPointerException(decompressor));

        assertTrue(checkCompressArrayIndexOutOfBoundsException(compressor,
            rawData));
        assertTrue(checkCompressArrayIndexOutOfBoundsException(decompressor,
            rawData));

        assertTrue(checkCompressNullPointerException(compressor, rawData));
        assertTrue(checkCompressNullPointerException(decompressor, rawData));

        assertTrue(checkSetInputArrayIndexOutOfBoundsException(compressor));
        assertTrue(checkSetInputArrayIndexOutOfBoundsException(decompressor));
      }

      private boolean checkSetInputNullPointerException(Compressor compressor) {
        try {
          compressor.setInput(null, 0, 1);
        } catch (NullPointerException npe) {
          return true;
        } catch (Exception ex) {
          logger.error(joiner.join(compressor.getClass().getCanonicalName(),
              "checkSetInputNullPointerException error !!!"));
        }
        return false;
      }

      private boolean checkCompressNullPointerException(Compressor compressor,
          byte[] rawData) {
        try {
          compressor.setInput(rawData, 0, rawData.length);
          compressor.compress(null, 0, 1);
        } catch (NullPointerException npe) {
          return true;
        } catch (Exception ex) {
          logger.error(joiner.join(compressor.getClass().getCanonicalName(),
              "checkCompressNullPointerException error !!!"));
        }
        return false;
      }

      private boolean checkCompressNullPointerException(
          Decompressor decompressor, byte[] rawData) {
        try {
          decompressor.setInput(rawData, 0, rawData.length);
          decompressor.decompress(null, 0, 1);
        } catch (NullPointerException npe) {
          return true;
        } catch (Exception ex) {
          logger.error(joiner.join(decompressor.getClass().getCanonicalName(),
              "checkCompressNullPointerException error !!!"));
        }
        return false;
      }

      private boolean checkSetInputNullPointerException(
          Decompressor decompressor) {
        try {
          decompressor.setInput(null, 0, 1);
        } catch (NullPointerException npe) {
          return true;
        } catch (Exception ex) {
          logger.error(joiner.join(decompressor.getClass().getCanonicalName(),
              "checkSetInputNullPointerException error !!!"));
        }
        return false;
      }

      private boolean checkSetInputArrayIndexOutOfBoundsException(
          Compressor compressor) {
        try {
          compressor.setInput(new byte[] { (byte) 0 }, 0, -1);
        } catch (ArrayIndexOutOfBoundsException e) {
          return true;
        } catch (Exception e) {
          logger.error(joiner.join(compressor.getClass().getCanonicalName(),
              "checkSetInputArrayIndexOutOfBoundsException error !!!"));
        }
        return false;
      }

      private boolean checkCompressArrayIndexOutOfBoundsException(
          Compressor compressor, byte[] rawData) {
        try {
          compressor.setInput(rawData, 0, rawData.length);
          compressor.compress(new byte[rawData.length], 0, -1);
        } catch (ArrayIndexOutOfBoundsException e) {
          return true;
        } catch (Exception e) {
          logger.error(joiner.join(compressor.getClass().getCanonicalName(),
              "checkCompressArrayIndexOutOfBoundsException error !!!"));
        }
        return false;
      }

      private boolean checkCompressArrayIndexOutOfBoundsException(
          Decompressor decompressor, byte[] rawData) {
        try {
          decompressor.setInput(rawData, 0, rawData.length);
          decompressor.decompress(new byte[rawData.length], 0, -1);
        } catch (ArrayIndexOutOfBoundsException e) {
          return true;
        } catch (Exception e) {
          logger.error(joiner.join(decompressor.getClass().getCanonicalName(),
              "checkCompressArrayIndexOutOfBoundsException error !!!"));
        }
        return false;
      }

      private boolean checkSetInputArrayIndexOutOfBoundsException(
          Decompressor decompressor) {
        try {
          decompressor.setInput(new byte[] { (byte) 0 }, 0, -1);
        } catch (ArrayIndexOutOfBoundsException e) {
          return true;
        } catch (Exception e) {
          logger.error(joiner.join(decompressor.getClass().getCanonicalName(),
              "checkNullPointerException error !!!"));
        }
        return false;
      }

    }),

    COMPRESS_DECOMPRESS_SINGLE_BLOCK(new TesterCompressionStrategy() {
      final Joiner joiner = Joiner.on("- ");

      @Override
      public void assertCompression(String name, Compressor compressor,
          Decompressor decompressor, byte[] rawData) throws Exception {

        int cSize = 0;
        int decompressedSize = 0;
        // Snappy compression can increase data size
        int maxCompressedLength = 32 + rawData.length + rawData.length/6;
        byte[] compressedResult = new byte[maxCompressedLength];
        byte[] decompressedBytes = new byte[rawData.length];
        assertTrue(
            joiner.join(name, "compressor.needsInput before error !!!"),
            compressor.needsInput());
        assertEquals(
              joiner.join(name, "compressor.getBytesWritten before error !!!"),
            0, compressor.getBytesWritten());
        compressor.setInput(rawData, 0, rawData.length);
        compressor.finish();
        while (!compressor.finished()) {
          cSize += compressor.compress(compressedResult, 0,
              compressedResult.length);
        }
        compressor.reset();

        assertTrue(
            joiner.join(name, "decompressor.needsInput() before error !!!"),
            decompressor.needsInput());
        decompressor.setInput(compressedResult, 0, cSize);
        assertFalse(
            joiner.join(name, "decompressor.needsInput() after error !!!"),
            decompressor.needsInput());
        while (!decompressor.finished()) {
          decompressedSize = decompressor.decompress(decompressedBytes, 0,
              decompressedBytes.length);
        }
        decompressor.reset();
        assertEquals(joiner.join(name, " byte size not equals error !!!"),
            rawData.length, decompressedSize);
        assertArrayEquals(
            joiner.join(name, " byte arrays not equals error !!!"), rawData,
            decompressedBytes);
      }
    }),

    COMPRESS_DECOMPRESS_WITH_EMPTY_STREAM(new TesterCompressionStrategy() {
      final Joiner joiner = Joiner.on("- ");
      final ImmutableMap<Class<? extends Compressor>, Integer> emptySize = ImmutableMap
          .of(Lz4Compressor.class, 4, ZlibCompressor.class, 16,
              SnappyCompressor.class, 4, BuiltInZlibDeflater.class, 16);

      @Override
      void assertCompression(String name, Compressor compressor,
          Decompressor decompressor, byte[] originalRawData) {
        byte[] buf = null;
        ByteArrayInputStream bytesIn = null;
        BlockDecompressorStream blockDecompressorStream = null;
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        // close without write
        try {
          compressor.reset();
          // decompressor.end();
          BlockCompressorStream blockCompressorStream = new BlockCompressorStream(
              bytesOut, compressor, 1024, 0);
          blockCompressorStream.close();
          // check compressed output
          buf = bytesOut.toByteArray();
          int emSize = emptySize.get(compressor.getClass());
          Assert.assertEquals(
              joiner.join(name, "empty stream compressed output size != "
                  + emSize), emSize, buf.length);
          // use compressed output as input for decompression
          bytesIn = new ByteArrayInputStream(buf);
          // create decompression stream
          blockDecompressorStream = new BlockDecompressorStream(bytesIn,
              decompressor, 1024);
          // no byte is available because stream was closed
          assertEquals(joiner.join(name, " return value is not -1"), -1,
              blockDecompressorStream.read());
        } catch (IOException e) {
          fail(joiner.join(name, e.getMessage()));
        } finally {
          if (blockDecompressorStream != null)
            try {
              bytesOut.close();
              blockDecompressorStream.close();
              bytesIn.close();
              blockDecompressorStream.close();
            } catch (IOException e) {
            }
        }
      }

    }),

    COMPRESS_DECOMPRESS_BLOCK(new TesterCompressionStrategy() {
      private final Joiner joiner = Joiner.on("- ");
      private static final int BLOCK_SIZE = 512;
      private final byte[] operationBlock = new byte[BLOCK_SIZE];
      // Use default of 512 as bufferSize and compressionOverhead of
      // (1% of bufferSize + 12 bytes) = 18 bytes (zlib algorithm).
      private static final int overheadSpace = BLOCK_SIZE / 100 + 12;

      @Override
      public void assertCompression(String name, Compressor compressor,
          Decompressor decompressor, byte[] originalRawData) {
        int off = 0;
        int len = originalRawData.length;
        int maxSize = BLOCK_SIZE - overheadSpace;
        int compresSize = 0;
        List<Integer> blockLabels = new ArrayList<Integer>();
        ByteArrayOutputStream compressedOut = new ByteArrayOutputStream();
        ByteArrayOutputStream decompressOut = new ByteArrayOutputStream();
        try {
          if (originalRawData.length > maxSize) {
            do {
              int bufLen = Math.min(len, maxSize);
              compressor.setInput(originalRawData, off, bufLen);
              compressor.finish();
              while (!compressor.finished()) {
                compresSize = compressor.compress(operationBlock, 0,
                    operationBlock.length);
                compressedOut.write(operationBlock, 0, compresSize);
                blockLabels.add(compresSize);
              }
              compressor.reset();
              off += bufLen;
              len -= bufLen;
            } while (len > 0);
          }

          off = 0;
          // compressed bytes
          byte[] compressedBytes = compressedOut.toByteArray();
          for (Integer step : blockLabels) {
            decompressor.setInput(compressedBytes, off, step);
            while (!decompressor.finished()) {
              int dSize = decompressor.decompress(operationBlock, 0,
                  operationBlock.length);
              decompressOut.write(operationBlock, 0, dSize);
            }
            decompressor.reset();
            off = off + step;
          }
          assertArrayEquals(
              joiner.join(name, "byte arrays not equals error !!!"),
              originalRawData, decompressOut.toByteArray());
        } catch (Exception ex) {
          throw new AssertionError(name + ex, ex);
        } finally {
          try {
            compressedOut.close();
          } catch (IOException e) {
          }
          try {
            decompressOut.close();
          } catch (IOException e) {
          }
        }
      }
    });

    private final TesterCompressionStrategy testerStrategy;

    CompressionTestStrategy(TesterCompressionStrategy testStrategy) {
      this.testerStrategy = testStrategy;
    }

    public TesterCompressionStrategy getTesterStrategy() {
      return testerStrategy;
    }
  }

  static final class TesterPair<T extends Compressor, E extends Decompressor> {
    private final T compressor;
    private final E decompressor;
    private final String name;

    TesterPair(String name, T compressor, E decompressor) {
      this.compressor = compressor;
      this.decompressor = decompressor;
      this.name = name;
    }

    public void end() {
      Configuration cfg = new Configuration();
      compressor.reinit(cfg);
      compressor.end();
      decompressor.end();
    }

    public T getCompressor() {
      return compressor;
    }

    public E getDecompressor() {
      return decompressor;
    }

    public String getName() {
      return name;
    }
  }
  
  /**
   * Method for compressor availability check
   */
  private static <T extends Compressor, E extends Decompressor> boolean isAvailable(TesterPair<T, E> pair) {
    Compressor compressor = pair.compressor;

    if (compressor.getClass().isAssignableFrom(Lz4Compressor.class))
      return true;

    else if (compressor.getClass().isAssignableFrom(BuiltInZlibDeflater.class)
            && NativeCodeLoader.isNativeCodeLoaded())
      return true;

    else if (compressor.getClass().isAssignableFrom(ZlibCompressor.class)) {
      return ZlibFactory.isNativeZlibLoaded(new Configuration());
    } else if (compressor.getClass().isAssignableFrom(SnappyCompressor.class)) {
      return true;
    }

    return false;      
  }
  
  abstract static class TesterCompressionStrategy {

    protected final Logger logger = Logger.getLogger(getClass());

    abstract void assertCompression(String name, Compressor compressor,
        Decompressor decompressor, byte[] originalRawData) throws Exception;
  }
}

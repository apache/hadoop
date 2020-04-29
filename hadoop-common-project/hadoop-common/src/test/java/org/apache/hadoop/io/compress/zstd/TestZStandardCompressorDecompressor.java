/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.compress.zstd;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CompressorStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DecompressorStream;
import org.apache.hadoop.io.compress.ZStandardCodec;
import org.apache.hadoop.test.MultithreadedTestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class TestZStandardCompressorDecompressor {
  private final static char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
  private static final Random RANDOM = new Random(12345L);
  private static final Configuration CONFIGURATION = new Configuration();
  private static File compressedFile;
  private static File uncompressedFile;

  @BeforeClass
  public static void beforeClass() throws Exception {
    CONFIGURATION.setInt(IO_FILE_BUFFER_SIZE_KEY, 1024 * 64);
    uncompressedFile = new File(TestZStandardCompressorDecompressor.class
        .getResource("/zstd/test_file.txt").toURI());
    compressedFile = new File(TestZStandardCompressorDecompressor.class
        .getResource("/zstd/test_file.txt.zst").toURI());
  }

  @Before
  public void before() throws Exception {
    assumeTrue(ZStandardCodec.isNativeCodeLoaded());
  }

  @Test
  public void testCompressionCompressesCorrectly() throws Exception {
    int uncompressedSize = (int) FileUtils.sizeOf(uncompressedFile);
    byte[] bytes = FileUtils.readFileToByteArray(uncompressedFile);
    assertEquals(uncompressedSize, bytes.length);

    Configuration conf = new Configuration();
    ZStandardCodec codec = new ZStandardCodec();
    codec.setConf(conf);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Compressor compressor = codec.createCompressor();
    CompressionOutputStream outputStream =
        codec.createOutputStream(baos, compressor);

    for (byte aByte : bytes) {
      outputStream.write(aByte);
    }

    outputStream.finish();
    outputStream.close();
    assertEquals(uncompressedSize, compressor.getBytesRead());
    assertTrue(compressor.finished());

    // just make sure we can decompress the file

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    Decompressor decompressor = codec.createDecompressor();
    CompressionInputStream inputStream =
        codec.createInputStream(bais, decompressor);
    byte[] buffer = new byte[100];
    int n = buffer.length;
    while ((n = inputStream.read(buffer, 0, n)) != -1) {
      byteArrayOutputStream.write(buffer, 0, n);
    }
    assertArrayEquals(bytes, byteArrayOutputStream.toByteArray());
  }

  @Test(expected = NullPointerException.class)
  public void testCompressorSetInputNullPointerException() {
    ZStandardCompressor compressor = new ZStandardCompressor();
    compressor.setInput(null, 0, 10);
  }

  //test on NullPointerException in {@code decompressor.setInput()}
  @Test(expected = NullPointerException.class)
  public void testDecompressorSetInputNullPointerException() {
    ZStandardDecompressor decompressor =
        new ZStandardDecompressor(IO_FILE_BUFFER_SIZE_DEFAULT);
    decompressor.setInput(null, 0, 10);
  }

  //test on ArrayIndexOutOfBoundsException in {@code compressor.setInput()}
  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testCompressorSetInputAIOBException() {
    ZStandardCompressor compressor = new ZStandardCompressor();
    compressor.setInput(new byte[] {}, -5, 10);
  }

  //test on ArrayIndexOutOfBoundsException in {@code decompressor.setInput()}
  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testDecompressorSetInputAIOUBException() {
    ZStandardDecompressor decompressor =
        new ZStandardDecompressor(IO_FILE_BUFFER_SIZE_DEFAULT);
    decompressor.setInput(new byte[] {}, -5, 10);
  }

  //test on NullPointerException in {@code compressor.compress()}
  @Test(expected = NullPointerException.class)
  public void testCompressorCompressNullPointerException() throws Exception {
    ZStandardCompressor compressor = new ZStandardCompressor();
    byte[] bytes = generate(1024 * 6);
    compressor.setInput(bytes, 0, bytes.length);
    compressor.compress(null, 0, 0);
  }

  //test on NullPointerException in {@code decompressor.decompress()}
  @Test(expected = NullPointerException.class)
  public void testDecompressorCompressNullPointerException() throws Exception {
    ZStandardDecompressor decompressor =
        new ZStandardDecompressor(IO_FILE_BUFFER_SIZE_DEFAULT);
    byte[] bytes = generate(1024 * 6);
    decompressor.setInput(bytes, 0, bytes.length);
    decompressor.decompress(null, 0, 0);
  }

  //test on ArrayIndexOutOfBoundsException in {@code compressor.compress()}
  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testCompressorCompressAIOBException() throws Exception {
    ZStandardCompressor compressor = new ZStandardCompressor();
    byte[] bytes = generate(1024 * 6);
    compressor.setInput(bytes, 0, bytes.length);
    compressor.compress(new byte[] {}, 0, -1);
  }

  //test on ArrayIndexOutOfBoundsException in decompressor.decompress()
  @Test(expected = ArrayIndexOutOfBoundsException.class)
  public void testDecompressorCompressAIOBException() throws Exception {
    ZStandardDecompressor decompressor =
        new ZStandardDecompressor(IO_FILE_BUFFER_SIZE_DEFAULT);
    byte[] bytes = generate(1024 * 6);
    decompressor.setInput(bytes, 0, bytes.length);
    decompressor.decompress(new byte[] {}, 0, -1);
  }

  // test ZStandardCompressor compressor.compress()
  @Test
  public void testSetInputWithBytesSizeMoreThenDefaultZStandardBufferSize()
      throws Exception {
    int bytesSize = 1024 * 2056 + 1;
    ZStandardCompressor compressor = new ZStandardCompressor();
    byte[] bytes = generate(bytesSize);
    assertTrue("needsInput error !!!", compressor.needsInput());
    compressor.setInput(bytes, 0, bytes.length);
    byte[] emptyBytes = new byte[bytesSize];
    int cSize = compressor.compress(emptyBytes, 0, bytes.length);
    assertTrue(cSize > 0);
  }

  // test compress/decompress process through
  // CompressionOutputStream/CompressionInputStream api
  @Test
  public void testCompressorDecompressorLogicWithCompressionStreams()
      throws Exception {
    DataOutputStream deflateOut = null;
    DataInputStream inflateIn = null;
    int byteSize = 1024 * 100;
    byte[] bytes = generate(byteSize);
    int bufferSize = IO_FILE_BUFFER_SIZE_DEFAULT;
    try {
      DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
      CompressionOutputStream deflateFilter =
          new CompressorStream(compressedDataBuffer, new ZStandardCompressor(),
              bufferSize);
      deflateOut =
          new DataOutputStream(new BufferedOutputStream(deflateFilter));
      deflateOut.write(bytes, 0, bytes.length);
      deflateOut.flush();
      deflateFilter.finish();

      DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
      deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0,
          compressedDataBuffer.getLength());

      CompressionInputStream inflateFilter =
          new DecompressorStream(deCompressedDataBuffer,
              new ZStandardDecompressor(bufferSize), bufferSize);

      inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));

      byte[] result = new byte[byteSize];
      inflateIn.read(result);
      assertArrayEquals("original array not equals compress/decompressed array",
          result, bytes);
    } finally {
      IOUtils.closeQuietly(deflateOut);
      IOUtils.closeQuietly(inflateIn);
    }
  }

  @Test
  public void testZStandardCompressDecompressInMultiThreads() throws Exception {
    MultithreadedTestUtil.TestContext ctx =
        new MultithreadedTestUtil.TestContext();
    for (int i = 0; i < 10; i++) {
      ctx.addThread(new MultithreadedTestUtil.TestingThread(ctx) {
        @Override
        public void doWork() throws Exception {
          testCompressDecompress();
        }
      });
    }
    ctx.startThreads();

    ctx.waitFor(60000);
  }

  @Test
  public void testCompressDecompress() throws Exception {
    byte[] rawData;
    int rawDataSize;
    rawDataSize = IO_FILE_BUFFER_SIZE_DEFAULT;
    rawData = generate(rawDataSize);
    ZStandardCompressor compressor = new ZStandardCompressor();
    ZStandardDecompressor decompressor =
        new ZStandardDecompressor(IO_FILE_BUFFER_SIZE_DEFAULT);
    assertFalse(compressor.finished());
    compressor.setInput(rawData, 0, rawData.length);
    assertEquals(0, compressor.getBytesRead());
    compressor.finish();

    byte[] compressedResult = new byte[rawDataSize];
    int cSize = compressor.compress(compressedResult, 0, rawDataSize);
    assertEquals(rawDataSize, compressor.getBytesRead());
    assertTrue(cSize < rawDataSize);
    decompressor.setInput(compressedResult, 0, cSize);
    byte[] decompressedBytes = new byte[rawDataSize];
    decompressor.decompress(decompressedBytes, 0, decompressedBytes.length);
    assertEquals(bytesToHex(rawData), bytesToHex(decompressedBytes));
    compressor.reset();
    decompressor.reset();
  }

  @Test
  public void testCompressingWithOneByteOutputBuffer() throws Exception {
    int uncompressedSize = (int) FileUtils.sizeOf(uncompressedFile);
    byte[] bytes = FileUtils.readFileToByteArray(uncompressedFile);
    assertEquals(uncompressedSize, bytes.length);

    Configuration conf = new Configuration();
    ZStandardCodec codec = new ZStandardCodec();
    codec.setConf(conf);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Compressor compressor =
        new ZStandardCompressor(3, IO_FILE_BUFFER_SIZE_DEFAULT, 1);
    CompressionOutputStream outputStream =
        codec.createOutputStream(baos, compressor);

    for (byte aByte : bytes) {
      outputStream.write(aByte);
    }

    outputStream.finish();
    outputStream.close();
    assertEquals(uncompressedSize, compressor.getBytesRead());
    assertTrue(compressor.finished());

    // just make sure we can decompress the file

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    Decompressor decompressor = codec.createDecompressor();
    CompressionInputStream inputStream =
        codec.createInputStream(bais, decompressor);
    byte[] buffer = new byte[100];
    int n = buffer.length;
    while ((n = inputStream.read(buffer, 0, n)) != -1) {
      byteArrayOutputStream.write(buffer, 0, n);
    }
    assertArrayEquals(bytes, byteArrayOutputStream.toByteArray());

  }

  @Test
  public void testZStandardCompressDecompress() throws Exception {
    byte[] rawData = null;
    int rawDataSize = 0;
    rawDataSize = IO_FILE_BUFFER_SIZE_DEFAULT;
    rawData = generate(rawDataSize);
    ZStandardCompressor compressor = new ZStandardCompressor();
    ZStandardDecompressor decompressor = new ZStandardDecompressor(rawDataSize);
    assertTrue(compressor.needsInput());
    assertFalse("testZStandardCompressDecompress finished error",
        compressor.finished());
    compressor.setInput(rawData, 0, rawData.length);
    compressor.finish();

    byte[] compressedResult = new byte[rawDataSize];
    int cSize = compressor.compress(compressedResult, 0, rawDataSize);
    assertEquals(rawDataSize, compressor.getBytesRead());
    assertTrue("compressed size no less then original size",
        cSize < rawDataSize);
    decompressor.setInput(compressedResult, 0, cSize);
    byte[] decompressedBytes = new byte[rawDataSize];
    decompressor.decompress(decompressedBytes, 0, decompressedBytes.length);
    String decompressed = bytesToHex(decompressedBytes);
    String original = bytesToHex(rawData);
    assertEquals(original, decompressed);
    compressor.reset();
    decompressor.reset();
  }

  @Test
  public void testDecompressingOutput() throws Exception {
    byte[] expectedDecompressedResult =
        FileUtils.readFileToByteArray(uncompressedFile);
    ZStandardCodec codec = new ZStandardCodec();
    codec.setConf(CONFIGURATION);
    CompressionInputStream inputStream = codec
        .createInputStream(FileUtils.openInputStream(compressedFile),
            codec.createDecompressor());

    byte[] toDecompress = new byte[100];
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] decompressedResult;
    int totalFileSize = 0;
    int result = toDecompress.length;
    try {
      while ((result = inputStream.read(toDecompress, 0, result)) != -1) {
        baos.write(toDecompress, 0, result);
        totalFileSize += result;
      }
      decompressedResult = baos.toByteArray();
    } finally {
      IOUtils.closeQuietly(baos);
    }

    assertEquals(decompressedResult.length, totalFileSize);
    assertEquals(bytesToHex(expectedDecompressedResult),
        bytesToHex(decompressedResult));
  }

  @Test
  public void testZStandardDirectCompressDecompress() throws Exception {
    int[] size = {1, 4, 16, 4 * 1024, 64 * 1024, 128 * 1024, 1024 * 1024 };
    for (int aSize : size) {
      System.out.println("aSize = " + aSize);
      compressDecompressLoop(aSize);
    }
  }

  private void compressDecompressLoop(int rawDataSize) throws IOException {
    byte[] rawData = null;
    rawData = generate(rawDataSize);
    ByteArrayOutputStream baos = new ByteArrayOutputStream(rawDataSize + 12);
    CompressionOutputStream deflateFilter =
        new CompressorStream(baos, new ZStandardCompressor(), 4096);
    DataOutputStream deflateOut =
        new DataOutputStream(new BufferedOutputStream(deflateFilter));

    deflateOut.write(rawData, 0, rawData.length);
    deflateOut.flush();
    deflateFilter.finish();
    byte[] compressedResult = baos.toByteArray();
    int compressedSize = compressedResult.length;
    ZStandardDecompressor.ZStandardDirectDecompressor decompressor =
        new ZStandardDecompressor.ZStandardDirectDecompressor(4096);

    ByteBuffer inBuf = ByteBuffer.allocateDirect(compressedSize);
    ByteBuffer outBuf = ByteBuffer.allocateDirect(8096);

    inBuf.put(compressedResult, 0, compressedSize);
    inBuf.flip();

    ByteBuffer expected = ByteBuffer.wrap(rawData);

    outBuf.clear();
    while (!decompressor.finished()) {
      decompressor.decompress(inBuf, outBuf);
      outBuf.flip();
      while (outBuf.remaining() > 0) {
        assertEquals(expected.get(), outBuf.get());
      }
      outBuf.clear();
    }
    outBuf.flip();
    while (outBuf.remaining() > 0) {
      assertEquals(expected.get(), outBuf.get());
    }
    outBuf.clear();

    assertEquals(0, expected.remaining());
  }

  @Test
  public void testReadingWithAStream() throws Exception {
    FileInputStream inputStream = FileUtils.openInputStream(compressedFile);
    ZStandardCodec codec = new ZStandardCodec();
    codec.setConf(CONFIGURATION);
    Decompressor decompressor = codec.createDecompressor();
    CompressionInputStream cis =
        codec.createInputStream(inputStream, decompressor);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] resultOfDecompression;
    try {
      byte[] buffer = new byte[100];
      int n;
      while ((n = cis.read(buffer, 0, buffer.length)) != -1) {
        baos.write(buffer, 0, n);
      }
      resultOfDecompression = baos.toByteArray();
    } finally {
      IOUtils.closeQuietly(baos);
      IOUtils.closeQuietly(cis);
    }

    byte[] expected = FileUtils.readFileToByteArray(uncompressedFile);
    assertEquals(bytesToHex(expected), bytesToHex(resultOfDecompression));
  }

  @Test
  public void testDecompressReturnsWhenNothingToDecompress() throws Exception {
    ZStandardDecompressor decompressor =
        new ZStandardDecompressor(IO_FILE_BUFFER_SIZE_DEFAULT);
    int result = decompressor.decompress(new byte[10], 0, 10);
    assertEquals(0, result);
  }

  public static byte[] generate(int size) {
    byte[] data = new byte[size];
    for (int i = 0; i < size; i++) {
      data[i] = (byte) RANDOM.nextInt(16);
    }
    return data;
  }

  private static String bytesToHex(byte[] bytes) {
    char[] hexChars = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = HEX_ARRAY[v >>> 4];
      hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars);
  }
}

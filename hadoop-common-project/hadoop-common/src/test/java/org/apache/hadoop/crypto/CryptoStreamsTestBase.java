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
package org.apache.hadoop.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.EnumSet;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.HasEnhancedByteBufferAccess;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RandomDatum;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class CryptoStreamsTestBase {
  protected static final Log LOG = LogFactory.getLog(
      CryptoStreamsTestBase.class);

  protected static CryptoCodec codec;
  private static final byte[] key = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 
    0x07, 0x08, 0x09, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16};
  private static final byte[] iv = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 
    0x07, 0x08, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
  
  protected static final int count = 10000;
  protected static int defaultBufferSize = 8192;
  protected static int smallBufferSize = 1024;
  private byte[] data;
  private int dataLen;
  
  @Before
  public void setUp() throws IOException {
    // Generate data
    final int seed = new Random().nextInt();
    final DataOutputBuffer dataBuf = new DataOutputBuffer();
    final RandomDatum.Generator generator = new RandomDatum.Generator(seed);
    for(int i = 0; i < count; ++i) {
      generator.next();
      final RandomDatum key = generator.getKey();
      final RandomDatum value = generator.getValue();
      
      key.write(dataBuf);
      value.write(dataBuf);
    }
    LOG.info("Generated " + count + " records");
    data = dataBuf.getData();
    dataLen = dataBuf.getLength();
  }
  
  protected void writeData(OutputStream out) throws Exception {
    out.write(data, 0, dataLen);
    out.close();
  }
  
  protected int getDataLen() {
    return dataLen;
  }
  
  private int readAll(InputStream in, byte[] b, int off, int len) 
      throws IOException {
    int n = 0;
    int total = 0;
    while (n != -1) {
      total += n;
      if (total >= len) {
        break;
      }
      n = in.read(b, off + total, len - total);
    }
    
    return total;
  }
  
  protected OutputStream getOutputStream(int bufferSize) throws IOException {
    return getOutputStream(bufferSize, key, iv);
  }
  
  protected abstract OutputStream getOutputStream(int bufferSize, byte[] key, 
      byte[] iv) throws IOException;
  
  protected InputStream getInputStream(int bufferSize) throws IOException {
    return getInputStream(bufferSize, key, iv);
  }
  
  protected abstract InputStream getInputStream(int bufferSize, byte[] key, 
      byte[] iv) throws IOException;
  
  /** Test crypto reading with different buffer size. */
  @Test(timeout=120000)
  public void testRead() throws Exception {
    OutputStream out = getOutputStream(defaultBufferSize);
    writeData(out);
    
    // Default buffer size
    InputStream in = getInputStream(defaultBufferSize);
    readCheck(in);
    in.close();
    
    // Small buffer size
    in = getInputStream(smallBufferSize);
    readCheck(in);
    in.close();
  }
  
  private void readCheck(InputStream in) throws Exception {
    byte[] result = new byte[dataLen];
    int n = readAll(in, result, 0, dataLen);
    
    Assert.assertEquals(dataLen, n);
    byte[] expectedData = new byte[n];
    System.arraycopy(data, 0, expectedData, 0, n);
    Assert.assertArrayEquals(result, expectedData);
    
    // EOF
    n = in.read(result, 0, dataLen);
    Assert.assertEquals(n, -1);
    in.close();
  }
  
  /** Test crypto writing with different buffer size. */
  @Test(timeout = 120000)
  public void testWrite() throws Exception {
    // Default buffer size
    writeCheck(defaultBufferSize);

    // Small buffer size
    writeCheck(smallBufferSize);
  }

  private void writeCheck(int bufferSize) throws Exception {
    OutputStream out = getOutputStream(bufferSize);
    writeData(out);

    if (out instanceof FSDataOutputStream) {
      Assert.assertEquals(((FSDataOutputStream) out).getPos(), getDataLen());
    }
  }

  /** Test crypto with different IV. */
  @Test(timeout=120000)
  public void testCryptoIV() throws Exception {
    byte[] iv1 = iv.clone();
    
    // Counter base: Long.MAX_VALUE
    setCounterBaseForIV(iv1, Long.MAX_VALUE);
    cryptoCheck(iv1);
    
    // Counter base: Long.MAX_VALUE - 1
    setCounterBaseForIV(iv1, Long.MAX_VALUE - 1);
    cryptoCheck(iv1);
    
    // Counter base: Integer.MAX_VALUE
    setCounterBaseForIV(iv1, Integer.MAX_VALUE);
    cryptoCheck(iv1);
    
    // Counter base: 0
    setCounterBaseForIV(iv1, 0);
    cryptoCheck(iv1);
    
    // Counter base: -1
    setCounterBaseForIV(iv1, -1);
    cryptoCheck(iv1);
  }
  
  private void cryptoCheck(byte[] iv) throws Exception {
    OutputStream out = getOutputStream(defaultBufferSize, key, iv);
    writeData(out);
    
    InputStream in = getInputStream(defaultBufferSize, key, iv);
    readCheck(in);
    in.close();
  }
  
  private void setCounterBaseForIV(byte[] iv, long counterBase) {
    ByteBuffer buf = ByteBuffer.wrap(iv);
    buf.order(ByteOrder.BIG_ENDIAN);
    buf.putLong(iv.length - 8, counterBase);
  }
  
  /**
   * Test hflush/hsync of crypto output stream, and with different buffer size.
   */
  @Test(timeout=120000)
  public void testSyncable() throws IOException {
    syncableCheck();
  }
  
  private void syncableCheck() throws IOException {
    OutputStream out = getOutputStream(smallBufferSize);
    try {
      int bytesWritten = dataLen / 3;
      out.write(data, 0, bytesWritten);
      ((Syncable) out).hflush();
      
      InputStream in = getInputStream(defaultBufferSize);
      verify(in, bytesWritten, data);
      in.close();
      
      out.write(data, bytesWritten, dataLen - bytesWritten);
      ((Syncable) out).hsync();
      
      in = getInputStream(defaultBufferSize);
      verify(in, dataLen, data);
      in.close();
    } finally {
      out.close();
    }
  }
  
  private void verify(InputStream in, int bytesToVerify, 
      byte[] expectedBytes) throws IOException {
    final byte[] readBuf = new byte[bytesToVerify];
    readAll(in, readBuf, 0, bytesToVerify);
    for (int i = 0; i < bytesToVerify; i++) {
      Assert.assertEquals(expectedBytes[i], readBuf[i]);
    }
  }
  
  private int readAll(InputStream in, long pos, byte[] b, int off, int len) 
      throws IOException {
    int n = 0;
    int total = 0;
    while (n != -1) {
      total += n;
      if (total >= len) {
        break;
      }
      n = ((PositionedReadable) in).read(pos + total, b, off + total, 
          len - total);
    }
    
    return total;
  }
  
  /** Test positioned read. */
  @Test(timeout=120000)
  public void testPositionedRead() throws Exception {
    OutputStream out = getOutputStream(defaultBufferSize);
    writeData(out);
    
    InputStream in = getInputStream(defaultBufferSize);
    // Pos: 1/3 dataLen
    positionedReadCheck(in , dataLen / 3);

    // Pos: 1/2 dataLen
    positionedReadCheck(in, dataLen / 2);
    in.close();
  }
  
  private void positionedReadCheck(InputStream in, int pos) throws Exception {
    byte[] result = new byte[dataLen];
    int n = readAll(in, pos, result, 0, dataLen);
    
    Assert.assertEquals(dataLen, n + pos);
    byte[] readData = new byte[n];
    System.arraycopy(result, 0, readData, 0, n);
    byte[] expectedData = new byte[n];
    System.arraycopy(data, pos, expectedData, 0, n);
    Assert.assertArrayEquals(readData, expectedData);
  }
  
  /** Test read fully */
  @Test(timeout=120000)
  public void testReadFully() throws Exception {
    OutputStream out = getOutputStream(defaultBufferSize);
    writeData(out);
    
    InputStream in = getInputStream(defaultBufferSize);
    final int len1 = dataLen / 4;
    // Read len1 bytes
    byte[] readData = new byte[len1];
    readAll(in, readData, 0, len1);
    byte[] expectedData = new byte[len1];
    System.arraycopy(data, 0, expectedData, 0, len1);
    Assert.assertArrayEquals(readData, expectedData);
    
    // Pos: 1/3 dataLen
    readFullyCheck(in, dataLen / 3);
    
    // Read len1 bytes
    readData = new byte[len1];
    readAll(in, readData, 0, len1);
    expectedData = new byte[len1];
    System.arraycopy(data, len1, expectedData, 0, len1);
    Assert.assertArrayEquals(readData, expectedData);
    
    // Pos: 1/2 dataLen
    readFullyCheck(in, dataLen / 2);
    
    // Read len1 bytes
    readData = new byte[len1];
    readAll(in, readData, 0, len1);
    expectedData = new byte[len1];
    System.arraycopy(data, 2 * len1, expectedData, 0, len1);
    Assert.assertArrayEquals(readData, expectedData);
    
    in.close();
  }
  
  private void readFullyCheck(InputStream in, int pos) throws Exception {
    byte[] result = new byte[dataLen - pos];
    ((PositionedReadable) in).readFully(pos, result);
    
    byte[] expectedData = new byte[dataLen - pos];
    System.arraycopy(data, pos, expectedData, 0, dataLen - pos);
    Assert.assertArrayEquals(result, expectedData);
    
    result = new byte[dataLen]; // Exceeds maximum length 
    try {
      ((PositionedReadable) in).readFully(pos, result);
      Assert.fail("Read fully exceeds maximum length should fail.");
    } catch (IOException e) {
    }
  }
  
  /** Test seek to different position. */
  @Test(timeout=120000)
  public void testSeek() throws Exception {
    OutputStream out = getOutputStream(defaultBufferSize);
    writeData(out);
    
    InputStream in = getInputStream(defaultBufferSize);
    // Pos: 1/3 dataLen
    seekCheck(in, dataLen / 3);
    
    // Pos: 0
    seekCheck(in, 0);
    
    // Pos: 1/2 dataLen
    seekCheck(in, dataLen / 2);
    
    final long pos = ((Seekable) in).getPos();
    
    // Pos: -3
    try {
      seekCheck(in, -3);
      Assert.fail("Seek to negative offset should fail.");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("Cannot seek to negative " +
          "offset", e);
    }
    Assert.assertEquals(pos, ((Seekable) in).getPos());
    
    // Pos: dataLen + 3
    try {
      seekCheck(in, dataLen + 3);
      Assert.fail("Seek after EOF should fail.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Cannot seek after EOF", e);
    }
    Assert.assertEquals(pos, ((Seekable) in).getPos());
    
    in.close();
  }
  
  private void seekCheck(InputStream in, int pos) throws Exception {
    byte[] result = new byte[dataLen];
    ((Seekable) in).seek(pos);
    int n = readAll(in, result, 0, dataLen);
    
    Assert.assertEquals(dataLen, n + pos);
    byte[] readData = new byte[n];
    System.arraycopy(result, 0, readData, 0, n);
    byte[] expectedData = new byte[n];
    System.arraycopy(data, pos, expectedData, 0, n);
    Assert.assertArrayEquals(readData, expectedData);
  }
  
  /** Test get position. */
  @Test(timeout=120000)
  public void testGetPos() throws Exception {
    OutputStream out = getOutputStream(defaultBufferSize);
    writeData(out);
    
    // Default buffer size
    InputStream in = getInputStream(defaultBufferSize);
    byte[] result = new byte[dataLen];
    int n1 = readAll(in, result, 0, dataLen / 3);
    Assert.assertEquals(n1, ((Seekable) in).getPos());
    
    int n2 = readAll(in, result, n1, dataLen - n1);
    Assert.assertEquals(n1 + n2, ((Seekable) in).getPos());
    in.close();
  }
  
  @Test(timeout=120000)
  public void testAvailable() throws Exception {
    OutputStream out = getOutputStream(defaultBufferSize);
    writeData(out);
    
    // Default buffer size
    InputStream in = getInputStream(defaultBufferSize);
    byte[] result = new byte[dataLen];
    int n1 = readAll(in, result, 0, dataLen / 3);
    Assert.assertEquals(in.available(), dataLen - n1);
    
    int n2 = readAll(in, result, n1, dataLen - n1);
    Assert.assertEquals(in.available(), dataLen - n1 - n2);
    in.close();
  }
  
  /** Test skip. */
  @Test(timeout=120000)
  public void testSkip() throws Exception {
    OutputStream out = getOutputStream(defaultBufferSize);
    writeData(out);
        
    // Default buffer size
    InputStream in = getInputStream(defaultBufferSize);
    byte[] result = new byte[dataLen];
    int n1 = readAll(in, result, 0, dataLen / 3);
    Assert.assertEquals(n1, ((Seekable) in).getPos());
    
    long skipped = in.skip(dataLen / 3);
    int n2 = readAll(in, result, 0, dataLen);
    
    Assert.assertEquals(dataLen, n1 + skipped + n2);
    byte[] readData = new byte[n2];
    System.arraycopy(result, 0, readData, 0, n2);
    byte[] expectedData = new byte[n2];
    System.arraycopy(data, dataLen - n2, expectedData, 0, n2);
    Assert.assertArrayEquals(readData, expectedData);
    
    try {
      skipped = in.skip(-3);
      Assert.fail("Skip Negative length should fail.");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("Negative skip length", e);
    }
    
    // Skip after EOF
    skipped = in.skip(3);
    Assert.assertEquals(skipped, 0);
    
    in.close();
  }
  
  private void byteBufferReadCheck(InputStream in, ByteBuffer buf, 
      int bufPos) throws Exception {
    buf.position(bufPos);
    int n = ((ByteBufferReadable) in).read(buf);
    Assert.assertEquals(bufPos + n, buf.position());
    byte[] readData = new byte[n];
    buf.rewind();
    buf.position(bufPos);
    buf.get(readData);
    byte[] expectedData = new byte[n];
    System.arraycopy(data, 0, expectedData, 0, n);
    Assert.assertArrayEquals(readData, expectedData);
  }
  
  /** Test byte buffer read with different buffer size. */
  @Test(timeout=120000)
  public void testByteBufferRead() throws Exception {
    OutputStream out = getOutputStream(defaultBufferSize);
    writeData(out);
    
    // Default buffer size, initial buffer position is 0
    InputStream in = getInputStream(defaultBufferSize);
    ByteBuffer buf = ByteBuffer.allocate(dataLen + 100);
    byteBufferReadCheck(in, buf, 0);
    in.close();
    
    // Default buffer size, initial buffer position is not 0
    in = getInputStream(defaultBufferSize);
    buf.clear();
    byteBufferReadCheck(in, buf, 11);
    in.close();
    
    // Small buffer size, initial buffer position is 0
    in = getInputStream(smallBufferSize);
    buf.clear();
    byteBufferReadCheck(in, buf, 0);
    in.close();
    
    // Small buffer size, initial buffer position is not 0
    in = getInputStream(smallBufferSize);
    buf.clear();
    byteBufferReadCheck(in, buf, 11);
    in.close();
    
    // Direct buffer, default buffer size, initial buffer position is 0
    in = getInputStream(defaultBufferSize);
    buf = ByteBuffer.allocateDirect(dataLen + 100);
    byteBufferReadCheck(in, buf, 0);
    in.close();
    
    // Direct buffer, default buffer size, initial buffer position is not 0
    in = getInputStream(defaultBufferSize);
    buf.clear();
    byteBufferReadCheck(in, buf, 11);
    in.close();
    
    // Direct buffer, small buffer size, initial buffer position is 0
    in = getInputStream(smallBufferSize);
    buf.clear();
    byteBufferReadCheck(in, buf, 0);
    in.close();
    
    // Direct buffer, small buffer size, initial buffer position is not 0
    in = getInputStream(smallBufferSize);
    buf.clear();
    byteBufferReadCheck(in, buf, 11);
    in.close();
  }
  
  @Test(timeout=120000)
  public void testCombinedOp() throws Exception {
    OutputStream out = getOutputStream(defaultBufferSize);
    writeData(out);
    
    final int len1 = dataLen / 8;
    final int len2 = dataLen / 10;
    
    InputStream in = getInputStream(defaultBufferSize);
    // Read len1 data.
    byte[] readData = new byte[len1];
    readAll(in, readData, 0, len1);
    byte[] expectedData = new byte[len1];
    System.arraycopy(data, 0, expectedData, 0, len1);
    Assert.assertArrayEquals(readData, expectedData);
    
    long pos = ((Seekable) in).getPos();
    Assert.assertEquals(len1, pos);
    
    // Seek forward len2
    ((Seekable) in).seek(pos + len2);
    // Skip forward len2
    long n = in.skip(len2);
    Assert.assertEquals(len2, n);
    
    // Pos: 1/4 dataLen
    positionedReadCheck(in , dataLen / 4);
    
    // Pos should be len1 + len2 + len2
    pos = ((Seekable) in).getPos();
    Assert.assertEquals(len1 + len2 + len2, pos);
    
    // Read forward len1
    ByteBuffer buf = ByteBuffer.allocate(len1);
    int nRead = ((ByteBufferReadable) in).read(buf);
    Assert.assertEquals(nRead, buf.position());
    readData = new byte[nRead];
    buf.rewind();
    buf.get(readData);
    expectedData = new byte[nRead];
    System.arraycopy(data, (int)pos, expectedData, 0, nRead);
    Assert.assertArrayEquals(readData, expectedData);
    
    long lastPos = pos;
    // Pos should be lastPos + nRead
    pos = ((Seekable) in).getPos();
    Assert.assertEquals(lastPos + nRead, pos);
    
    // Pos: 1/3 dataLen
    positionedReadCheck(in , dataLen / 3);
    
    // Read forward len1
    readData = new byte[len1];
    readAll(in, readData, 0, len1);
    expectedData = new byte[len1];
    System.arraycopy(data, (int)pos, expectedData, 0, len1);
    Assert.assertArrayEquals(readData, expectedData);
    
    lastPos = pos;
    // Pos should be lastPos + len1
    pos = ((Seekable) in).getPos();
    Assert.assertEquals(lastPos + len1, pos);
    
    // Read forward len1
    buf = ByteBuffer.allocate(len1);
    nRead = ((ByteBufferReadable) in).read(buf);
    Assert.assertEquals(nRead, buf.position());
    readData = new byte[nRead];
    buf.rewind();
    buf.get(readData);
    expectedData = new byte[nRead];
    System.arraycopy(data, (int)pos, expectedData, 0, nRead);
    Assert.assertArrayEquals(readData, expectedData);
    
    lastPos = pos;
    // Pos should be lastPos + nRead
    pos = ((Seekable) in).getPos();
    Assert.assertEquals(lastPos + nRead, pos);
    
    // ByteBuffer read after EOF
    ((Seekable) in).seek(dataLen);
    buf.clear();
    n = ((ByteBufferReadable) in).read(buf);
    Assert.assertEquals(n, -1);
    
    in.close();
  }
  
  @Test(timeout=120000)
  public void testSeekToNewSource() throws Exception {
    OutputStream out = getOutputStream(defaultBufferSize);
    writeData(out);
    
    InputStream in = getInputStream(defaultBufferSize);
    
    final int len1 = dataLen / 8;
    byte[] readData = new byte[len1];
    readAll(in, readData, 0, len1);
    
    // Pos: 1/3 dataLen
    seekToNewSourceCheck(in, dataLen / 3);
    
    // Pos: 0
    seekToNewSourceCheck(in, 0);
    
    // Pos: 1/2 dataLen
    seekToNewSourceCheck(in, dataLen / 2);
    
    // Pos: -3
    try {
      seekToNewSourceCheck(in, -3);
      Assert.fail("Seek to negative offset should fail.");
    } catch (IllegalArgumentException e) {
      GenericTestUtils.assertExceptionContains("Cannot seek to negative " +
          "offset", e);
    }
    
    // Pos: dataLen + 3
    try {
      seekToNewSourceCheck(in, dataLen + 3);
      Assert.fail("Seek after EOF should fail.");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Attempted to read past " +
          "end of file", e);
    }
    
    in.close();
  }
  
  private void seekToNewSourceCheck(InputStream in, int targetPos) 
      throws Exception {
    byte[] result = new byte[dataLen];
    ((Seekable) in).seekToNewSource(targetPos);
    int n = readAll(in, result, 0, dataLen);
    
    Assert.assertEquals(dataLen, n + targetPos);
    byte[] readData = new byte[n];
    System.arraycopy(result, 0, readData, 0, n);
    byte[] expectedData = new byte[n];
    System.arraycopy(data, targetPos, expectedData, 0, n);
    Assert.assertArrayEquals(readData, expectedData);
  }
  
  private ByteBufferPool getBufferPool() {
    return new ByteBufferPool() {
      @Override
      public ByteBuffer getBuffer(boolean direct, int length) {
        return ByteBuffer.allocateDirect(length);
      }
      
      @Override
      public void putBuffer(ByteBuffer buffer) {
      }
    };
  }
  
  @Test(timeout=120000)
  public void testHasEnhancedByteBufferAccess() throws Exception {
    OutputStream out = getOutputStream(defaultBufferSize);
    writeData(out);
    
    InputStream in = getInputStream(defaultBufferSize);
    final int len1 = dataLen / 8;
    // ByteBuffer size is len1
    ByteBuffer buffer = ((HasEnhancedByteBufferAccess) in).read(
        getBufferPool(), len1, EnumSet.of(ReadOption.SKIP_CHECKSUMS));
    int n1 = buffer.remaining();
    byte[] readData = new byte[n1];
    buffer.get(readData);
    byte[] expectedData = new byte[n1];
    System.arraycopy(data, 0, expectedData, 0, n1);
    Assert.assertArrayEquals(readData, expectedData);
    ((HasEnhancedByteBufferAccess) in).releaseBuffer(buffer);
    
    // Read len1 bytes
    readData = new byte[len1];
    readAll(in, readData, 0, len1);
    expectedData = new byte[len1];
    System.arraycopy(data, n1, expectedData, 0, len1);
    Assert.assertArrayEquals(readData, expectedData);
    
    // ByteBuffer size is len1
    buffer = ((HasEnhancedByteBufferAccess) in).read(
        getBufferPool(), len1, EnumSet.of(ReadOption.SKIP_CHECKSUMS));
    int n2 = buffer.remaining();
    readData = new byte[n2];
    buffer.get(readData);
    expectedData = new byte[n2];
    System.arraycopy(data, n1 + len1, expectedData, 0, n2);
    Assert.assertArrayEquals(readData, expectedData);
    ((HasEnhancedByteBufferAccess) in).releaseBuffer(buffer);
    
    in.close();
  }
}

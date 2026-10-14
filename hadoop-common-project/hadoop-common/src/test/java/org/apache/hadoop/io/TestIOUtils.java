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

package org.apache.hadoop.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.HttpURLConnection;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test cases for IOUtils.java
 */
public class TestIOUtils {
  private static final String TEST_FILE_NAME = "test_file";
  private static final Logger LOG = LoggerFactory.getLogger(TestIOUtils.class);
  
  @Test
  public void testCopyBytesShouldCloseStreamsWhenCloseIsTrue() throws Exception {
    InputStream inputStream = mock(InputStream.class);
    OutputStream outputStream = mock(OutputStream.class);
    doReturn(-1).when(inputStream).read(new byte[1]);
    IOUtils.copyBytes(inputStream, outputStream, 1, true);
    verify(inputStream, atLeastOnce()).close();
    verify(outputStream, atLeastOnce()).close();
  }

  @Test
  public void testCopyBytesShouldCloseInputSteamWhenOutputStreamCloseThrowsException()
      throws Exception {
    InputStream inputStream = mock(InputStream.class);
    OutputStream outputStream = mock(OutputStream.class);
    doReturn(-1).when(inputStream).read(new byte[1]);
    doThrow(new IOException()).when(outputStream).close();
    try{
      IOUtils.copyBytes(inputStream, outputStream, 1, true);
    } catch (IOException e) {
    }
    verify(inputStream, atLeastOnce()).close();
    verify(outputStream, atLeastOnce()).close();
  }

  @Test
  public void testCopyBytesShouldCloseInputSteamWhenOutputStreamCloseThrowsRunTimeException()
      throws Exception {
    InputStream inputStream = mock(InputStream.class);
    OutputStream outputStream = mock(OutputStream.class);
    doReturn(-1).when(inputStream).read(new byte[1]);
    doThrow(new RuntimeException()).when(outputStream).close();
    try {
      IOUtils.copyBytes(inputStream, outputStream, 1, true);
      fail("Didn't throw exception");
    } catch (RuntimeException e) {
    }
    verify(outputStream, atLeastOnce()).close();
  }

  @Test
  public void testCopyBytesShouldCloseInputSteamWhenInputStreamCloseThrowsRunTimeException()
      throws Exception {
    InputStream inputStream = mock(InputStream.class);
    OutputStream outputStream = mock(OutputStream.class);
    doReturn(-1).when(inputStream).read(new byte[1]);
    doThrow(new RuntimeException()).when(inputStream).close();
    try {
      IOUtils.copyBytes(inputStream, outputStream, 1, true);
      fail("Didn't throw exception");
    } catch (RuntimeException e) {
    }
    verify(inputStream, atLeastOnce()).close();
  }

  @Test
  public void testCopyBytesShouldNotCloseStreamsWhenCloseIsFalse()
      throws Exception {
    InputStream inputStream = mock(InputStream.class);
    OutputStream outputStream = mock(OutputStream.class);
    doReturn(-1).when(inputStream).read(new byte[1]);
    IOUtils.copyBytes(inputStream, outputStream, 1, false);
    verify(inputStream, atMost(0)).close();
    verify(outputStream, atMost(0)).close();
  }

  @Test
  public void testCopyBytesWithCountShouldCloseStreamsWhenCloseIsTrue()
      throws Exception {
    InputStream inputStream = mock(InputStream.class);
    OutputStream outputStream = mock(OutputStream.class);
    doReturn(-1).when(inputStream).read(new byte[4096], 0, 1);
    IOUtils.copyBytes(inputStream, outputStream, (long) 1, true);
    verify(inputStream, atLeastOnce()).close();
    verify(outputStream, atLeastOnce()).close();
  }

  @Test
  public void testCopyBytesWithCountShouldNotCloseStreamsWhenCloseIsFalse()
      throws Exception {
    InputStream inputStream = mock(InputStream.class);
    OutputStream outputStream = mock(OutputStream.class);
    doReturn(-1).when(inputStream).read(new byte[4096], 0, 1);
    IOUtils.copyBytes(inputStream, outputStream, (long) 1, false);
    verify(inputStream, atMost(0)).close();
    verify(outputStream, atMost(0)).close();
  }

  @Test
  public void testCopyBytesWithCountShouldThrowOutTheStreamClosureExceptions()
      throws Exception {
    InputStream inputStream = mock(InputStream.class);
    OutputStream outputStream = mock(OutputStream.class);
    doReturn(-1).when(inputStream).read(new byte[4096], 0, 1);
    doThrow(new IOException("Exception in closing the stream")).when(
        outputStream).close();
    try {
      IOUtils.copyBytes(inputStream, outputStream, (long) 1, true);
      fail("Should throw out the exception");
    } catch (IOException e) {
      assertEquals("Exception in closing the stream", e.getMessage(),
          "Not throwing the expected exception.");
    }
    verify(inputStream, atLeastOnce()).close();
    verify(outputStream, atLeastOnce()).close();
  }

  @Test
  public void testWriteFully() throws IOException {
    final int INPUT_BUFFER_LEN = 10000;
    final int HALFWAY = 1 + (INPUT_BUFFER_LEN / 2);
    byte[] input = new byte[INPUT_BUFFER_LEN];
    for (int i = 0; i < input.length; i++) {
      input[i] = (byte)(i & 0xff);
    }
    byte[] output = new byte[input.length];
    
    try {
      RandomAccessFile raf = new RandomAccessFile(TEST_FILE_NAME, "rw");
      FileChannel fc = raf.getChannel();
      ByteBuffer buf = ByteBuffer.wrap(input);
      IOUtils.writeFully(fc, buf);
      raf.seek(0);
      raf.read(output);
      for (int i = 0; i < input.length; i++) {
        assertEquals(input[i], output[i]);
      }
      buf.rewind();
      IOUtils.writeFully(fc, buf, HALFWAY);
      for (int i = 0; i < HALFWAY; i++) {
        assertEquals(input[i], output[i]);
      }
      raf.seek(0);
      raf.read(output);
      for (int i = HALFWAY; i < input.length; i++) {
        assertEquals(input[i - HALFWAY], output[i]);
      }
      raf.close();
    } finally {
      File f = new File(TEST_FILE_NAME);
      if (f.exists()) {
        f.delete();
      }
    }
  }

  @Test
  public void testWrappedReadForCompressedData() throws IOException {
    byte[] buf = new byte[2];
    InputStream mockStream = mock(InputStream.class);
    when(mockStream.read(buf, 0, 1)).thenReturn(1);
    when(mockStream.read(buf, 0, 2)).thenThrow(
        new java.lang.InternalError());

    try {
      assertEquals(1, IOUtils.wrappedReadForCompressedData(mockStream, buf, 0, 1),
          "Check expected value");
    } catch (IOException ioe) {
      fail("Unexpected error while reading");
    }
    try {
      IOUtils.wrappedReadForCompressedData(mockStream, buf, 0, 2);
    } catch (IOException ioe) {
      GenericTestUtils.assertExceptionContains(
          "Error while reading compressed data", ioe);
    }
  }

  @Test
  public void testSkipFully() throws IOException {
    byte inArray[] = new byte[] {0, 1, 2, 3, 4};
    ByteArrayInputStream in = new ByteArrayInputStream(inArray);
    try {
      in.mark(inArray.length);
      IOUtils.skipFully(in, 2);
      IOUtils.skipFully(in, 2);
      try {
        IOUtils.skipFully(in, 2);
        fail("expected to get a PrematureEOFException");
      } catch (EOFException e) {
        assertEquals("Premature EOF from inputStream " +
                "after skipping 1 byte(s).",e.getMessage());
      }
      in.reset();
      try {
        IOUtils.skipFully(in, 20);
        fail("expected to get a PrematureEOFException");
      } catch (EOFException e) {
        assertEquals("Premature EOF from inputStream " +
                "after skipping 5 byte(s).",e.getMessage());
      }
      in.reset();
      IOUtils.skipFully(in, 5);
      try {
        IOUtils.skipFully(in, 10);
        fail("expected to get a PrematureEOFException");
      } catch (EOFException e) {
        assertEquals("Premature EOF from inputStream " +
                "after skipping 0 byte(s).",e.getMessage());
      }
    } finally {
      in.close();
    }
  }

  private enum NoEntry3Filter implements FilenameFilter {
    INSTANCE;

    @Override
    public boolean accept(File dir, String name) {
      return !name.equals("entry3");
    }
  }

  @Test
  public void testListDirectory() throws IOException {
    File dir = new File("testListDirectory");
    Files.createDirectory(dir.toPath());
    try {
      Set<String> entries = new HashSet<>();
      entries.add("entry1");
      entries.add("entry2");
      entries.add("entry3");
      for (String entry : entries) {
        Files.createDirectory(new File(dir, entry).toPath());
      }
      List<String> list = IOUtils.listDirectory(dir,
          NoEntry3Filter.INSTANCE);
      for (String entry : list) {
        assertTrue(entries.remove(entry));
      }
      assertTrue(entries.contains("entry3"));
      list = IOUtils.listDirectory(dir, null);
      for (String entry : list) {
        entries.remove(entry);
      }
      assertTrue(entries.isEmpty());
    } finally {
      FileUtils.deleteDirectory(dir);
    }
  }

  @Test
  public void testCloseStreams() throws IOException {
    File tmpFile = null;
    FileOutputStream fos;
    BufferedOutputStream bos;
    FileOutputStream nullStream = null;

    try {
      tmpFile = new File(GenericTestUtils.getTestDir(), "testCloseStreams.txt");
      fos = new FileOutputStream(tmpFile) {
        @Override
        public void close() throws IOException {
          throw new IOException();
        }
      };
      bos = new BufferedOutputStream(
          new FileOutputStream(tmpFile)) {
        @Override
        public void close() {
          throw new NullPointerException();
        }
      };

      IOUtils.closeStreams(fos, bos, nullStream);
      IOUtils.closeStreams();
    } finally {
      FileUtils.deleteQuietly(tmpFile);
    }

  }

  @Test
  public void testWrapException() throws Exception {
    // Test for IOException with valid (String) constructor
    LambdaTestUtils.intercept(EOFException.class,
        "Failed with java.io.EOFException while processing file/directory "
            + ":[/tmp/abc.txt] in method:[testWrapException]", () -> {
          throw IOUtils.wrapException("/tmp/abc.txt", "testWrapException",
              new EOFException("EOFException "));
        });

    // Test for IOException with  no (String) constructor
    PathIOException returnedEx = LambdaTestUtils
        .intercept(PathIOException.class, "Input/output error:",
            () -> {
              throw IOUtils.wrapException("/tmp/abc.txt", "testWrapEx",
                  new CharacterCodingException());
            });
    assertEquals("/tmp/abc.txt", returnedEx.getPath().toString());
  }

  @Test
  public void testDrainAndCloseNullStream() {
    IOUtils.drainAndClose((InputStream) null);
  }

  @Test
  public void testDrainAndCloseConsumesUntilEofAndCloses() throws Exception {
    byte[] payload = "json{}trailing-bytes".getBytes(StandardCharsets.UTF_8);
    TrackingInputStream in = new TrackingInputStream(payload);
    in.read(new byte[6]);
    IOUtils.drainAndClose(in);
    assertTrue(in.isClosed());
    assertEquals(payload.length, in.getTotalRead());
  }

  @Test
  public void testDrainAndCloseSwallowsReadIOException() throws Exception {
    InputStream in = mock(InputStream.class);
    when(in.read(any(byte[].class))).thenThrow(new IOException("read failed"));
    IOUtils.drainAndClose(in);
    verify(in).close();
  }

  @Test
  public void testDrainAndCloseHttpUrlConnectionPrefersErrorStream()
      throws Exception {
    HttpURLConnection conn = mock(HttpURLConnection.class);
    byte[] body = "error-body".getBytes(StandardCharsets.UTF_8);
    when(conn.getErrorStream()).thenReturn(new ByteArrayInputStream(body));
    IOUtils.drainAndClose(conn);
    verify(conn, never()).getInputStream();
  }

  @Test
  public void testDrainAndCloseHttpUrlConnectionWhenInputStreamThrows()
      throws Exception {
    HttpURLConnection conn = mock(HttpURLConnection.class);
    when(conn.getErrorStream()).thenReturn(null);
    when(conn.getInputStream()).thenThrow(new IOException("no body"));
    IOUtils.drainAndClose(conn);
  }

  @Test
  public void testDrainAndCloseRespectsMaxBytes() throws Exception {
    InfiniteInputStream in = new InfiniteInputStream();
    IOUtils.drainAndClose(in);
    assertTrue(in.isClosed());
    assertTrue(in.getTotalRead() >= 8L * 1024 * 1024);
    assertTrue(in.getTotalRead() <= 8L * 1024 * 1024 + 4096);
  }

  private static final class TrackingInputStream extends InputStream {
    private final byte[] data;
    private int pos;
    private int totalRead;
    private boolean closed;

    TrackingInputStream(byte[] data) {
      this.data = data;
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      int n = read(one, 0, 1);
      return n < 0 ? -1 : one[0] & 0xff;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (pos >= data.length) {
        return -1;
      }
      int n = Math.min(len, data.length - pos);
      System.arraycopy(data, pos, b, off, n);
      pos += n;
      totalRead += n;
      return n;
    }

    boolean isClosed() {
      return closed;
    }

    int getTotalRead() {
      return totalRead;
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }
  }

  private static final class InfiniteInputStream extends InputStream {
    private long totalRead;
    private boolean closed;

    @Override
    public int read() {
      totalRead++;
      return 0;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      int n = Math.min(len, 4096);
      totalRead += n;
      return n;
    }

    long getTotalRead() {
      return totalRead;
    }

    boolean isClosed() {
      return closed;
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }
  }
}

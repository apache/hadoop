/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.ozone.client.io.ChunkGroupInputStream;
import org.apache.hadoop.ozone.client.io.ChunkGroupOutputStream;
import org.apache.hadoop.hdds.scm.storage.ChunkInputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

/**
 * This class tests ChunkGroupInputStream and ChunkGroupOutStream.
 */
public class TestChunkStreams {

  @Rule
  public ExpectedException exception = ExpectedException.none();

  /**
   * This test uses ByteArrayOutputStream as the underlying stream to test
   * the correctness of ChunkGroupOutputStream.
   *
   * @throws Exception
   */
  @Test
  public void testWriteGroupOutputStream() throws Exception {
    try (ChunkGroupOutputStream groupOutputStream =
             new ChunkGroupOutputStream()) {
      ArrayList<OutputStream> outputStreams = new ArrayList<>();

      // 5 byte streams, each 100 bytes. write 500 bytes means writing to each
      // of them with 100 bytes.
      for (int i = 0; i < 5; i++) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(100);
        outputStreams.add(out);
        groupOutputStream.addStream(out, 100);
      }
      assertEquals(0, groupOutputStream.getByteOffset());

      String dataString = RandomStringUtils.randomAscii(500);
      byte[] data = dataString.getBytes();
      groupOutputStream.write(data, 0, data.length);
      assertEquals(500, groupOutputStream.getByteOffset());

      String res = "";
      int offset = 0;
      for (OutputStream stream : outputStreams) {
        String subString = stream.toString();
        res += subString;
        assertEquals(dataString.substring(offset, offset + 100), subString);
        offset += 100;
      }
      assertEquals(dataString, res);
    }
  }

  @Test
  public void testErrorWriteGroupOutputStream() throws Exception {
    try (ChunkGroupOutputStream groupOutputStream =
             new ChunkGroupOutputStream()) {
      ArrayList<OutputStream> outputStreams = new ArrayList<>();

      // 5 byte streams, each 100 bytes. write 500 bytes means writing to each
      // of them with 100 bytes. all 5 streams makes up a ChunkGroupOutputStream
      // with a total of 500 bytes in size
      for (int i = 0; i < 5; i++) {
        ByteArrayOutputStream out = new ByteArrayOutputStream(100);
        outputStreams.add(out);
        groupOutputStream.addStream(out, 100);
      }
      assertEquals(0, groupOutputStream.getByteOffset());

      // first writes of 100 bytes should succeed
      groupOutputStream.write(RandomStringUtils.randomAscii(100).getBytes());
      assertEquals(100, groupOutputStream.getByteOffset());

      // second writes of 500 bytes should fail, as there should be only 400
      // bytes space left
      // TODO : if we decide to take the 400 bytes instead in the future,
      // other add more informative error code rather than exception, need to
      // change this part.
      exception.expect(Exception.class);
      groupOutputStream.write(RandomStringUtils.randomAscii(500).getBytes());
      assertEquals(100, groupOutputStream.getByteOffset());
    }
  }

  @Test
  public void testReadGroupInputStream() throws Exception {
    try (ChunkGroupInputStream groupInputStream = new ChunkGroupInputStream()) {
      ArrayList<ChunkInputStream> inputStreams = new ArrayList<>();

      String dataString = RandomStringUtils.randomAscii(500);
      byte[] buf = dataString.getBytes();
      int offset = 0;
      for (int i = 0; i < 5; i++) {
        int tempOffset = offset;
        ChunkInputStream in =
            new ChunkInputStream(null, null, null, new ArrayList<>(), null) {
              private long pos = 0;
              private ByteArrayInputStream in =
                  new ByteArrayInputStream(buf, tempOffset, 100);

              @Override
              public void seek(long pos) throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public long getPos() throws IOException {
                return pos;
              }

              @Override
              public boolean seekToNewSource(long targetPos)
                  throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public int read() throws IOException {
                return in.read();
              }

              @Override
              public int read(byte[] b, int off, int len) throws IOException {
                int readLen = in.read(b, off, len);
                pos += readLen;
                return readLen;
              }
            };
        inputStreams.add(in);
        offset += 100;
        groupInputStream.addStream(in, 100);
      }

      byte[] resBuf = new byte[500];
      int len = groupInputStream.read(resBuf, 0, 500);

      assertEquals(500, len);
      assertEquals(dataString, new String(resBuf));
    }
  }

  @Test
  public void testErrorReadGroupInputStream() throws Exception {
    try (ChunkGroupInputStream groupInputStream = new ChunkGroupInputStream()) {
      ArrayList<ChunkInputStream> inputStreams = new ArrayList<>();

      String dataString = RandomStringUtils.randomAscii(500);
      byte[] buf = dataString.getBytes();
      int offset = 0;
      for (int i = 0; i < 5; i++) {
        int tempOffset = offset;
        ChunkInputStream in =
            new ChunkInputStream(null, null, null, new ArrayList<>(), null) {
              private long pos = 0;
              private ByteArrayInputStream in =
                  new ByteArrayInputStream(buf, tempOffset, 100);

              @Override
              public void seek(long pos) throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public long getPos() throws IOException {
                return pos;
              }

              @Override
              public boolean seekToNewSource(long targetPos)
                  throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public int read() throws IOException {
                return in.read();
              }

              @Override
              public int read(byte[] b, int off, int len) throws IOException {
                int readLen = in.read(b, off, len);
                pos += readLen;
                return readLen;
              }
            };
        inputStreams.add(in);
        offset += 100;
        groupInputStream.addStream(in, 100);
      }

      byte[] resBuf = new byte[600];
      // read 300 bytes first
      int len = groupInputStream.read(resBuf, 0, 340);
      assertEquals(3, groupInputStream.getCurrentStreamIndex());
      assertEquals(60, groupInputStream.getRemainingOfIndex(3));
      assertEquals(340, len);
      assertEquals(dataString.substring(0, 340),
          new String(resBuf).substring(0, 340));

      // read following 300 bytes, but only 200 left
      len = groupInputStream.read(resBuf, 340, 260);
      assertEquals(5, groupInputStream.getCurrentStreamIndex());
      assertEquals(0, groupInputStream.getRemainingOfIndex(4));
      assertEquals(160, len);
      assertEquals(dataString, new String(resBuf).substring(0, 500));

      // further read should get EOF
      len = groupInputStream.read(resBuf, 0, 1);
      // reached EOF, further read should get -1
      assertEquals(-1, len);
    }
  }
}

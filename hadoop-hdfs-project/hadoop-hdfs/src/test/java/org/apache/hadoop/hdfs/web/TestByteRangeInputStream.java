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
package org.apache.hadoop.hdfs.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import com.google.common.net.HttpHeaders;
import org.apache.hadoop.hdfs.web.ByteRangeInputStream.InputStreamAndFileLength;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

public class TestByteRangeInputStream {
  private class ByteRangeInputStreamImpl extends ByteRangeInputStream {
    public ByteRangeInputStreamImpl(URLOpener o, URLOpener r)
        throws IOException {
      super(o, r);
    }

    @Override
    protected URL getResolvedUrl(HttpURLConnection connection)
        throws IOException {
      return new URL("http://resolvedurl/");
    }
  }

  private ByteRangeInputStream.URLOpener getMockURLOpener(URL url)
      throws IOException {
    ByteRangeInputStream.URLOpener opener =
        mock(ByteRangeInputStream.URLOpener.class, CALLS_REAL_METHODS);
    opener.setURL(url);
    doReturn(getMockConnection("65535"))
        .when(opener).connect(anyLong(), anyBoolean());
    return opener;
  }

  private HttpURLConnection getMockConnection(String length)
      throws IOException {
    HttpURLConnection mockConnection = mock(HttpURLConnection.class);
    doReturn(new ByteArrayInputStream("asdf".getBytes()))
        .when(mockConnection).getInputStream();
    doReturn(length).when(mockConnection)
        .getHeaderField(HttpHeaders.CONTENT_LENGTH);
    return mockConnection;
  }

  @Test
  public void testByteRange() throws IOException {
    ByteRangeInputStream.URLOpener oMock = getMockURLOpener(
        new URL("http://test"));
    ByteRangeInputStream.URLOpener rMock = getMockURLOpener(null);
    ByteRangeInputStream bris = new ByteRangeInputStreamImpl(oMock, rMock);

    bris.seek(0);

    assertEquals("getPos wrong", 0, bris.getPos());

    bris.read();

    assertEquals("Initial call made incorrectly (offset check)",
        0, bris.startPos);
    assertEquals("getPos should return 1 after reading one byte", 1,
        bris.getPos());
    verify(oMock, times(1)).connect(0, false);

    bris.read();

    assertEquals("getPos should return 2 after reading two bytes", 2,
        bris.getPos());
    // No additional connections should have been made (no seek)
    verify(oMock, times(1)).connect(0, false);

    rMock.setURL(new URL("http://resolvedurl/"));

    bris.seek(100);
    bris.read();

    assertEquals("Seek to 100 bytes made incorrectly (offset Check)",
        100, bris.startPos);
    assertEquals("getPos should return 101 after reading one byte", 101,
        bris.getPos());
    verify(rMock, times(1)).connect(100, true);

    bris.seek(101);
    bris.read();

    // Seek to 101 should not result in another request
    verify(rMock, times(1)).connect(100, true);
    verify(rMock, times(0)).connect(101, true);

    bris.seek(2500);
    bris.read();

    assertEquals("Seek to 2500 bytes made incorrectly (offset Check)",
        2500, bris.startPos);

    doReturn(getMockConnection(null))
        .when(rMock).connect(anyLong(), anyBoolean());
    bris.seek(500);
    try {
      bris.read();
      fail("Exception should be thrown when content-length is not given");
    } catch (IOException e) {
      assertTrue("Incorrect response message: " + e.getMessage(),
          e.getMessage().startsWith(HttpHeaders.CONTENT_LENGTH +
                                    " is missing: "));
    }
    bris.close();
  }

  @Test
  public void testPropagatedClose() throws IOException {
    ByteRangeInputStream bris =
        mock(ByteRangeInputStream.class, CALLS_REAL_METHODS);
    InputStreamAndFileLength mockStream = new InputStreamAndFileLength(1L,
        mock(InputStream.class));
    doReturn(mockStream).when(bris).openInputStream(Mockito.anyLong());
    Whitebox.setInternalState(bris, "status",
                              ByteRangeInputStream.StreamStatus.SEEK);

    int brisOpens = 0;
    int brisCloses = 0;
    int isCloses = 0;

    // first open, shouldn't close underlying stream
    bris.getInputStream();
    verify(bris, times(++brisOpens)).openInputStream(Mockito.anyLong());
    verify(bris, times(brisCloses)).close();
    verify(mockStream.in, times(isCloses)).close();

    // stream is open, shouldn't close underlying stream
    bris.getInputStream();
    verify(bris, times(brisOpens)).openInputStream(Mockito.anyLong());
    verify(bris, times(brisCloses)).close();
    verify(mockStream.in, times(isCloses)).close();

    // seek forces a reopen, should close underlying stream
    bris.seek(1);
    bris.getInputStream();
    verify(bris, times(++brisOpens)).openInputStream(Mockito.anyLong());
    verify(bris, times(brisCloses)).close();
    verify(mockStream.in, times(++isCloses)).close();

    // verify that the underlying stream isn't closed after a seek
    // ie. the state was correctly updated
    bris.getInputStream();
    verify(bris, times(brisOpens)).openInputStream(Mockito.anyLong());
    verify(bris, times(brisCloses)).close();
    verify(mockStream.in, times(isCloses)).close();

    // seeking to same location should be a no-op
    bris.seek(1);
    bris.getInputStream();
    verify(bris, times(brisOpens)).openInputStream(Mockito.anyLong());
    verify(bris, times(brisCloses)).close();
    verify(mockStream.in, times(isCloses)).close();

    // close should of course close
    bris.close();
    verify(bris, times(++brisCloses)).close();
    verify(mockStream.in, times(++isCloses)).close();

    // it's already closed, underlying stream should not close
    bris.close();
    verify(bris, times(++brisCloses)).close();
    verify(mockStream.in, times(isCloses)).close();

    // it's closed, don't reopen it
    boolean errored = false;
    try {
      bris.getInputStream();
    } catch (IOException e) {
      errored = true;
      assertEquals("Stream closed", e.getMessage());
    } finally {
      assertTrue("Read a closed steam", errored);
    }
    verify(bris, times(brisOpens)).openInputStream(Mockito.anyLong());
    verify(bris, times(brisCloses)).close();

    verify(mockStream.in, times(isCloses)).close();
  }


  @Test
  public void testAvailable() throws IOException {
    ByteRangeInputStream bris =
            mock(ByteRangeInputStream.class, CALLS_REAL_METHODS);
    InputStreamAndFileLength mockStream = new InputStreamAndFileLength(65535L,
            mock(InputStream.class));
    doReturn(mockStream).when(bris).openInputStream(Mockito.anyLong());
    Whitebox.setInternalState(bris, "status",
            ByteRangeInputStream.StreamStatus.SEEK);


    assertEquals("Before read or seek, available should be same as filelength",
            65535, bris.available());
    verify(bris, times(1)).openInputStream(Mockito.anyLong());

    bris.seek(10);
    assertEquals("Seek 10 bytes, available should return filelength - 10"
            , 65525,
            bris.available());

    //no more bytes available
    bris.seek(65535);
    assertEquals("Seek till end of file, available should return 0 bytes", 0,
            bris.available());

    //test reads, seek back to 0 and start reading
    bris.seek(0);
    bris.read();
    assertEquals("Read 1 byte, available must return  filelength - 1",
            65534, bris.available());

    bris.read();
    assertEquals("Read another 1 byte, available must return  filelength - 2",
            65533, bris.available());

    //seek and read
    bris.seek(100);
    bris.read();
    assertEquals("Seek to offset 100 and read 1 byte, available should return filelength - 101",
            65434, bris.available());
    bris.close();
  }

  @Test
  public void testAvailableLengthNotKnown() throws IOException {
    ByteRangeInputStream bris =
            mock(ByteRangeInputStream.class, CALLS_REAL_METHODS);
    //Length is null for chunked transfer-encoding
    InputStreamAndFileLength mockStream = new InputStreamAndFileLength(null,
            mock(InputStream.class));
    doReturn(mockStream).when(bris).openInputStream(Mockito.anyLong());
    Whitebox.setInternalState(bris, "status",
            ByteRangeInputStream.StreamStatus.SEEK);

    assertEquals(Integer.MAX_VALUE, bris.available());
  }

  @Test
  public void testAvailableStreamClosed() throws IOException {
    ByteRangeInputStream bris =
            mock(ByteRangeInputStream.class, CALLS_REAL_METHODS);
    InputStreamAndFileLength mockStream = new InputStreamAndFileLength(null,
            mock(InputStream.class));
    doReturn(mockStream).when(bris).openInputStream(Mockito.anyLong());
    Whitebox.setInternalState(bris, "status",
            ByteRangeInputStream.StreamStatus.SEEK);

    bris.close();
    try{
      bris.available();
      fail("Exception should be thrown when stream is closed");
    }catch(IOException e){
      assertTrue("Exception when stream is closed",
              e.getMessage().equals("Stream closed"));
    }
  }

}

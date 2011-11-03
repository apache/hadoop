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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.TestByteRangeInputStream.MockHttpURLConnection;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem.OffsetUrlInputStream;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem.OffsetUrlOpener;
import org.junit.Test;

public class TestOffsetUrlInputStream {
  @Test
  public void testRemoveOffset() throws IOException {
    { //no offset
      String s = "http://test/Abc?Length=99";
      assertEquals(s, WebHdfsFileSystem.removeOffsetParam(new URL(s)).toString());
    }

    { //no parameters
      String s = "http://test/Abc";
      assertEquals(s, WebHdfsFileSystem.removeOffsetParam(new URL(s)).toString());
    }

    { //offset as first parameter
      String s = "http://test/Abc?offset=10&Length=99";
      assertEquals("http://test/Abc?Length=99",
          WebHdfsFileSystem.removeOffsetParam(new URL(s)).toString());
    }

    { //offset as second parameter
      String s = "http://test/Abc?op=read&OFFset=10&Length=99";
      assertEquals("http://test/Abc?op=read&Length=99",
          WebHdfsFileSystem.removeOffsetParam(new URL(s)).toString());
    }

    { //offset as last parameter
      String s = "http://test/Abc?Length=99&offset=10";
      assertEquals("http://test/Abc?Length=99",
          WebHdfsFileSystem.removeOffsetParam(new URL(s)).toString());
    }

    { //offset as the only parameter
      String s = "http://test/Abc?offset=10";
      assertEquals("http://test/Abc",
          WebHdfsFileSystem.removeOffsetParam(new URL(s)).toString());
    }
  }
  
  @Test
  public void testByteRange() throws Exception {
    final Configuration conf = new Configuration(); 
    final String uri = WebHdfsFileSystem.SCHEME  + "://localhost:50070/";
    final WebHdfsFileSystem webhdfs = (WebHdfsFileSystem)FileSystem.get(new URI(uri), conf);

    OffsetUrlOpener ospy = spy(webhdfs.new OffsetUrlOpener(new URL("http://test/")));
    doReturn(new MockHttpURLConnection(ospy.getURL())).when(ospy)
        .openConnection();
    OffsetUrlOpener rspy = spy(webhdfs.new OffsetUrlOpener((URL) null));
    doReturn(new MockHttpURLConnection(rspy.getURL())).when(rspy)
        .openConnection();
    final OffsetUrlInputStream is = new OffsetUrlInputStream(ospy, rspy);

    assertEquals("getPos wrong", 0, is.getPos());

    is.read();

    assertNull("Initial call made incorrectly (Range Check)", ospy
        .openConnection().getRequestProperty("Range"));

    assertEquals("getPos should be 1 after reading one byte", 1, is.getPos());

    is.read();

    assertEquals("getPos should be 2 after reading two bytes", 2, is.getPos());

    // No additional connections should have been made (no seek)

    rspy.setURL(new URL("http://resolvedurl/"));

    is.seek(100);
    is.read();

    assertEquals("getPos should be 101 after reading one byte", 101,
        is.getPos());

    verify(rspy, times(1)).openConnection();

    is.seek(101);
    is.read();

    verify(rspy, times(1)).openConnection();

    // Seek to 101 should not result in another request"

    is.seek(2500);
    is.read();

    ((MockHttpURLConnection) rspy.openConnection()).setResponseCode(206);
    is.seek(0);

    try {
      is.read();
      fail("Exception should be thrown when 206 response is given "
           + "but 200 is expected");
    } catch (IOException e) {
      WebHdfsFileSystem.LOG.info(e.toString());
    }
  }
}

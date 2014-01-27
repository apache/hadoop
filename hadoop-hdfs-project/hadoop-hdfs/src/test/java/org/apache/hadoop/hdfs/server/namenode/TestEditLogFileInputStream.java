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

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.EnumMap;

import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.util.Holder;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.junit.Test;
import org.mockito.Mockito;

public class TestEditLogFileInputStream {
  private static final byte[] FAKE_LOG_DATA = TestEditLog.HADOOP20_SOME_EDITS;

  @Test
  public void testReadURL() throws Exception {
    HttpURLConnection conn = mock(HttpURLConnection.class);
    doReturn(new ByteArrayInputStream(FAKE_LOG_DATA)).when(conn).getInputStream();
    doReturn(HttpURLConnection.HTTP_OK).when(conn).getResponseCode();
    doReturn(Integer.toString(FAKE_LOG_DATA.length)).when(conn).getHeaderField("Content-Length");

    URLConnectionFactory factory = mock(URLConnectionFactory.class);
    doReturn(conn).when(factory).openConnection(Mockito.<URL> any(),
        anyBoolean());

    URL url = new URL("http://localhost/fakeLog");
    EditLogInputStream elis = EditLogFileInputStream.fromUrl(factory, url,
        HdfsConstants.INVALID_TXID, HdfsConstants.INVALID_TXID, false);
    // Read the edit log and verify that we got all of the data.
    EnumMap<FSEditLogOpCodes, Holder<Integer>> counts = FSImageTestUtil
        .countEditLogOpTypes(elis);
    assertThat(counts.get(FSEditLogOpCodes.OP_ADD).held, is(1));
    assertThat(counts.get(FSEditLogOpCodes.OP_SET_GENSTAMP_V1).held, is(1));
    assertThat(counts.get(FSEditLogOpCodes.OP_CLOSE).held, is(1));

    // Check that length header was picked up.
    assertEquals(FAKE_LOG_DATA.length, elis.length());
    elis.close();
  }
}

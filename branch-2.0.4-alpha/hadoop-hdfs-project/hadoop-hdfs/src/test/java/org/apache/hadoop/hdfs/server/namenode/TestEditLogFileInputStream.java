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

import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.EnumMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.util.Holder;
import org.apache.hadoop.http.HttpServer;
import org.junit.Test;

public class TestEditLogFileInputStream {
  private static final byte[] FAKE_LOG_DATA = TestEditLog.HADOOP20_SOME_EDITS;

  @Test
  public void testReadURL() throws Exception {
    // Start a simple web server which hosts the log data.
    HttpServer server = new HttpServer("test", "0.0.0.0", 0, true);
    server.start();
    try {
      server.addServlet("fakeLog", "/fakeLog", FakeLogServlet.class);
      URL url = new URL("http://localhost:" + server.getPort() + "/fakeLog");
      EditLogInputStream elis = EditLogFileInputStream.fromUrl(
          url, HdfsConstants.INVALID_TXID, HdfsConstants.INVALID_TXID,
          false);
      // Read the edit log and verify that we got all of the data.
      EnumMap<FSEditLogOpCodes, Holder<Integer>> counts =
          FSImageTestUtil.countEditLogOpTypes(elis);
      assertEquals(1L, (long)counts.get(FSEditLogOpCodes.OP_ADD).held);
      assertEquals(1L, (long)counts.get(FSEditLogOpCodes.OP_SET_GENSTAMP).held);
      assertEquals(1L, (long)counts.get(FSEditLogOpCodes.OP_CLOSE).held);
     
      // Check that length header was picked up.
      assertEquals(FAKE_LOG_DATA.length, elis.length());
      elis.close();
    } finally {
      server.stop();
    }
  }

  @SuppressWarnings("serial")
  public static class FakeLogServlet extends HttpServlet {
    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      response.setHeader("Content-Length",
          String.valueOf(FAKE_LOG_DATA.length));
      OutputStream out = response.getOutputStream();
      out.write(FAKE_LOG_DATA);
      out.close();
    }
  }

}

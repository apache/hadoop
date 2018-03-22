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

package org.apache.hadoop.fs.http;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

/**
 * Testing HttpFileSystem.
 */
public class TestHttpFileSystem {
  @Test
  public void testHttpFileSystem() throws IOException, URISyntaxException,
      InterruptedException {
    Configuration conf = new Configuration(false);
    conf.set("fs.http.impl", HttpFileSystem.class.getCanonicalName());
    final String data = "foo";

    try (MockWebServer server = new MockWebServer()) {
      server.enqueue(new MockResponse().setBody(data));
      server.start();
      URI uri = URI.create(String.format("http://%s:%d", server.getHostName(),
          server.getPort()));
      FileSystem fs = FileSystem.get(uri, conf);
      try (InputStream is = fs.open(
          new Path(new URL(uri.toURL(), "/foo").toURI()),
          4096)) {
        byte[] buf = new byte[data.length()];
        IOUtils.readFully(is, buf, 0, buf.length);
        assertEquals(data, new String(buf, StandardCharsets.UTF_8));
      }
      RecordedRequest req = server.takeRequest();
      assertEquals("/foo", req.getPath());
    }
  }
}

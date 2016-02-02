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

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

public final class TestURLConnectionFactory {

  @Test
  public void testConnConfiguratior() throws IOException {
    final URL u = new URL("http://localhost");
    final List<HttpURLConnection> conns = Lists.newArrayList();
    URLConnectionFactory fc = new URLConnectionFactory(new ConnectionConfigurator() {
      @Override
      public HttpURLConnection configure(HttpURLConnection conn)
          throws IOException {
        Assert.assertEquals(u, conn.getURL());
        conns.add(conn);
        return conn;
      }
    });

    fc.openConnection(u);
    Assert.assertEquals(1, conns.size());
  }
}

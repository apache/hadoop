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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Assert;

public class WebHdfsTestUtil {
  public static final Log LOG = LogFactory.getLog(WebHdfsTestUtil.class);

  public static WebHdfsFileSystem getWebHdfsFileSystem(final Configuration conf
      ) throws IOException, URISyntaxException {
    final String uri = WebHdfsFileSystem.SCHEME  + "://"
        + conf.get("dfs.http.address");
    return (WebHdfsFileSystem)FileSystem.get(new URI(uri), conf);
  }

  public static WebHdfsFileSystem getWebHdfsFileSystemAs(
      final UserGroupInformation ugi, final Configuration conf
      ) throws IOException, URISyntaxException, InterruptedException {
    return ugi.doAs(new PrivilegedExceptionAction<WebHdfsFileSystem>() {
      @Override
      public WebHdfsFileSystem run() throws Exception {
        return getWebHdfsFileSystem(conf);
      }
    });
  }

  public static URL toUrl(final WebHdfsFileSystem webhdfs,
      final HttpOpParam.Op op, final Path fspath,
      final Param<?,?>... parameters) throws IOException {
    final URL url = webhdfs.toUrl(op, fspath, parameters);
    WebHdfsTestUtil.LOG.info("url=" + url);
    return url;
  }

  public static Map<?, ?> connectAndGetJson(final HttpURLConnection conn,
      final int expectedResponseCode) throws IOException {
    conn.connect();
    Assert.assertEquals(expectedResponseCode, conn.getResponseCode());
    return WebHdfsFileSystem.jsonParse(conn.getInputStream());
  }
  
  public static HttpURLConnection twoStepWrite(HttpURLConnection conn,
      final HttpOpParam.Op op) throws IOException {
    conn.setRequestMethod(op.getType().toString());
    conn = WebHdfsFileSystem.twoStepWrite(conn, op);
    conn.setDoOutput(true);
    conn.connect();
    return conn;
  }

  public static FSDataOutputStream write(final WebHdfsFileSystem webhdfs,
      final HttpOpParam.Op op, final HttpURLConnection conn,
      final int bufferSize) throws IOException {
    return webhdfs.write(op, conn, bufferSize);
  }
}

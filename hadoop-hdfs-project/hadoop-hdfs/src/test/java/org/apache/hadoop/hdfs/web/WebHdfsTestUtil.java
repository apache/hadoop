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

import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.token.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.security.UserGroupInformation;

public class WebHdfsTestUtil {
  public static final Logger LOG =
      LoggerFactory.getLogger(WebHdfsTestUtil.class);

  public static Configuration createConf() {
    final Configuration conf = new Configuration();
    return conf;
  }

  public static WebHdfsFileSystem getWebHdfsFileSystem(
      final Configuration conf, String scheme) throws IOException,
      URISyntaxException {
    final String uri;

    if (WebHdfsConstants.WEBHDFS_SCHEME.equals(scheme)) {
      uri = WebHdfsConstants.WEBHDFS_SCHEME + "://"
          + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY);
    } else if (WebHdfsConstants.SWEBHDFS_SCHEME.equals(scheme)) {
      uri = WebHdfsConstants.SWEBHDFS_SCHEME + "://"
          + conf.get(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY);
    } else {
      throw new IllegalArgumentException("unknown scheme:" + scheme);
    }
    return (WebHdfsFileSystem)FileSystem.get(new URI(uri), conf);
  }

  public static WebHdfsFileSystem getWebHdfsFileSystemAs(
  final UserGroupInformation ugi, final Configuration conf
  ) throws IOException, InterruptedException {
    return getWebHdfsFileSystemAs(ugi, conf, WebHdfsConstants.WEBHDFS_SCHEME);
  }

  public static WebHdfsFileSystem getWebHdfsFileSystemAs(
      final UserGroupInformation ugi, final Configuration conf, String scheme
      ) throws IOException, InterruptedException {
    return ugi.doAs(new PrivilegedExceptionAction<WebHdfsFileSystem>() {
      @Override
      public WebHdfsFileSystem run() throws Exception {
        return getWebHdfsFileSystem(conf, WebHdfsConstants.WEBHDFS_SCHEME);
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

  public static HttpURLConnection openConnection(URL url, Configuration conf)
      throws IOException {
    URLConnectionFactory connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(60000, 60000, conf);
    return (HttpURLConnection) connectionFactory.openConnection(url);
  }

  public static int sendRequest(final HttpURLConnection conn)
      throws IOException {
    conn.connect();
    return conn.getResponseCode();
  }

  public static Map<?, ?> getAndParseResponse(final HttpURLConnection conn)
      throws IOException {
    return WebHdfsFileSystem.jsonParse(conn, false);
  }

  public static Token<DelegationTokenIdentifier> convertJsonToDelegationToken(
      Map<?, ?> json) throws IOException {
    return JsonUtilClient.toDelegationToken(json);
  }
}

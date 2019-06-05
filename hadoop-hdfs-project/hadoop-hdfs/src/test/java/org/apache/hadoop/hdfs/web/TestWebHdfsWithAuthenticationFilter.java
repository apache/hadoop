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
import java.net.InetSocketAddress;
import java.net.URI;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.http.FilterContainer;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.net.NetUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestWebHdfsWithAuthenticationFilter {
  private static boolean authorized = false;

  public static final class CustomizedFilter implements Filter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
        FilterChain chain) throws IOException, ServletException {
      if (authorized) {
        chain.doFilter(request, response);
      } else {
        ((HttpServletResponse) response)
            .sendError(HttpServletResponse.SC_FORBIDDEN);
      }
    }

    @Override
    public void destroy() {
    }

    /** Initializer for Custom Filter. */
    static public class Initializer extends FilterInitializer {
      public Initializer() {}

      @Override
      public void initFilter(FilterContainer container, Configuration config) {
        container.addFilter("customFilter",
            TestWebHdfsWithAuthenticationFilter.CustomizedFilter.class.
            getName(), null);
      }
    }
  }

  private static Configuration conf;
  private static MiniDFSCluster cluster;
  private static FileSystem fs;

  @BeforeClass
  public static void setUp() throws IOException {
    conf = new Configuration();
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        CustomizedFilter.Initializer.class.getName());
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY, "localhost:0");
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    InetSocketAddress addr = cluster.getNameNode().getHttpAddress();
    fs = FileSystem.get(
        URI.create("webhdfs://" + NetUtils.getHostPortString(addr)), conf);
    cluster.waitActive();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testWebHdfsAuthFilter() throws IOException {
    // getFileStatus() is supposed to pass through with the default filter.
    authorized = false;
    try {
      fs.getFileStatus(new Path("/"));
      Assert.fail("The filter fails to block the request");
    } catch (IOException e) {
    }
    authorized = true;
    fs.getFileStatus(new Path("/"));
  }
}

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
package org.apache.hadoop.http;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Set;
import java.util.TreeSet;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestPathFilter extends HttpServerFunctionalTest {
  static final Logger LOG = LoggerFactory.getLogger(HttpServer2.class);
  static final Set<String> RECORDS = new TreeSet<String>(); 

  /** A very simple filter that records accessed uri's */
  static public class RecordingFilter implements Filter {
    private FilterConfig filterConfig = null;

    @Override
    public void init(FilterConfig filterConfig) {
      this.filterConfig = filterConfig;
    }

    @Override
    public void destroy() {
      this.filterConfig = null;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response,
        FilterChain chain) throws IOException, ServletException {
      if (filterConfig == null)
         return;

      String uri = ((HttpServletRequest)request).getRequestURI();
      LOG.info("filtering " + uri);
      RECORDS.add(uri);
      chain.doFilter(request, response);
    }

    /** Configuration for RecordingFilter */
    static public class Initializer extends FilterInitializer {
      public Initializer() {}

      @Override
      public void initFilter(FilterContainer container, Configuration conf) {
        container.addFilter("recording", RecordingFilter.class.getName(), null);
      }
    }
  }
  
  
  /** access a url, ignoring some IOException such as the page does not exist */
  static void access(String urlstring) throws IOException {
    LOG.warn("access " + urlstring);
    URL url = new URL(urlstring);
    
    URLConnection connection = url.openConnection();
    connection.connect();
    
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(
          connection.getInputStream()));
      try {
        for(; in.readLine() != null; );
      } finally {
        in.close();
      }
    } catch(IOException ioe) {
      LOG.warn("urlstring=" + urlstring, ioe);
    }
  }

  public void testPathSpecFilters() throws Exception {
    Configuration conf = new Configuration();
    
    //start a http server with CountingFilter
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        RecordingFilter.Initializer.class.getName());
    String[] pathSpecs = { "/path", "/path/*" };
    HttpServer2 http = createTestServer(conf, pathSpecs);
    http.start();

    final String baseURL = "/path";
    final String baseSlashURL = "/path/";
    final String addedURL = "/path/nodes";
    final String addedSlashURL = "/path/nodes/";
    final String longURL = "/path/nodes/foo/job";
    final String rootURL = "/";
    final String allURL = "/*";

    final String[] filteredUrls = {baseURL, baseSlashURL, addedURL, 
        addedSlashURL, longURL};
    final String[] notFilteredUrls = {rootURL, allURL};

    // access the urls and verify our paths specs got added to the 
    // filters
    final String prefix = "http://"
        + NetUtils.getHostPortString(http.getConnectorAddress(0));
    try {
      for(int i = 0; i < filteredUrls.length; i++) {
        access(prefix + filteredUrls[i]);
      }
      for(int i = 0; i < notFilteredUrls.length; i++) {
        access(prefix + notFilteredUrls[i]);
      }
    } finally {
      http.stop();
    }

    LOG.info("RECORDS = " + RECORDS);
    
    //verify records
    for(int i = 0; i < filteredUrls.length; i++) {
      assertTrue(RECORDS.remove(filteredUrls[i]));
    }
    assertTrue(RECORDS.isEmpty());
  }
}

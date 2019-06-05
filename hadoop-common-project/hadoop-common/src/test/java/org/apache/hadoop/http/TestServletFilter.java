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
import java.util.Random;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestServletFilter extends HttpServerFunctionalTest {
  static final Logger LOG = LoggerFactory.getLogger(HttpServer2.class);
  static volatile String uri = null; 

  /** A very simple filter which record the uri filtered. */
  static public class SimpleFilter implements Filter {
    private FilterConfig filterConfig = null;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
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

      uri = ((HttpServletRequest)request).getRequestURI();
      LOG.info("filtering " + uri);
      chain.doFilter(request, response);
    }

    /** Configuration for the filter */
    static public class Initializer extends FilterInitializer {
      public Initializer() {}

      @Override
      public void initFilter(FilterContainer container, Configuration conf) {
        container.addFilter("simple", SimpleFilter.class.getName(), null);
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

  public void testServletFilter() throws Exception {
    Configuration conf = new Configuration();
    
    //start a http server with CountingFilter
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        SimpleFilter.Initializer.class.getName());
    HttpServer2 http = createTestServer(conf);
    http.start();

    final String fsckURL = "/fsck";
    final String stacksURL = "/stacks";
    final String ajspURL = "/a.jsp";
    final String logURL = "/logs/a.log";
    final String hadooplogoURL = "/static/hadoop-logo.jpg";
    
    final String[] urls = {fsckURL, stacksURL, ajspURL, logURL, hadooplogoURL};
    final Random ran = new Random();
    final int[] sequence = new int[50];

    //generate a random sequence and update counts 
    for(int i = 0; i < sequence.length; i++) {
      sequence[i] = ran.nextInt(urls.length);
    }

    //access the urls as the sequence
    final String prefix = "http://"
        + NetUtils.getHostPortString(http.getConnectorAddress(0));
    try {
      for(int i = 0; i < sequence.length; i++) {
        access(prefix + urls[sequence[i]]);

        //make sure everything except fsck get filtered
        if (sequence[i] == 0) {
          assertEquals(null, uri);
        } else {
          assertEquals(urls[sequence[i]], uri);
          uri = null;
        }
      }
    } finally {
      http.stop();
    }
  }
  
  static public class ErrorFilter extends SimpleFilter {
    static final String EXCEPTION_MESSAGE =
        "Throwing the exception from Filter init";
    @Override
    public void init(FilterConfig arg0) throws ServletException {
      throw new ServletException(EXCEPTION_MESSAGE);
    }

    /** Configuration for the filter */
    static public class Initializer extends FilterInitializer {
      public Initializer() {
      }

      @Override
      public void initFilter(FilterContainer container, Configuration conf) {
        container.addFilter("simple", ErrorFilter.class.getName(), null);
      }
    }
  }

  @Test
  public void testServletFilterWhenInitThrowsException() throws Exception {
    Configuration conf = new Configuration();
    // start a http server with ErrorFilter
    conf.set(HttpServer2.FILTER_INITIALIZER_PROPERTY,
        ErrorFilter.Initializer.class.getName());
    HttpServer2 http = createTestServer(conf);
    try {
      http.start();
      fail("expecting exception");
    } catch (IOException e) {
      assertEquals("Problem starting http server", e.getMessage());
      assertEquals(ErrorFilter.EXCEPTION_MESSAGE, e.getCause().getMessage());
    }
  }
  
  /**
   * Similar to the above test case, except that it uses a different API to add the
   * filter. Regression test for HADOOP-8786.
   */
  @Test
  public void testContextSpecificServletFilterWhenInitThrowsException()
      throws Exception {
    Configuration conf = new Configuration();
    HttpServer2 http = createTestServer(conf);
    HttpServer2.defineFilter(http.webAppContext,
        "ErrorFilter", ErrorFilter.class.getName(),
        null, null);
    try {
      http.start();
      fail("expecting exception");
    } catch (IOException e) {
      GenericTestUtils.assertExceptionContains("Unable to initialize WebAppContext", e);
    }
  }

}

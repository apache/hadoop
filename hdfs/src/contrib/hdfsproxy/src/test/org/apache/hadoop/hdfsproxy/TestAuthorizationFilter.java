/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdfsproxy;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.ArrayList;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.cactus.FilterTestCase;
import org.apache.cactus.WebRequest;
import org.apache.cactus.WebResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

public class TestAuthorizationFilter extends FilterTestCase {

  public static final Log LOG = LogFactory.getLog(TestAuthorizationFilter.class);

  private class DummyFilterChain implements FilterChain {
    public void doFilter(ServletRequest theRequest, ServletResponse theResponse)
        throws IOException, ServletException {
      PrintWriter writer = theResponse.getWriter();

      writer.print("<p>some content</p>");
      writer.close();
    }

    public void init(FilterConfig theConfig) {
    }

    public void destroy() {
    }
  }

  private class ConfiguredAuthorizationFilter extends AuthorizationFilter {
    
    private ConfiguredAuthorizationFilter(String nameNode) {
      this.namenode = nameNode;
    }  
  }

  public void beginPathRestriction(WebRequest theRequest) {
    theRequest.setURL("proxy-test:0", null, "/streamFile", null,
        "filename=/nontestdir");
  }

  public void testPathRestriction() throws ServletException, IOException {
    AuthorizationFilter filter = new 
        ConfiguredAuthorizationFilter("hdfs://apache.org");
    request.setRemoteIPAddress("127.0.0.1");
    request.setAttribute("org.apache.hadoop.hdfsproxy.authorized.userID",
        System.getProperty("user.name"));
    List<Path> paths = new ArrayList<Path>();
    paths.add(new Path("/deny"));
    paths.add(new Path("hdfs://test:100/deny"));
    paths.add(new Path("hdfs://test/deny"));
    request.setAttribute("org.apache.hadoop.hdfsproxy.authorized.paths",
        paths);
    FilterChain mockFilterChain = new DummyFilterChain();
    filter.doFilter(request, response, mockFilterChain);
  }

  public void endPathRestriction(WebResponse theResponse) {
    assertEquals(theResponse.getStatusCode(), 403);
    assertTrue("Text missing 'User not authorized to access path' : : ["
        + theResponse.getText() + "]", theResponse.getText().indexOf(
        "is not authorized to access path") > 0);
  }

  public void beginPathPermit(WebRequest theRequest) {
    theRequest.setURL("proxy-test:0", null, "/streamFile", null,
        "filename=/data/file");
  }

  public void testPathPermit() throws ServletException, IOException {
    AuthorizationFilter filter = new 
        ConfiguredAuthorizationFilter("hdfs://apache.org");
    request.setRemoteIPAddress("127.0.0.1");
    request.setAttribute("org.apache.hadoop.hdfsproxy.authorized.userID",
        System.getProperty("user.name"));
    List<Path> paths = new ArrayList<Path>();
    paths.add(new Path("/data"));
    request.setAttribute("org.apache.hadoop.hdfsproxy.authorized.paths",
        paths);
    FilterChain mockFilterChain = new DummyFilterChain();
    filter.doFilter(request, response, mockFilterChain);
  }

  public void endPathPermit(WebResponse theResponse) {
    assertEquals(theResponse.getStatusCode(), 200);
  }

  public void beginPathPermitQualified(WebRequest theRequest) {
    theRequest.setURL("proxy-test:0", null, "/streamFile", null,
        "filename=/data/file");
  }

  public void testPathPermitQualified() throws ServletException, IOException {
    AuthorizationFilter filter = new 
        ConfiguredAuthorizationFilter("hdfs://apache.org");
    request.setRemoteIPAddress("127.0.0.1");
    request.setAttribute("org.apache.hadoop.hdfsproxy.authorized.userID",
        System.getProperty("user.name"));
    List<Path> paths = new ArrayList<Path>();
    paths.add(new Path("hdfs://apache.org/data"));
    request.setAttribute("org.apache.hadoop.hdfsproxy.authorized.paths",
        paths);
    FilterChain mockFilterChain = new DummyFilterChain();
    filter.doFilter(request, response, mockFilterChain);
  }

  public void endPathPermitQualified(WebResponse theResponse) {
    assertEquals(theResponse.getStatusCode(), 200);
  }
  
  public void beginPathQualifiediReject(WebRequest theRequest) {
    theRequest.setURL("proxy-test:0", null, "/streamFile", null,
        "filename=/data/file");
  }

  public void testPathQualifiedReject() throws ServletException, IOException {
    AuthorizationFilter filter = new 
        ConfiguredAuthorizationFilter("hdfs://apache.org:1111");
    request.setRemoteIPAddress("127.0.0.1");
    request.setAttribute("org.apache.hadoop.hdfsproxy.authorized.userID",
        System.getProperty("user.name"));
    List<Path> paths = new ArrayList<Path>();
    paths.add(new Path("hdfs://apache.org:2222/data"));
    request.setAttribute("org.apache.hadoop.hdfsproxy.authorized.paths",
        paths);
    FilterChain mockFilterChain = new DummyFilterChain();
    filter.doFilter(request, response, mockFilterChain);
  }

  public void endPathQualifiedReject(WebResponse theResponse) {
    assertEquals(theResponse.getStatusCode(), 403);
  }
}

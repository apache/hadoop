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

import javax.naming.NamingException;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;

import org.apache.cactus.FilterTestCase;
import org.apache.cactus.WebRequest;
import org.apache.cactus.WebResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestLdapIpDirFilter extends FilterTestCase {

  public static final Log LOG = LogFactory.getLog(TestLdapIpDirFilter.class);

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

  public void testIpRestriction() throws ServletException, IOException,
      NamingException {
    LdapIpDirFilter filter = new LdapIpDirFilter();
    String baseName = "ou=proxyroles,dc=mycompany,dc=com";
    DummyLdapContext dlc = new DummyLdapContext();
    filter.initialize(baseName, dlc);
    request.setRemoteIPAddress("127.0.0.2");
    request.removeAttribute("org.apache.hadoop.hdfsproxy.authorized.userID");
    FilterChain mockFilterChain = new DummyFilterChain();
    filter.doFilter(request, response, mockFilterChain);
    assertNull(request
        .getAttribute("org.apache.hadoop.hdfsproxy.authorized.userID"));
  }

  public void endIpRestriction(WebResponse theResponse) {
    assertEquals(theResponse.getStatusCode(), 403);
    assertTrue("Text missing 'IP not authorized to access' : : ["
        + theResponse.getText() + "]", theResponse.getText().indexOf(
        "not authorized to access") > 0);
  }

  public void beginDoFilter(WebRequest theRequest) {
    theRequest.setURL("proxy-test:0", null, "/streamFile", null,
        "filename=/testdir");
  }

  public void testDoFilter() throws ServletException, IOException,
      NamingException {
    LdapIpDirFilter filter = new LdapIpDirFilter();
    String baseName = "ou=proxyroles,dc=mycompany,dc=com";
    DummyLdapContext dlc = new DummyLdapContext();
    filter.initialize(baseName, dlc);
    request.setRemoteIPAddress("127.0.0.1");

    ServletContext context = config.getServletContext();
    context.removeAttribute("name.node.address");
    context.removeAttribute("name.conf");
    assertNull(context.getAttribute("name.node.address"));
    assertNull(context.getAttribute("name.conf"));
    filter.init(config);
    assertNotNull(context.getAttribute("name.node.address"));
    assertNotNull(context.getAttribute("name.conf"));

    request.removeAttribute("org.apache.hadoop.hdfsproxy.authorized.userID");
    FilterChain mockFilterChain = new DummyFilterChain();
    filter.doFilter(request, response, mockFilterChain);
    assertEquals(request
        .getAttribute("org.apache.hadoop.hdfsproxy.authorized.userID"),
        "testuser");

  }

  public void endDoFilter(WebResponse theResponse) {
    assertEquals("<p>some content</p>", theResponse.getText());
  }

}

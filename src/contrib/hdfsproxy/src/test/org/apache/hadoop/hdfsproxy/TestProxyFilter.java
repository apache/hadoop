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

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.ServletContext;

import org.apache.cactus.FilterTestCase;
import org.apache.cactus.WebRequest;
import org.apache.cactus.WebResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class TestProxyFilter extends FilterTestCase {
  
  public static final Log LOG = LogFactory.getLog(TestProxyFilter.class);
  
  private static String TEST_CLIENT_SSL_CERT = System.getProperty("javax.net.ssl.clientCert", 
  "./src/test/resources/ssl-keys/test.crt");
  
  private class DummyFilterChain implements FilterChain {
    public void doFilter(ServletRequest theRequest, ServletResponse theResponse) 
      throws IOException, ServletException  {
      PrintWriter writer = theResponse.getWriter();
  
      writer.print("<p>some content</p>");
      writer.close();
    }
  
    public void init(FilterConfig theConfig) {
    }
  
    public void destroy() {
    }
  }
   
  public void beginDoFilterHttp(WebRequest theRequest) {
    theRequest.addParameter("ugi", "nobody,test");
  }  
  
  public void testDoFilterHttp() throws ServletException, IOException  {    
    ProxyFilter filter = new ProxyFilter();
    
    ServletContext context = config.getServletContext();
    context.removeAttribute("name.node.address");
    context.removeAttribute("name.conf");
    assertNull(context.getAttribute("name.node.address"));
    assertNull(context.getAttribute("name.conf"));
    
    filter.init(config);
    
    assertNotNull(context.getAttribute("name.node.address"));
    assertNotNull(context.getAttribute("name.conf"));
    
    request.removeAttribute("authorized.ugi");
    assertNull(request.getAttribute("authorized.ugi"));
        
    FilterChain mockFilterChain = new DummyFilterChain();
    filter.doFilter(request, response, mockFilterChain);    
    assertEquals(request.getAttribute("authorized.ugi").toString(), "nobody,test");
    
  }

  public void endDoFilterHttp(WebResponse theResponse)  {
    assertEquals("<p>some content</p>", theResponse.getText());    
  }
  
  public void beginDoFilterHttps(WebRequest theRequest) throws Exception{
    theRequest.addParameter("UnitTest", "true");
    theRequest.addParameter("SslPath", TEST_CLIENT_SSL_CERT);
    theRequest.addParameter("ugi", "nobody,test");    
    theRequest.addParameter("TestSevletPathInfo", "/streamFile");
    theRequest.addParameter("filename", "/user");
  }  
  
  public void testDoFilterHttps() throws Exception  {    
    ProxyFilter filter = new ProxyFilter();
    
    request.removeAttribute("authorized.ugi");
    assertNull(request.getAttribute("authorized.ugi"));        
    
    FilterChain mockFilterChain = new DummyFilterChain();
    filter.init(config);
    filter.doFilter(request, response, mockFilterChain);
    
    LOG.info("Finish setting up X509Certificate");  
    assertEquals(request.getAttribute("authorized.ugi").toString().substring(0, 6), "nobody");
    
  }

  public void endDoFilterHttps(WebResponse theResponse)  {
    assertEquals("<p>some content</p>", theResponse.getText());    
  }
  
    
}


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

package org.apache.hadoop.hdfsproxy;

import org.apache.cactus.ServletTestCase;
import org.apache.cactus.WebRequest;
import org.apache.cactus.WebResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import javax.servlet.ServletException;

/** Unit tests for ProxyUtil */
public class TestProxyForwardServlet extends ServletTestCase {
  public static final Log LOG = LogFactory.getLog(TestProxyForwardServlet.class);
  
 
  public void beginDoGet(WebRequest theRequest) {
    theRequest.setURL("proxy-test:0", null, "/simple", null, null);
  }
  
  public void testDoGet() throws IOException, ServletException {
    ProxyForwardServlet servlet = new ProxyForwardServlet();
    
    servlet.init(config);
    servlet.doGet(request, response);
  }
  
  public void endDoGet(WebResponse theResponse)
    throws IOException {
    String expected = "<html><head/><body>A GET request</body></html>";
    String result = theResponse.getText();

    assertEquals(expected, result);
  }
  
  
  public void testForwardRequest() throws Exception  {
    ProxyForwardServlet servlet = new ProxyForwardServlet();

    servlet.forwardRequest(request, response, config.getServletContext(), "/simple");
  }
  
  public void endForwardRequest(WebResponse theResponse) throws IOException  {
    String expected = "<html><head/><body>A GET request</body></html>";
    String result = theResponse.getText();
    
    assertEquals(expected, result);
    
  } 
 
}

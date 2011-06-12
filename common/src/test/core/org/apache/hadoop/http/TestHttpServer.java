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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestHttpServer {
  private static HttpServer server;
  private static URL baseUrl;
  
  @SuppressWarnings("serial")
  public static class EchoMapServlet extends HttpServlet {
    @SuppressWarnings("unchecked")
    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      PrintStream out = new PrintStream(response.getOutputStream());
      Map<String, String[]> params = request.getParameterMap();
      SortedSet<String> keys = new TreeSet(params.keySet());
      for(String key: keys) {
        out.print(key);
        out.print(':');
        String[] values = params.get(key);
        if (values.length > 0) {
          out.print(values[0]);
          for(int i=1; i < values.length; ++i) {
            out.print(',');
            out.print(values[i]);
          }
        }
        out.print('\n');
      }
      out.close();
    }    
  }

  @SuppressWarnings("serial")
  public static class EchoServlet extends HttpServlet {
    @SuppressWarnings("unchecked")
    @Override
    public void doGet(HttpServletRequest request, 
                      HttpServletResponse response
                      ) throws ServletException, IOException {
      PrintStream out = new PrintStream(response.getOutputStream());
      SortedSet<String> sortedKeys = new TreeSet();
      Enumeration<String> keys = request.getParameterNames();
      while(keys.hasMoreElements()) {
        sortedKeys.add(keys.nextElement());
      }
      for(String key: sortedKeys) {
        out.print(key);
        out.print(':');
        out.print(request.getParameter(key));
        out.print('\n');
      }
      out.close();
    }    
  }

  private String readOutput(URL url) throws IOException {
    StringBuilder out = new StringBuilder();
    InputStream in = url.openConnection().getInputStream();
    byte[] buffer = new byte[64 * 1024];
    int len = in.read(buffer);
    while (len > 0) {
      out.append(new String(buffer, 0, len));
      len = in.read(buffer);
    }
    return out.toString();
  }
  
  @BeforeClass public static void setup() throws Exception {
    new File(System.getProperty("build.webapps", "build/webapps") + "/test"
             ).mkdirs();
    server = new HttpServer("test", "0.0.0.0", 0, true);
    server.addServlet("echo", "/echo", EchoServlet.class);
    server.addServlet("echomap", "/echomap", EchoMapServlet.class);
    server.start();
    int port = server.getPort();
    baseUrl = new URL("http://localhost:" + port + "/");
  }
  
  @AfterClass public static void cleanup() throws Exception {
    server.stop();
  }

  @Test public void testEcho() throws Exception {
    assertEquals("a:b\nc:d\n", 
                 readOutput(new URL(baseUrl, "/echo?a=b&c=d")));
    assertEquals("a:b\nc&lt;:d\ne:&gt;\n", 
                 readOutput(new URL(baseUrl, "/echo?a=b&c<=d&e=>")));    
  }
  
  /** Test the echo map servlet that uses getParameterMap. */
  @Test public void testEchoMap() throws Exception {
    assertEquals("a:b\nc:d\n", 
                 readOutput(new URL(baseUrl, "/echomap?a=b&c=d")));
    assertEquals("a:b,&gt;\nc&lt;:d\n", 
                 readOutput(new URL(baseUrl, "/echomap?a=b&c<=d&a=>")));
  }

}

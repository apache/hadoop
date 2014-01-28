/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.jmx;


import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.http.HttpServerFunctionalTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJMXJsonServlet extends HttpServerFunctionalTest {
  private   static final Log LOG = LogFactory.getLog(TestJMXJsonServlet.class);
  private static HttpServer2 server;
  private static URL baseUrl;

  @BeforeClass public static void setup() throws Exception {
    server = createTestServer();
    server.start();
    baseUrl = getServerURL(server);
  }
  
  @AfterClass public static void cleanup() throws Exception {
    server.stop();
  }
  
  public static void assertReFind(String re, String value) {
    Pattern p = Pattern.compile(re);
    Matcher m = p.matcher(value);
    assertTrue("'"+p+"' does not match "+value, m.find());
  }
  
  @Test public void testQury() throws Exception {
    String result = readOutput(new URL(baseUrl, "/jmx?qry=java.lang:type=Runtime"));
    LOG.info("/jmx?qry=java.lang:type=Runtime RESULT: "+result);
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Runtime\"", result);
    assertReFind("\"modelerType\"", result);
    
    result = readOutput(new URL(baseUrl, "/jmx?qry=java.lang:type=Memory"));
    LOG.info("/jmx?qry=java.lang:type=Memory RESULT: "+result);
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"", result);
    assertReFind("\"modelerType\"", result);
    
    result = readOutput(new URL(baseUrl, "/jmx"));
    LOG.info("/jmx RESULT: "+result);
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"", result);
    
    // test to get an attribute of a mbean
    result = readOutput(new URL(baseUrl, 
        "/jmx?get=java.lang:type=Memory::HeapMemoryUsage"));
    LOG.info("/jmx RESULT: "+result);
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"", result);
    assertReFind("\"committed\"\\s*:", result);
    
    // negative test to get an attribute of a mbean
    result = readOutput(new URL(baseUrl, 
        "/jmx?get=java.lang:type=Memory::"));
    LOG.info("/jmx RESULT: "+result);
    assertReFind("\"ERROR\"", result);

    // test to get JSONP result
    result = readOutput(new URL(baseUrl, "/jmx?qry=java.lang:type=Memory&callback=mycallback1"));
    LOG.info("/jmx?qry=java.lang:type=Memory&callback=mycallback RESULT: "+result);
    assertReFind("^mycallback1\\(\\{", result);
    assertReFind("\\}\\);$", result);

    // negative test to get an attribute of a mbean as JSONP
    result = readOutput(new URL(baseUrl,
        "/jmx?get=java.lang:type=Memory::&callback=mycallback2"));
    LOG.info("/jmx RESULT: "+result);
    assertReFind("^mycallback2\\(\\{", result);
    assertReFind("\"ERROR\"", result);
    assertReFind("\\}\\);$", result);

    // test to get an attribute of a mbean as JSONP
    result = readOutput(new URL(baseUrl,
        "/jmx?get=java.lang:type=Memory::HeapMemoryUsage&callback=mycallback3"));
    LOG.info("/jmx RESULT: "+result);
    assertReFind("^mycallback3\\(\\{", result);
    assertReFind("\"name\"\\s*:\\s*\"java.lang:type=Memory\"", result);
    assertReFind("\"committed\"\\s*:", result);
    assertReFind("\\}\\);$", result);

  }
}

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

package org.apache.hadoop.lib.servlet;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.lib.server.Server;
import org.apache.hadoop.test.HTestCase;
import org.apache.hadoop.test.TestDir;
import org.apache.hadoop.test.TestDirHelper;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;

public class TestServerWebApp extends HTestCase {

  @Test(expected = IllegalArgumentException.class)
  public void getHomeDirNotDef() {
    ServerWebApp.getHomeDir("TestServerWebApp00");
  }

  @Test
  public void getHomeDir() {
    System.setProperty("TestServerWebApp0.home.dir", "/tmp");
    assertEquals(ServerWebApp.getHomeDir("TestServerWebApp0"), "/tmp");
    assertEquals(ServerWebApp.getDir("TestServerWebApp0", ".log.dir", "/tmp/log"), "/tmp/log");
    System.setProperty("TestServerWebApp0.log.dir", "/tmplog");
    assertEquals(ServerWebApp.getDir("TestServerWebApp0", ".log.dir", "/tmp/log"), "/tmplog");
  }

  @Test
  @TestDir
  public void lifecycle() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    System.setProperty("TestServerWebApp1.home.dir", dir);
    System.setProperty("TestServerWebApp1.config.dir", dir);
    System.setProperty("TestServerWebApp1.log.dir", dir);
    System.setProperty("TestServerWebApp1.temp.dir", dir);
    ServerWebApp server = new ServerWebApp("TestServerWebApp1") {
    };

    assertEquals(server.getStatus(), Server.Status.UNDEF);
    server.contextInitialized(null);
    assertEquals(server.getStatus(), Server.Status.NORMAL);
    server.contextDestroyed(null);
    assertEquals(server.getStatus(), Server.Status.SHUTDOWN);
  }

  @Test(expected = RuntimeException.class)
  @TestDir
  public void failedInit() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    System.setProperty("TestServerWebApp2.home.dir", dir);
    System.setProperty("TestServerWebApp2.config.dir", dir);
    System.setProperty("TestServerWebApp2.log.dir", dir);
    System.setProperty("TestServerWebApp2.temp.dir", dir);
    System.setProperty("testserverwebapp2.services", "FOO");
    ServerWebApp server = new ServerWebApp("TestServerWebApp2") {
    };

    server.contextInitialized(null);
  }

  @Test
  @TestDir
  public void testResolveAuthority() throws Exception {
    String dir = TestDirHelper.getTestDir().getAbsolutePath();
    System.setProperty("TestServerWebApp3.home.dir", dir);
    System.setProperty("TestServerWebApp3.config.dir", dir);
    System.setProperty("TestServerWebApp3.log.dir", dir);
    System.setProperty("TestServerWebApp3.temp.dir", dir);
    System.setProperty("testserverwebapp3.http.hostname", "localhost");
    System.setProperty("testserverwebapp3.http.port", "14000");
    ServerWebApp server = new ServerWebApp("TestServerWebApp3") {
    };

    InetSocketAddress address = server.resolveAuthority();
    Assert.assertEquals("localhost", address.getHostName());
    Assert.assertEquals(14000, address.getPort());
  }

}

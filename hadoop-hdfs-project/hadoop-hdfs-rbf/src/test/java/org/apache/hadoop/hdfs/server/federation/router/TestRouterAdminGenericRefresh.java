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

package org.apache.hadoop.hdfs.server.federation.router;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.federation.RouterConfigBuilder;
import org.apache.hadoop.hdfs.tools.federation.RouterAdmin;
import org.apache.hadoop.ipc.RefreshHandler;
import org.apache.hadoop.ipc.RefreshRegistry;
import org.apache.hadoop.ipc.RefreshResponse;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Before all tests, a router is spun up.
 * Before each test, mock refresh handlers are created and registered.
 * After each test, the mock handlers are unregistered.
 * After all tests, the router is spun down.
 */
public class TestRouterAdminGenericRefresh {
  private static Router router;
  private static RouterAdmin admin;

  private static RefreshHandler firstHandler;
  private static RefreshHandler secondHandler;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    // Build and start a router with admin + RPC
    router = new Router();
    Configuration config = new RouterConfigBuilder()
        .admin()
        .rpc()
        .build();
    router.init(config);
    router.start();
    admin = new RouterAdmin(config);
  }

  @AfterClass
  public static void tearDownBeforeClass() throws IOException {
    if (router != null) {
      router.stop();
      router.close();
    }
  }

  @Before
  public void setUp() throws Exception {
    // Register Handlers, first one just sends an ok response
    firstHandler = Mockito.mock(RefreshHandler.class);
    Mockito.when(firstHandler.handleRefresh(Mockito.anyString(),
        Mockito.any(String[].class))).thenReturn(
            RefreshResponse.successResponse());
    RefreshRegistry.defaultRegistry().register("firstHandler", firstHandler);

    // Second handler has conditional response for testing args
    secondHandler = Mockito.mock(RefreshHandler.class);
    Mockito.when(secondHandler.handleRefresh(
        "secondHandler", new String[]{"one", "two"})).thenReturn(
            new RefreshResponse(3, "three"));
    Mockito.when(secondHandler.handleRefresh(
        "secondHandler", new String[]{"one"})).thenReturn(
            new RefreshResponse(2, "two"));
    RefreshRegistry.defaultRegistry().register("secondHandler", secondHandler);
  }

  @After
  public void tearDown() throws Exception {
    RefreshRegistry.defaultRegistry().unregisterAll("firstHandler");
    RefreshRegistry.defaultRegistry().unregisterAll("secondHandler");
  }

  @Test
  public void testInvalidCommand() throws Exception {
    String[] args = new String[]{"-refreshRouterArgs", "nn"};
    int exitCode = admin.run(args);
    assertEquals("RouterAdmin should fail due to bad args", -1, exitCode);
  }

  @Test
  public void testInvalidIdentifier() throws Exception {
    String[] argv = new String[]{"-refreshRouterArgs", "localhost:" +
        router.getAdminServerAddress().getPort(), "unregisteredIdentity"};
    int exitCode = admin.run(argv);
    assertEquals("RouterAdmin should fail due to no handler registered",
        -1, exitCode);
  }

  @Test
  public void testValidIdentifier() throws Exception {
    String[] args = new String[]{"-refreshRouterArgs", "localhost:" +
        router.getAdminServerAddress().getPort(), "firstHandler"};
    int exitCode = admin.run(args);
    assertEquals("RouterAdmin should succeed", 0, exitCode);

    Mockito.verify(firstHandler).handleRefresh("firstHandler", new String[]{});
    // Second handler was never called
    Mockito.verify(secondHandler, Mockito.never())
      .handleRefresh(Mockito.anyString(), Mockito.any(String[].class));
  }

  @Test
  public void testVariableArgs() throws Exception {
    String[] args = new String[]{"-refreshRouterArgs", "localhost:" +
        router.getAdminServerAddress().getPort(), "secondHandler", "one"};
    int exitCode = admin.run(args);
    assertEquals("RouterAdmin should return 2", 2, exitCode);

    exitCode = admin.run(new String[]{"-refreshRouterArgs", "localhost:" +
        router.getAdminServerAddress().getPort(),
        "secondHandler", "one", "two"});
    assertEquals("RouterAdmin should now return 3", 3, exitCode);

    Mockito.verify(secondHandler).handleRefresh(
        "secondHandler", new String[]{"one"});
    Mockito.verify(secondHandler).handleRefresh(
        "secondHandler", new String[]{"one", "two"});
  }

  @Test
  public void testUnregistration() throws Exception {
    RefreshRegistry.defaultRegistry().unregisterAll("firstHandler");

    // And now this should fail
    String[] args = new String[]{"-refreshRouterArgs", "localhost:" +
        router.getAdminServerAddress().getPort(), "firstHandler"};
    int exitCode = admin.run(args);
    assertEquals("RouterAdmin should return -1", -1, exitCode);
  }

  @Test
  public void testUnregistrationReturnValue() {
    RefreshHandler mockHandler = Mockito.mock(RefreshHandler.class);
    RefreshRegistry.defaultRegistry().register("test", mockHandler);
    boolean ret = RefreshRegistry.defaultRegistry().
        unregister("test", mockHandler);
    assertTrue(ret);
  }

  @Test
  public void testMultipleRegistration() throws Exception {
    RefreshRegistry.defaultRegistry().register("sharedId", firstHandler);
    RefreshRegistry.defaultRegistry().register("sharedId", secondHandler);

    // this should trigger both
    String[] args = new String[]{"-refreshRouterArgs", "localhost:" +
        router.getAdminServerAddress().getPort(), "sharedId", "one"};
    int exitCode = admin.run(args);

    // -1 because one of the responses is unregistered
    assertEquals(-1, exitCode);

    // verify we called both
    Mockito.verify(firstHandler).handleRefresh(
        "sharedId", new String[]{"one"});
    Mockito.verify(secondHandler).handleRefresh(
        "sharedId", new String[]{"one"});

    RefreshRegistry.defaultRegistry().unregisterAll("sharedId");
  }

  @Test
  public void testMultipleReturnCodeMerging() throws Exception {
    // Two handlers which return two non-zero values
    RefreshHandler handlerOne = Mockito.mock(RefreshHandler.class);
    Mockito.when(handlerOne.handleRefresh(Mockito.anyString(),
        Mockito.any(String[].class))).thenReturn(
            new RefreshResponse(23, "Twenty Three"));

    RefreshHandler handlerTwo = Mockito.mock(RefreshHandler.class);
    Mockito.when(handlerTwo.handleRefresh(Mockito.anyString(),
        Mockito.any(String[].class))).thenReturn(
            new RefreshResponse(10, "Ten"));

    // Then registered to the same ID
    RefreshRegistry.defaultRegistry().register("shared", handlerOne);
    RefreshRegistry.defaultRegistry().register("shared", handlerTwo);

    // We refresh both
    String[] args = new String[]{"-refreshRouterArgs", "localhost:" +
        router.getAdminServerAddress().getPort(), "shared"};
    int exitCode = admin.run(args);

    // We get -1 because of our logic for melding non-zero return codes
    assertEquals(-1, exitCode);

    // Verify we called both
    Mockito.verify(handlerOne).handleRefresh("shared", new String[]{});
    Mockito.verify(handlerTwo).handleRefresh("shared", new String[]{});

    RefreshRegistry.defaultRegistry().unregisterAll("shared");
  }

  @Test
  public void testExceptionResultsInNormalError() throws Exception {
    // In this test, we ensure that all handlers are called
    // even if we throw an exception in one
    RefreshHandler exceptionalHandler = Mockito.mock(RefreshHandler.class);
    Mockito.when(exceptionalHandler.handleRefresh(Mockito.anyString(),
        Mockito.any(String[].class))).thenThrow(
            new RuntimeException("Exceptional Handler Throws Exception"));

    RefreshHandler otherExceptionalHandler = Mockito.mock(RefreshHandler.class);
    Mockito.when(otherExceptionalHandler.handleRefresh(Mockito.anyString(),
        Mockito.any(String[].class))).thenThrow(
            new RuntimeException("More Exceptions"));

    RefreshRegistry.defaultRegistry().register("exceptional",
        exceptionalHandler);
    RefreshRegistry.defaultRegistry().register("exceptional",
        otherExceptionalHandler);

    String[] args = new String[]{"-refreshRouterArgs", "localhost:" +
        router.getAdminServerAddress().getPort(), "exceptional"};
    int exitCode = admin.run(args);
    assertEquals(-1, exitCode); // Exceptions result in a -1

    Mockito.verify(exceptionalHandler).handleRefresh(
        "exceptional", new String[]{});
    Mockito.verify(otherExceptionalHandler).handleRefresh(
        "exceptional", new String[]{});

    RefreshRegistry.defaultRegistry().unregisterAll("exceptional");
  }
}

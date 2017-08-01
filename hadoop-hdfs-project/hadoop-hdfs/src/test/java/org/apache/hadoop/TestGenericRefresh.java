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

package org.apache.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.tools.DFSAdmin;
import org.apache.hadoop.ipc.RefreshHandler;

import org.apache.hadoop.ipc.RefreshRegistry;
import org.apache.hadoop.ipc.RefreshResponse;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.mockito.Mockito;

/**
 * Before all tests, a MiniDFSCluster is spun up.
 * Before each test, mock refresh handlers are created and registered.
 * After each test, the mock handlers are unregistered.
 * After all tests, the cluster is spun down.
 */
public class TestGenericRefresh {
  private static MiniDFSCluster cluster;
  private static Configuration config;

  private static RefreshHandler firstHandler;
  private static RefreshHandler secondHandler;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    config = new Configuration();
    config.set("hadoop.security.authorization", "true");

    FileSystem.setDefaultUri(config, "hdfs://localhost:0");
    cluster = new MiniDFSCluster.Builder(config).build();
    cluster.waitActive();
  }

  @AfterClass
  public static void tearDownBeforeClass() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setUp() throws Exception {
    // Register Handlers, first one just sends an ok response
    firstHandler = Mockito.mock(RefreshHandler.class);
    Mockito.when(firstHandler.handleRefresh(Mockito.anyString(), Mockito.any(String[].class)))
      .thenReturn(RefreshResponse.successResponse());
    RefreshRegistry.defaultRegistry().register("firstHandler", firstHandler);

    // Second handler has conditional response for testing args
    secondHandler = Mockito.mock(RefreshHandler.class);
    Mockito.when(secondHandler.handleRefresh("secondHandler", new String[]{"one", "two"}))
      .thenReturn(new RefreshResponse(3, "three"));
    Mockito.when(secondHandler.handleRefresh("secondHandler", new String[]{"one"}))
      .thenReturn(new RefreshResponse(2, "two"));
    RefreshRegistry.defaultRegistry().register("secondHandler", secondHandler);
  }

  @After
  public void tearDown() throws Exception {
    RefreshRegistry.defaultRegistry().unregisterAll("firstHandler");
    RefreshRegistry.defaultRegistry().unregisterAll("secondHandler");
  }

  @Test
  public void testInvalidCommand() throws Exception {
    DFSAdmin admin = new DFSAdmin(config);
    String [] args = new String[]{"-refresh", "nn"};
    int exitCode = admin.run(args);
    assertEquals("DFSAdmin should fail due to bad args", -1, exitCode);
  }

  @Test
  public void testInvalidIdentifier() throws Exception {
    DFSAdmin admin = new DFSAdmin(config);
    String [] args = new String[]{"-refresh", "localhost:" + 
        cluster.getNameNodePort(), "unregisteredIdentity"};
    int exitCode = admin.run(args);
    assertEquals("DFSAdmin should fail due to no handler registered", -1, exitCode);
  }

  @Test
  public void testValidIdentifier() throws Exception {
    DFSAdmin admin = new DFSAdmin(config);
    String[] args = new String[]{"-refresh",
        "localhost:" + cluster.getNameNodePort(), "firstHandler"};
    int exitCode = admin.run(args);
    assertEquals("DFSAdmin should succeed", 0, exitCode);

    Mockito.verify(firstHandler).handleRefresh("firstHandler", new String[]{});
    // Second handler was never called
    Mockito.verify(secondHandler, Mockito.never())
      .handleRefresh(Mockito.anyString(), Mockito.any(String[].class));
  }

  @Test
  public void testVariableArgs() throws Exception {
    DFSAdmin admin = new DFSAdmin(config);
    String[] args = new String[]{"-refresh", "localhost:" +
        cluster.getNameNodePort(), "secondHandler", "one"};
    int exitCode = admin.run(args);
    assertEquals("DFSAdmin should return 2", 2, exitCode);

    exitCode = admin.run(new String[]{"-refresh", "localhost:" +
        cluster.getNameNodePort(), "secondHandler", "one", "two"});
    assertEquals("DFSAdmin should now return 3", 3, exitCode);

    Mockito.verify(secondHandler).handleRefresh("secondHandler", new String[]{"one"});
    Mockito.verify(secondHandler).handleRefresh("secondHandler", new String[]{"one", "two"});
  }

  @Test
  public void testUnregistration() throws Exception {
    RefreshRegistry.defaultRegistry().unregisterAll("firstHandler");

    // And now this should fail
    DFSAdmin admin = new DFSAdmin(config);
    String[] args = new String[]{"-refresh", "localhost:" +
        cluster.getNameNodePort(), "firstHandler"};
    int exitCode = admin.run(args);
    assertEquals("DFSAdmin should return -1", -1, exitCode);
  }

  @Test
  public void testUnregistrationReturnValue() {
    RefreshHandler mockHandler = Mockito.mock(RefreshHandler.class);
    RefreshRegistry.defaultRegistry().register("test", mockHandler);
    boolean ret = RefreshRegistry.defaultRegistry().unregister("test", mockHandler);
    assertTrue(ret);
  }

  @Test
  public void testMultipleRegistration() throws Exception {
    RefreshRegistry.defaultRegistry().register("sharedId", firstHandler);
    RefreshRegistry.defaultRegistry().register("sharedId", secondHandler);

    // this should trigger both
    DFSAdmin admin = new DFSAdmin(config);
    String[] args = new String[]{"-refresh", "localhost:" +
        cluster.getNameNodePort(), "sharedId", "one"};
    int exitCode = admin.run(args);
    assertEquals(-1, exitCode); // -1 because one of the responses is unregistered

    // verify we called both
    Mockito.verify(firstHandler).handleRefresh("sharedId", new String[]{"one"});
    Mockito.verify(secondHandler).handleRefresh("sharedId", new String[]{"one"});

    RefreshRegistry.defaultRegistry().unregisterAll("sharedId");
  }

  @Test
  public void testMultipleReturnCodeMerging() throws Exception {
    // Two handlers which return two non-zero values
    RefreshHandler handlerOne = Mockito.mock(RefreshHandler.class);
    Mockito.when(handlerOne.handleRefresh(Mockito.anyString(), Mockito.any(String[].class)))
      .thenReturn(new RefreshResponse(23, "Twenty Three"));

    RefreshHandler handlerTwo = Mockito.mock(RefreshHandler.class);
    Mockito.when(handlerTwo.handleRefresh(Mockito.anyString(), Mockito.any(String[].class)))
      .thenReturn(new RefreshResponse(10, "Ten"));

    // Then registered to the same ID
    RefreshRegistry.defaultRegistry().register("shared", handlerOne);
    RefreshRegistry.defaultRegistry().register("shared", handlerTwo);

    // We refresh both
    DFSAdmin admin = new DFSAdmin(config);
    String[] args = new String[]{"-refresh", "localhost:" +
        cluster.getNameNodePort(), "shared"};
    int exitCode = admin.run(args);
    assertEquals(-1, exitCode); // We get -1 because of our logic for melding non-zero return codes

    // Verify we called both
    Mockito.verify(handlerOne).handleRefresh("shared", new String[]{});
    Mockito.verify(handlerTwo).handleRefresh("shared", new String[]{});

    RefreshRegistry.defaultRegistry().unregisterAll("shared");
  }

  @Test
  public void testExceptionResultsInNormalError() throws Exception {
    // In this test, we ensure that all handlers are called even if we throw an exception in one
    RefreshHandler exceptionalHandler = Mockito.mock(RefreshHandler.class);
    Mockito.when(exceptionalHandler.handleRefresh(Mockito.anyString(), Mockito.any(String[].class)))
      .thenThrow(new RuntimeException("Exceptional Handler Throws Exception"));

    RefreshHandler otherExceptionalHandler = Mockito.mock(RefreshHandler.class);
    Mockito.when(otherExceptionalHandler.handleRefresh(Mockito.anyString(), Mockito.any(String[].class)))
      .thenThrow(new RuntimeException("More Exceptions"));

    RefreshRegistry.defaultRegistry().register("exceptional", exceptionalHandler);
    RefreshRegistry.defaultRegistry().register("exceptional", otherExceptionalHandler);

    DFSAdmin admin = new DFSAdmin(config);
    String[] args = new String[]{"-refresh", "localhost:" +
        cluster.getNameNodePort(), "exceptional"};
    int exitCode = admin.run(args);
    assertEquals(-1, exitCode); // Exceptions result in a -1

    Mockito.verify(exceptionalHandler).handleRefresh("exceptional", new String[]{});
    Mockito.verify(otherExceptionalHandler).handleRefresh("exceptional", new String[]{});

    RefreshRegistry.defaultRegistry().unregisterAll("exceptional");
  }
}
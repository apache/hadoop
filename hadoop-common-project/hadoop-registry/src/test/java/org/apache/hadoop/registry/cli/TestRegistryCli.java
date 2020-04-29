/*
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
package org.apache.hadoop.registry.cli;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.apache.hadoop.registry.AbstractRegistryTest;
import org.apache.hadoop.registry.operations.TestRegistryOperations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRegistryCli extends AbstractRegistryTest {
  protected static final Logger LOG =
      LoggerFactory.getLogger(TestRegistryOperations.class);

  private ByteArrayOutputStream sysOutStream;
  private PrintStream sysOut;
  private ByteArrayOutputStream sysErrStream;
  private PrintStream sysErr;
  private RegistryCli cli;

  @Before
  public void setUp() throws Exception {
    sysOutStream = new ByteArrayOutputStream();
    sysOut = new PrintStream(sysOutStream);
    sysErrStream = new ByteArrayOutputStream();
    sysErr = new PrintStream(sysErrStream);
    System.setOut(sysOut);
    cli = new RegistryCli(operations, createRegistryConfiguration(), sysOut, sysErr);
  }

  @After
  public void tearDown() throws Exception {
    cli.close();
  }

  private void assertResult(RegistryCli cli, int code, String...args) throws Exception {
    int result = cli.run(args);
    assertEquals(code, result);
  }

  @Test
  public void testBadCommands() throws Exception {
    assertResult(cli, -1, new String[] { });
    assertResult(cli, -1, "foo");
  }

  @Test
  public void testInvalidNumArgs() throws Exception {
    assertResult(cli, -1, "ls");
    assertResult(cli, -1, "ls", "/path", "/extraPath");
    assertResult(cli, -1, "resolve");
    assertResult(cli, -1, "resolve", "/path", "/extraPath");
    assertResult(cli, -1, "mknode");
    assertResult(cli, -1, "mknode", "/path", "/extraPath");
    assertResult(cli, -1, "rm");
    assertResult(cli, -1, "rm", "/path", "/extraPath");
    assertResult(cli, -1, "bind");
    assertResult(cli, -1, "bind", "foo");
    assertResult(cli, -1, "bind", "-inet", "foo");
    assertResult(cli, -1, "bind", "-inet", "-api", "-p", "378", "-h", "host", "/foo");
    assertResult(cli, -1, "bind", "-inet", "-api", "Api", "-p", "-h", "host", "/foo");
    assertResult(cli, -1, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "/foo");
    assertResult(cli, -1, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "host");
    assertResult(cli, -1, "bind", "-api", "Api", "-p", "378", "-h", "host", "/foo");
    assertResult(cli, -1, "bind", "-webui", "foo");
    assertResult(cli, -1, "bind", "-webui", "-api", "Api", "/foo");
    assertResult(cli, -1, "bind", "-webui", "uriString", "-api", "/foo");
    assertResult(cli, -1, "bind", "-webui", "uriString", "-api", "Api");
    assertResult(cli, -1, "bind", "-rest", "foo");
    assertResult(cli, -1, "bind", "-rest", "uriString", "-api", "Api");
    assertResult(cli, -1, "bind", "-rest", "-api", "Api", "/foo");
    assertResult(cli, -1, "bind", "-rest", "uriString", "-api", "/foo");
    assertResult(cli, -1, "bind", "uriString", "-api", "Api", "/foo");
  }

  @Test
  public void testBadArgType() throws Exception {
    assertResult(cli, -1, "bind", "-inet", "-api", "Api", "-p", "fooPort", "-h",
        "host", "/dir");
  }

  @Test
  public void testBadPath() throws Exception {
    assertResult(cli, -1, "ls", "NonSlashPath");
    assertResult(cli, -1, "ls", "//");
    assertResult(cli, -1, "resolve", "NonSlashPath");
    assertResult(cli, -1, "resolve", "//");
    assertResult(cli, -1, "mknode", "NonSlashPath");
    assertResult(cli, -1, "mknode", "//");
    assertResult(cli, -1, "rm", "NonSlashPath");
    assertResult(cli, -1, "rm", "//");
    assertResult(cli, -1, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "host", "NonSlashPath");
    assertResult(cli, -1, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "host", "//");
    assertResult(cli, -1, "bind", "-webui", "uriString", "-api", "Api", "NonSlashPath");
    assertResult(cli, -1, "bind", "-webui", "uriString", "-api", "Api", "//");
    assertResult(cli, -1, "bind", "-rest", "uriString", "-api", "Api", "NonSlashPath");
    assertResult(cli, -1, "bind", "-rest", "uriString", "-api", "Api", "//");
  }

  @Test
  public void testNotExistingPaths() throws Exception {
    assertResult(cli, -1, "ls", "/nonexisting_path");
    assertResult(cli, -1, "ls", "/NonExistingDir/nonexisting_path");
    assertResult(cli, -1, "resolve", "/nonexisting_path");
    assertResult(cli, -1, "resolve", "/NonExistingDir/nonexisting_path");
    assertResult(cli, -1, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "host", "/NonExistingDir/nonexisting_path");
    assertResult(cli, -1, "bind", "-webui", "uriString", "-api", "Api", "/NonExistingDir/nonexisting_path");
    assertResult(cli, -1, "bind", "-rest", "uriString", "-api", "Api", "/NonExistingDir/nonexisting_path");
  }

  @Test
  public void testValidCommands() throws Exception {
    assertResult(cli, 0, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "host", "/foo");
    assertResult(cli, 0, "resolve", "/foo");
    assertResult(cli, 0, "rm", "/foo");
    assertResult(cli, -1, "resolve", "/foo");

    assertResult(cli, 0, "bind", "-webui", "uriString", "-api", "Api", "/foo");
    assertResult(cli, 0, "resolve", "/foo");
    assertResult(cli, 0, "rm", "/foo");
    assertResult(cli, -1, "resolve", "/foo");

    assertResult(cli, 0, "bind", "-rest", "uriString", "-api", "Api", "/foo");
    assertResult(cli, 0, "resolve", "/foo");
    assertResult(cli, 0, "rm", "/foo");
    assertResult(cli, -1, "resolve", "/foo");

    //Test Sub Directories Binds
    assertResult(cli, 0, "mknode", "/subdir");
    assertResult(cli, -1, "resolve", "/subdir");

    assertResult(cli, 0, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "host", "/subdir/foo");
    assertResult(cli, 0, "resolve", "/subdir/foo");
    assertResult(cli, 0, "rm", "/subdir/foo");
    assertResult(cli, -1, "resolve", "/subdir/foo");

    assertResult(cli, 0, "bind", "-webui", "uriString", "-api", "Api", "/subdir/foo");
    assertResult(cli, 0, "resolve", "/subdir/foo");
    assertResult(cli, 0, "rm", "/subdir/foo");
    assertResult(cli, -1, "resolve", "/subdir/foo");

    assertResult(cli, 0, "bind", "-rest", "uriString", "-api", "Api", "/subdir/foo");
    assertResult(cli, 0, "resolve", "/subdir/foo");
    assertResult(cli, 0, "rm", "/subdir/foo");
    assertResult(cli, -1, "resolve", "/subdir/foo");

    assertResult(cli, 0, "rm", "/subdir");
    assertResult(cli, -1, "resolve", "/subdir");

    //Test Bind that the dir itself
    assertResult(cli, 0, "mknode", "/dir");
    assertResult(cli, -1, "resolve", "/dir");

    assertResult(cli, 0, "bind", "-inet", "-api", "Api", "-p", "378", "-h", "host", "/dir");
    assertResult(cli, 0, "resolve", "/dir");
    assertResult(cli, 0, "rm", "/dir");
    assertResult(cli, -1, "resolve", "/dir");

    assertResult(cli, 0, "mknode", "/dir");
    assertResult(cli, -1, "resolve", "/dir");

    assertResult(cli, 0, "bind", "-webui", "uriString", "-api", "Api", "/dir");
    assertResult(cli, 0, "resolve", "/dir");
    assertResult(cli, 0, "rm", "/dir");
    assertResult(cli, -1, "resolve", "/dir");

    assertResult(cli, 0, "mknode", "/dir");
    assertResult(cli, -1, "resolve", "/dir");

    assertResult(cli, 0, "bind", "-rest", "uriString", "-api", "Api", "/dir");
    assertResult(cli, 0, "resolve", "/dir");
    assertResult(cli, 0, "rm", "/dir");
    assertResult(cli, -1, "resolve", "/dir");

    assertResult(cli, 0, "rm", "/Nonexitent");
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.utils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.service.api.records.KerberosPrincipal;
import org.apache.hadoop.yarn.submarine.FileUtilitiesForTests;
import org.apache.hadoop.yarn.submarine.client.cli.param.runjob.RunJobParameters;
import org.apache.hadoop.yarn.submarine.common.MockClientContext;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.FileSystemOperations;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This class is to test {@link KerberosPrincipalFactory}.
 */
public class TestKerberosPrincipalFactory {
  private FileUtilitiesForTests fileUtils = new FileUtilitiesForTests();

  @Before
  public void setUp() {
    fileUtils.setup();
  }

  @After
  public void teardown() throws IOException {
    fileUtils.teardown();
  }

  private File createKeytabFile(String keytabFileName) throws IOException {
    return fileUtils.createFileInTempDir(keytabFileName);
  }

  @Test
  public void testCreatePrincipalEmptyPrincipalAndKeytab() throws IOException {
    MockClientContext mockClientContext = new MockClientContext();

    RunJobParameters parameters = mock(RunJobParameters.class);
    when(parameters.getPrincipal()).thenReturn("");
    when(parameters.getKeytab()).thenReturn("");

    FileSystemOperations fsOperations =
        new FileSystemOperations(mockClientContext);
    KerberosPrincipal result =
        KerberosPrincipalFactory.create(fsOperations,
            mockClientContext.getRemoteDirectoryManager(), parameters);

    assertNull(result);
  }
  @Test
  public void testCreatePrincipalEmptyPrincipalString() throws IOException {
    MockClientContext mockClientContext = new MockClientContext();

    RunJobParameters parameters = mock(RunJobParameters.class);
    when(parameters.getPrincipal()).thenReturn("");
    when(parameters.getKeytab()).thenReturn("keytab");

    FileSystemOperations fsOperations =
        new FileSystemOperations(mockClientContext);
    KerberosPrincipal result =
        KerberosPrincipalFactory.create(fsOperations,
            mockClientContext.getRemoteDirectoryManager(), parameters);

    assertNull(result);
  }

  @Test
  public void testCreatePrincipalEmptyKeyTabString() throws IOException {
    MockClientContext mockClientContext = new MockClientContext();

    RunJobParameters parameters = mock(RunJobParameters.class);
    when(parameters.getPrincipal()).thenReturn("principal");
    when(parameters.getKeytab()).thenReturn("");

    FileSystemOperations fsOperations =
        new FileSystemOperations(mockClientContext);
    KerberosPrincipal result =
        KerberosPrincipalFactory.create(fsOperations,
            mockClientContext.getRemoteDirectoryManager(), parameters);

    assertNull(result);
  }

  @Test
  public void testCreatePrincipalNonEmptyPrincipalAndKeytab()
      throws IOException {
    MockClientContext mockClientContext = new MockClientContext();

    RunJobParameters parameters = mock(RunJobParameters.class);
    when(parameters.getPrincipal()).thenReturn("principal");
    when(parameters.getKeytab()).thenReturn("keytab");

    FileSystemOperations fsOperations =
        new FileSystemOperations(mockClientContext);
    KerberosPrincipal result =
        KerberosPrincipalFactory.create(fsOperations,
            mockClientContext.getRemoteDirectoryManager(), parameters);

    assertNotNull(result);
    assertEquals("file://keytab", result.getKeytab());
    assertEquals("principal", result.getPrincipalName());
  }

  @Test
  public void testCreatePrincipalDistributedKeytab() throws IOException {
    MockClientContext mockClientContext = new MockClientContext();
    String jobname = "testJobname";
    String keytab = "testKeytab";
    File keytabFile = createKeytabFile(keytab);

    RunJobParameters parameters = mock(RunJobParameters.class);
    when(parameters.getPrincipal()).thenReturn("principal");
    when(parameters.getKeytab()).thenReturn(keytabFile.getAbsolutePath());
    when(parameters.getName()).thenReturn(jobname);
    when(parameters.isDistributeKeytab()).thenReturn(true);

    FileSystemOperations fsOperations =
        new FileSystemOperations(mockClientContext);

    KerberosPrincipal result =
        KerberosPrincipalFactory.create(fsOperations,
            mockClientContext.getRemoteDirectoryManager(), parameters);

    Path stagingDir = mockClientContext.getRemoteDirectoryManager()
        .getJobStagingArea(parameters.getName(), true);
    String expectedKeytabFilePath =
        FileUtilitiesForTests.getFilename(stagingDir, keytab).getAbsolutePath();

    assertNotNull(result);
    assertEquals(expectedKeytabFilePath, result.getKeytab());
    assertEquals("principal", result.getPrincipalName());
  }

}
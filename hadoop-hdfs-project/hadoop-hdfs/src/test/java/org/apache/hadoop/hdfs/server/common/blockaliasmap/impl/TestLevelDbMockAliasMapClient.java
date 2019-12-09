/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.common.blockaliasmap.impl;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryLevelDBAliasMapServer;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.iq80.leveldb.DBException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.IOException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the in-memory alias map with a mock level-db implementation.
 */
public class TestLevelDbMockAliasMapClient {
  private InMemoryLevelDBAliasMapServer levelDBAliasMapServer;
  private InMemoryLevelDBAliasMapClient inMemoryLevelDBAliasMapClient;
  private File tempDir;
  private Configuration conf;
  private InMemoryAliasMap aliasMapMock;
  private final String bpid = "BPID-0";

  @Before
  public void setUp() throws IOException {
    aliasMapMock = mock(InMemoryAliasMap.class);
    when(aliasMapMock.getBlockPoolId()).thenReturn(bpid);
    levelDBAliasMapServer = new InMemoryLevelDBAliasMapServer(
        (config, blockPoolID) -> aliasMapMock, bpid);
    conf = new Configuration();
    int port = 9877;

    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS,
        "localhost:" + port);
    tempDir = Files.createTempDir();
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR,
        tempDir.getAbsolutePath());
    levelDBAliasMapServer.setConf(conf);
    levelDBAliasMapServer.start();
    inMemoryLevelDBAliasMapClient = new InMemoryLevelDBAliasMapClient();
    inMemoryLevelDBAliasMapClient.setConf(conf);
  }

  @After
  public void tearDown() throws IOException {
    levelDBAliasMapServer.close();
    inMemoryLevelDBAliasMapClient.close();
    FileUtils.deleteDirectory(tempDir);
  }

  @Test
  public void readFailure() throws Exception {
    Block block = new Block(42, 43, 44);
    doThrow(new IOException())
        .doThrow(new DBException())
        .when(aliasMapMock)
        .read(block);

    assertThatExceptionOfType(IOException.class)
        .isThrownBy(() ->
            inMemoryLevelDBAliasMapClient.getReader(null, bpid)
                .resolve(block));

    assertThatExceptionOfType(IOException.class)
        .isThrownBy(() ->
            inMemoryLevelDBAliasMapClient.getReader(null, bpid)
                .resolve(block));
  }

  @Test
  public void writeFailure() throws IOException {
    Block block = new Block(42, 43, 44);
    byte[] nonce = new byte[0];
    Path path = new Path("koekoek");
    ProvidedStorageLocation providedStorageLocation =
        new ProvidedStorageLocation(path, 45, 46, nonce);

    doThrow(new IOException())
        .when(aliasMapMock)
        .write(block, providedStorageLocation);

    assertThatExceptionOfType(IOException.class)
        .isThrownBy(() ->
            inMemoryLevelDBAliasMapClient.getWriter(null, bpid)
                .store(new FileRegion(block, providedStorageLocation)));

    assertThatExceptionOfType(IOException.class)
        .isThrownBy(() ->
            inMemoryLevelDBAliasMapClient.getWriter(null, bpid)
                .store(new FileRegion(block, providedStorageLocation)));
  }

}
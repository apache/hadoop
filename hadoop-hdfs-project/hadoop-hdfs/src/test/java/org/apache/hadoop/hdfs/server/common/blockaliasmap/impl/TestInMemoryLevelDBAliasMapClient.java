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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ProvidedStorageLocation;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryAliasMap;
import org.apache.hadoop.hdfs.server.aliasmap.InMemoryLevelDBAliasMapServer;
import org.apache.hadoop.hdfs.server.common.blockaliasmap.BlockAliasMap;
import org.apache.hadoop.hdfs.server.common.FileRegion;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.thirdparty.com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Tests the {@link InMemoryLevelDBAliasMapClient}.
 */
public class TestInMemoryLevelDBAliasMapClient {

  private InMemoryLevelDBAliasMapServer levelDBAliasMapServer;
  private InMemoryLevelDBAliasMapClient inMemoryLevelDBAliasMapClient;
  private File tempDir;
  private Configuration conf;
  private final static String BPID = "BPID-0";

  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    int port = 9876;

    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_RPC_ADDRESS,
        "localhost:" + port);
    File testDir = GenericTestUtils.getTestDir();
    tempDir = Files
        .createTempDirectory(testDir.toPath(), "test").toFile();
    File levelDBDir = new File(tempDir, BPID);
    levelDBDir.mkdirs();
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_LEVELDB_DIR,
        tempDir.getAbsolutePath());
    levelDBAliasMapServer =
        new InMemoryLevelDBAliasMapServer(InMemoryAliasMap::init, BPID);
    inMemoryLevelDBAliasMapClient = new InMemoryLevelDBAliasMapClient();
  }

  @After
  public void tearDown() throws IOException {
    levelDBAliasMapServer.close();
    inMemoryLevelDBAliasMapClient.close();
    FileUtils.deleteDirectory(tempDir);
  }

  @Test
  public void writeRead() throws Exception {
    levelDBAliasMapServer.setConf(conf);
    levelDBAliasMapServer.start();
    inMemoryLevelDBAliasMapClient.setConf(conf);
    Block block = new Block(42, 43, 44);
    byte[] nonce = "blackbird".getBytes();
    ProvidedStorageLocation providedStorageLocation
        = new ProvidedStorageLocation(new Path("cuckoo"),
        45, 46, nonce);
    BlockAliasMap.Writer<FileRegion> writer =
        inMemoryLevelDBAliasMapClient.getWriter(null, BPID);
    writer.store(new FileRegion(block, providedStorageLocation));

    BlockAliasMap.Reader<FileRegion> reader =
        inMemoryLevelDBAliasMapClient.getReader(null, BPID);
    Optional<FileRegion> fileRegion = reader.resolve(block);
    assertEquals(new FileRegion(block, providedStorageLocation),
        fileRegion.get());
  }

  @Test
  public void iterateSingleBatch() throws Exception {
    levelDBAliasMapServer.setConf(conf);
    levelDBAliasMapServer.start();
    inMemoryLevelDBAliasMapClient.setConf(conf);
    Block block1 = new Block(42, 43, 44);
    Block block2 = new Block(43, 44, 45);
    byte[] nonce1 = "blackbird".getBytes();
    byte[] nonce2 = "cuckoo".getBytes();
    ProvidedStorageLocation providedStorageLocation1 =
        new ProvidedStorageLocation(new Path("eagle"),
        46, 47, nonce1);
    ProvidedStorageLocation providedStorageLocation2 =
        new ProvidedStorageLocation(new Path("falcon"),
            46, 47, nonce2);
    BlockAliasMap.Writer<FileRegion> writer1 =
        inMemoryLevelDBAliasMapClient.getWriter(null, BPID);
    writer1.store(new FileRegion(block1, providedStorageLocation1));
    BlockAliasMap.Writer<FileRegion> writer2 =
        inMemoryLevelDBAliasMapClient.getWriter(null, BPID);
    writer2.store(new FileRegion(block2, providedStorageLocation2));

    BlockAliasMap.Reader<FileRegion> reader =
        inMemoryLevelDBAliasMapClient.getReader(null, BPID);
    List<FileRegion> actualFileRegions =
        Lists.newArrayListWithCapacity(2);
    for (FileRegion fileRegion : reader) {
      actualFileRegions.add(fileRegion);
    }

    assertArrayEquals(
        new FileRegion[] {new FileRegion(block1, providedStorageLocation1),
            new FileRegion(block2, providedStorageLocation2)},
        actualFileRegions.toArray());
  }

  @Test
  public void iterateThreeBatches() throws Exception {
    conf.set(DFSConfigKeys.DFS_PROVIDED_ALIASMAP_INMEMORY_BATCH_SIZE, "2");
    levelDBAliasMapServer.setConf(conf);
    levelDBAliasMapServer.start();
    inMemoryLevelDBAliasMapClient.setConf(conf);
    Block block1 = new Block(42, 43, 44);
    Block block2 = new Block(43, 44, 45);
    Block block3 = new Block(44, 45, 46);
    Block block4 = new Block(47, 48, 49);
    Block block5 = new Block(50, 51, 52);
    Block block6 = new Block(53, 54, 55);
    byte[] nonce1 = "blackbird".getBytes();
    byte[] nonce2 = "cuckoo".getBytes();
    byte[] nonce3 = "sparrow".getBytes();
    byte[] nonce4 = "magpie".getBytes();
    byte[] nonce5 = "seagull".getBytes();
    byte[] nonce6 = "finch".getBytes();
    ProvidedStorageLocation providedStorageLocation1 =
        new ProvidedStorageLocation(new Path("eagle"),
            46, 47, nonce1);
    ProvidedStorageLocation providedStorageLocation2 =
        new ProvidedStorageLocation(new Path("falcon"),
            48, 49, nonce2);
    ProvidedStorageLocation providedStorageLocation3 =
        new ProvidedStorageLocation(new Path("robin"),
            50, 51, nonce3);
    ProvidedStorageLocation providedStorageLocation4 =
        new ProvidedStorageLocation(new Path("parakeet"),
            52, 53, nonce4);
    ProvidedStorageLocation providedStorageLocation5 =
        new ProvidedStorageLocation(new Path("heron"),
            54, 55, nonce5);
    ProvidedStorageLocation providedStorageLocation6 =
        new ProvidedStorageLocation(new Path("duck"),
            56, 57, nonce6);
    inMemoryLevelDBAliasMapClient
        .getWriter(null, BPID)
        .store(new FileRegion(block1, providedStorageLocation1));
    inMemoryLevelDBAliasMapClient
        .getWriter(null, BPID)
        .store(new FileRegion(block2, providedStorageLocation2));
    inMemoryLevelDBAliasMapClient
        .getWriter(null, BPID)
        .store(new FileRegion(block3, providedStorageLocation3));
    inMemoryLevelDBAliasMapClient
        .getWriter(null, BPID)
        .store(new FileRegion(block4, providedStorageLocation4));
    inMemoryLevelDBAliasMapClient
        .getWriter(null, BPID)
        .store(new FileRegion(block5, providedStorageLocation5));
    inMemoryLevelDBAliasMapClient
        .getWriter(null, BPID)
        .store(new FileRegion(block6, providedStorageLocation6));

    BlockAliasMap.Reader<FileRegion> reader =
        inMemoryLevelDBAliasMapClient.getReader(null, BPID);
    List<FileRegion> actualFileRegions =
        Lists.newArrayListWithCapacity(6);
    for (FileRegion fileRegion : reader) {
      actualFileRegions.add(fileRegion);
    }

    FileRegion[] expectedFileRegions =
        new FileRegion[] {new FileRegion(block1, providedStorageLocation1),
            new FileRegion(block2, providedStorageLocation2),
            new FileRegion(block3, providedStorageLocation3),
            new FileRegion(block4, providedStorageLocation4),
            new FileRegion(block5, providedStorageLocation5),
            new FileRegion(block6, providedStorageLocation6)};
    assertArrayEquals(expectedFileRegions, actualFileRegions.toArray());
  }


  class ReadThread implements Runnable {
    private final Block block;
    private final BlockAliasMap.Reader<FileRegion> reader;
    private int delay;
    private Optional<FileRegion> fileRegionOpt;

    ReadThread(Block block, BlockAliasMap.Reader<FileRegion> reader,
        int delay) {
      this.block = block;
      this.reader = reader;
      this.delay = delay;
    }

    public Optional<FileRegion> getFileRegion() {
      return fileRegionOpt;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(delay);
        fileRegionOpt = reader.resolve(block);
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  class WriteThread implements Runnable {
    private final Block block;
    private final BlockAliasMap.Writer<FileRegion> writer;
    private final ProvidedStorageLocation providedStorageLocation;
    private int delay;

    WriteThread(Block block, ProvidedStorageLocation providedStorageLocation,
        BlockAliasMap.Writer<FileRegion> writer, int delay) {
      this.block = block;
      this.writer = writer;
      this.providedStorageLocation = providedStorageLocation;
      this.delay = delay;
    }

    @Override
    public void run() {
      try {
        Thread.sleep(delay);
        writer.store(new FileRegion(block, providedStorageLocation));
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public FileRegion generateRandomFileRegion(int seed) {
    Block block = new Block(seed, seed + 1, seed + 2);
    Path path = new Path("koekoek");
    byte[] nonce = new byte[0];
    ProvidedStorageLocation providedStorageLocation =
        new ProvidedStorageLocation(path, seed + 3, seed + 4, nonce);
    return new FileRegion(block, providedStorageLocation);
  }

  @Test
  public void multipleReads() throws IOException {
    levelDBAliasMapServer.setConf(conf);
    levelDBAliasMapServer.start();
    inMemoryLevelDBAliasMapClient.setConf(conf);

    Random r = new Random();
    List<FileRegion> expectedFileRegions = r.ints(0, 200)
        .limit(50)
        .boxed()
        .map(i -> generateRandomFileRegion(i))
        .collect(Collectors.toList());


    BlockAliasMap.Reader<FileRegion> reader =
        inMemoryLevelDBAliasMapClient.getReader(null, BPID);
    BlockAliasMap.Writer<FileRegion> writer =
        inMemoryLevelDBAliasMapClient.getWriter(null, BPID);

    ExecutorService executor = Executors.newCachedThreadPool();

    List<ReadThread> readThreads = expectedFileRegions
        .stream()
        .map(fileRegion -> new ReadThread(fileRegion.getBlock(),
            reader,
            4000))
        .collect(Collectors.toList());


    List<? extends Future<?>> readFutures =
        readThreads.stream()
            .map(readThread -> executor.submit(readThread))
            .collect(Collectors.toList());

    List<? extends Future<?>> writeFutures = expectedFileRegions
        .stream()
        .map(fileRegion -> new WriteThread(fileRegion.getBlock(),
            fileRegion.getProvidedStorageLocation(),
            writer,
            1000))
        .map(writeThread -> executor.submit(writeThread))
        .collect(Collectors.toList());

    readFutures.stream()
        .map(readFuture -> {
          try {
            return readFuture.get();
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          } catch (ExecutionException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList());

    List<FileRegion> actualFileRegions = readThreads.stream()
        .map(readThread -> readThread.getFileRegion().get())
        .collect(Collectors.toList());

    assertThat(actualFileRegions).containsExactlyInAnyOrder(
        expectedFileRegions.toArray(new FileRegion[0]));
  }

  @Test
  public void testServerBindHost() throws Exception {
    conf.set(DFS_NAMENODE_SERVICE_RPC_BIND_HOST_KEY, "0.0.0.0");
    writeRead();
  }

  @Test
  public void testNonExistentBlock() throws Exception {
    inMemoryLevelDBAliasMapClient.setConf(conf);
    levelDBAliasMapServer.setConf(conf);
    levelDBAliasMapServer.start();
    Block block1 = new Block(100, 43, 44);
    ProvidedStorageLocation providedStorageLocation1 = null;
    BlockAliasMap.Writer<FileRegion> writer1 =
        inMemoryLevelDBAliasMapClient.getWriter(null, BPID);
    try {
      writer1.store(new FileRegion(block1, providedStorageLocation1));
      fail("Should fail on writing a region with null ProvidedLocation");
    } catch (IOException | IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("not be null"));
    }

    BlockAliasMap.Reader<FileRegion> reader =
        inMemoryLevelDBAliasMapClient.getReader(null, BPID);
    LambdaTestUtils.assertOptionalUnset("Expected empty BlockAlias",
        reader.resolve(block1));
  }
}
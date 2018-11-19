/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.interfaces.ContainerPacker;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Test the tar/untar for a given container.
 */
public class TestTarContainerPacker {

  private static final String TEST_DB_FILE_NAME = "test1";

  private static final String TEST_DB_FILE_CONTENT = "test1";

  private static final String TEST_CHUNK_FILE_NAME = "chunk1";

  private static final String TEST_CHUNK_FILE_CONTENT = "This is a chunk";

  private static final String TEST_DESCRIPTOR_FILE_CONTENT = "descriptor";

  private ContainerPacker packer = new TarContainerPacker();

  private static final Path SOURCE_CONTAINER_ROOT =
      Paths.get("target/test/data/packer-source-dir");

  private static final Path DEST_CONTAINER_ROOT =
      Paths.get("target/test/data/packer-dest-dir");

  @BeforeClass
  public static void init() throws IOException {
    initDir(SOURCE_CONTAINER_ROOT);
    initDir(DEST_CONTAINER_ROOT);
  }

  private static void initDir(Path path) throws IOException {
    if (path.toFile().exists()) {
      FileUtils.deleteDirectory(path.toFile());
    }
    path.toFile().mkdirs();
  }

  private KeyValueContainerData createContainer(long id, Path dir,
      OzoneConfiguration conf) throws IOException {

    Path containerDir = dir.resolve("container" + id);
    Path dbDir = containerDir.resolve("db");
    Path dataDir = containerDir.resolve("data");
    Files.createDirectories(dbDir);
    Files.createDirectories(dataDir);

    KeyValueContainerData containerData = new KeyValueContainerData(
        id, -1, UUID.randomUUID().toString(), UUID.randomUUID().toString());
    containerData.setChunksPath(dataDir.toString());
    containerData.setMetadataPath(dbDir.getParent().toString());
    containerData.setDbFile(dbDir.toFile());


    return containerData;
  }

  @Test
  public void pack() throws IOException, CompressorException {

    //GIVEN
    OzoneConfiguration conf = new OzoneConfiguration();

    KeyValueContainerData sourceContainerData =
        createContainer(1L, SOURCE_CONTAINER_ROOT, conf);

    KeyValueContainer sourceContainer =
        new KeyValueContainer(sourceContainerData, conf);

    //sample db file in the metadata directory
    try (FileWriter writer = new FileWriter(
        sourceContainerData.getDbFile().toPath()
            .resolve(TEST_DB_FILE_NAME)
            .toFile())) {
      IOUtils.write(TEST_DB_FILE_CONTENT, writer);
    }

    //sample chunk file in the chunk directory
    try (FileWriter writer = new FileWriter(
        Paths.get(sourceContainerData.getChunksPath())
            .resolve(TEST_CHUNK_FILE_NAME)
            .toFile())) {
      IOUtils.write(TEST_CHUNK_FILE_CONTENT, writer);
    }

    //sample container descriptor file
    try (FileWriter writer = new FileWriter(
        sourceContainer.getContainerFile())) {
      IOUtils.write(TEST_DESCRIPTOR_FILE_CONTENT, writer);
    }

    Path targetFile =
        SOURCE_CONTAINER_ROOT.getParent().resolve("container.tar.gz");

    //WHEN: pack it
    try (FileOutputStream output = new FileOutputStream(targetFile.toFile())) {
      packer.pack(sourceContainer, output);
    }

    //THEN: check the result
    try (FileInputStream input = new FileInputStream(targetFile.toFile())) {
      CompressorInputStream uncompressed = new CompressorStreamFactory()
          .createCompressorInputStream(CompressorStreamFactory.GZIP, input);
      TarArchiveInputStream tarStream = new TarArchiveInputStream(uncompressed);

      TarArchiveEntry entry;
      Map<String, TarArchiveEntry> entries = new HashMap<>();
      while ((entry = tarStream.getNextTarEntry()) != null) {
        entries.put(entry.getName(), entry);
      }

      Assert.assertTrue(
          entries.containsKey("container.yaml"));

    }

    //read the container descriptor only
    try (FileInputStream input = new FileInputStream(targetFile.toFile())) {
      String containerYaml = new String(packer.unpackContainerDescriptor(input),
          Charset.forName(UTF_8.name()));
      Assert.assertEquals(TEST_DESCRIPTOR_FILE_CONTENT, containerYaml);
    }

    KeyValueContainerData destinationContainerData =
        createContainer(2L, DEST_CONTAINER_ROOT, conf);

    KeyValueContainer destinationContainer =
        new KeyValueContainer(destinationContainerData, conf);

    String descriptor = "";

    //unpackContainerData
    try (FileInputStream input = new FileInputStream(targetFile.toFile())) {
      descriptor =
          new String(packer.unpackContainerData(destinationContainer, input),
              Charset.forName(UTF_8.name()));
    }

    assertExampleMetadataDbIsGood(
        destinationContainerData.getDbFile().toPath());
    assertExampleChunkFileIsGood(
        Paths.get(destinationContainerData.getChunksPath()));
    Assert.assertFalse(
        "Descriptor file should not been exctarcted by the "
            + "unpackContainerData Call",
        destinationContainer.getContainerFile().exists());
    Assert.assertEquals(TEST_DESCRIPTOR_FILE_CONTENT, descriptor);

  }


  private void assertExampleMetadataDbIsGood(Path dbPath)
      throws IOException {

    Path dbFile = dbPath.resolve(TEST_DB_FILE_NAME);

    Assert.assertTrue(
        "example DB file is missing after pack/unpackContainerData: " + dbFile,
        Files.exists(dbFile));

    try (FileInputStream testFile = new FileInputStream(dbFile.toFile())) {
      List<String> strings = IOUtils
          .readLines(testFile, Charset.forName(UTF_8.name()));
      Assert.assertEquals(1, strings.size());
      Assert.assertEquals(TEST_DB_FILE_CONTENT, strings.get(0));
    }
  }

  private void assertExampleChunkFileIsGood(Path chunkDirPath)
      throws IOException {

    Path chunkFile = chunkDirPath.resolve(TEST_CHUNK_FILE_NAME);

    Assert.assertTrue(
        "example chunk file is missing after pack/unpackContainerData: "
            + chunkFile,
        Files.exists(chunkFile));

    try (FileInputStream testFile = new FileInputStream(chunkFile.toFile())) {
      List<String> strings = IOUtils
          .readLines(testFile, Charset.forName(UTF_8.name()));
      Assert.assertEquals(1, strings.size());
      Assert.assertEquals(TEST_CHUNK_FILE_CONTENT, strings.get(0));
    }
  }

}